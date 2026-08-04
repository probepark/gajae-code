import { createHash, randomUUID } from "node:crypto";
import type { SdkFrame } from "./types";

export const REVERSE_HEARTBEAT_MS = 5_000;
export const REVERSE_LEASE_TTL_MS = 15_000;
export const REVERSE_RECLAIM_GRACE_MS = 30_000;
export const MAX_REVERSE_OUTSTANDING = 64;
export const MAX_REVERSE_PAYLOAD_BYTES = 256 * 1024;
export const MAX_REVERSE_TERMINAL_RECORDS = 256;
export const REVERSE_TOMBSTONE_TTL_MS = 30_000;

export class ReverseLeaseError extends Error {
	constructor(
		readonly code:
			| "lease_unavailable"
			| "lease_expired"
			| "provider_lease_conflict"
			| "provider_required"
			| "not_lease_owner"
			| "payload_too_large"
			| "too_many_outstanding"
			| "unknown_request"
			| "idempotency_conflict"
			| "terminal_capacity_exceeded"
			| "invalid_provider_result",
		message = code,
	) {
		super(message);
	}
}

export interface ProviderLease {
	leaseId: string;
	connectionId: string;
	capability: string;
	definitions: unknown;
	installedMethods: string[];
	expiresAt: number;
	graceUntil?: number;
	active: boolean;
}

export interface ProviderInstallationReceipt {
	installedMethods: readonly string[];
}
export type ValidatedReverseSettlement =
	| { ok: true; result: unknown; canonical: string }
	| { ok: false; error: { code: string; message: string }; canonical: string };

export type ReverseResultValidator = (wire: {
	ok: boolean;
	result?: unknown;
	error?: { code: string; message: string };
}) => ValidatedReverseSettlement;

interface Outstanding {
	connectionId: string;
	capability: string;
	method: string;
	leaseId: string;
	resolve: (value: unknown) => void;
	reject: (reason: Error) => void;
	validate?: ReverseResultValidator;
	signal?: AbortSignal;
	onAbort?: () => void;
}

interface TerminalRecord {
	capability: string;
	method: string;
	leaseId: string;
	fingerprint: string;
	validate?: ReverseResultValidator;
	expiresAt: number;
}

export interface ReverseLeaseOptions {
	now?: () => number;
	leaseTtlMs?: number;
	sendFrame: (connectionId: string, frame: SdkFrame) => void | Promise<void>;
	installDefinitions?: (capability: string, definitions: unknown) => unknown;
	onCancel?: (requestId: string, reason: "provider_disconnected" | "lease_released") => void;
	onDefinitionsRemoved?: (capability: string) => void;
}

function canonicalJson(value: unknown): string {
	if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
	if (value && typeof value === "object") {
		const entries = Object.entries(value as Record<string, unknown>)
			.filter(([, entry]) => entry !== undefined)
			.sort(([left], [right]) => left.localeCompare(right));
		return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`).join(",")}}`;
	}
	return JSON.stringify(value) ?? "null";
}

function registrationFingerprint(capability: string, definitions: unknown, expectedLeaseId?: string): string {
	return createHash("sha256")
		.update(canonicalJson({ capability, definitions, expectedLeaseId: expectedLeaseId ?? null }))
		.digest("hex");
}

function responseFingerprint(result: unknown, error?: { code: string; message: string }): string {
	return createHash("sha256").update(canonicalJson(error ? { ok: false, error } : { ok: true, result })).digest("hex");
}

function normalizedMethods(receipt: unknown): string[] {
	if (
		!receipt ||
		typeof receipt !== "object" ||
		!Array.isArray((receipt as Partial<ProviderInstallationReceipt>).installedMethods)
	) {
		return [];
	}
	return [
		...new Set(
			(receipt as ProviderInstallationReceipt).installedMethods.filter(
				(method): method is string => typeof method === "string" && method.length > 0,
			),
		),
	].sort();
}
function canonicalFingerprint(canonical: string): string {
	return createHash("sha256").update(canonical).digest("hex");
}

function validateSettlement(
	validate: ReverseResultValidator | undefined,
	result: unknown,
	error?: { code: string; message: string },
): { result?: unknown; error?: { code: string; message: string }; fingerprint: string } {
	if (!validate) return { ...(error ? { error } : { result }), fingerprint: responseFingerprint(result, error) };
	let settlement: ValidatedReverseSettlement;
	try {
		settlement = validate(error ? { ok: false, error } : { ok: true, result });
	} catch {
		throw new ReverseLeaseError("invalid_provider_result");
	}
	if (
		!settlement ||
		typeof settlement !== "object" ||
		typeof settlement.ok !== "boolean" ||
		typeof settlement.canonical !== "string" ||
		settlement.canonical.length === 0
	)
		throw new ReverseLeaseError("invalid_provider_result");
	if (settlement.ok) return { result: settlement.result, fingerprint: canonicalFingerprint(settlement.canonical) };
	if (
		!settlement.error ||
		typeof settlement.error.code !== "string" ||
		typeof settlement.error.message !== "string"
	)
		throw new ReverseLeaseError("invalid_provider_result");
	return { error: settlement.error, fingerprint: canonicalFingerprint(settlement.canonical) };
}
function cloneLease(lease: ProviderLease): ProviderLease {
	return { ...lease, installedMethods: [...lease.installedMethods] };
}

/** Session-local directed reverse RPC lease registry. */
export class ReverseLeaseRuntime {
	readonly #now: () => number;
	readonly #leaseTtlMs: number;
	readonly #sendFrame: ReverseLeaseOptions["sendFrame"];
	readonly #installDefinitions?: ReverseLeaseOptions["installDefinitions"];
	readonly #onDefinitionsRemoved?: ReverseLeaseOptions["onDefinitionsRemoved"];
	readonly #onCancel?: ReverseLeaseOptions["onCancel"];
	readonly #leases = new Map<string, ProviderLease>();
	readonly #idempotency = new Map<string, { fingerprint: string; lease: ProviderLease }>();
	readonly #outstanding = new Map<string, Outstanding>();
	readonly #terminal = new Map<string, TerminalRecord>();
	readonly #installedCapabilities = new Set<string>();
	readonly #sweepTimer: ReturnType<typeof setInterval>;
	#disposing = false;

	constructor(options: ReverseLeaseOptions) {
		this.#now = options.now ?? Date.now;
		this.#leaseTtlMs = options.leaseTtlMs ?? REVERSE_LEASE_TTL_MS;
		this.#sendFrame = options.sendFrame;
		this.#installDefinitions = options.installDefinitions;
		this.#onDefinitionsRemoved = options.onDefinitionsRemoved;
		this.#onCancel = options.onCancel;
		this.#sweepTimer = setInterval(() => this.#expireStaleLeases(), Math.max(1, this.#leaseTtlMs / 3));
		this.#sweepTimer.unref?.();
	}

	registerProvider(
		connectionId: string,
		capability: string,
		definitions: unknown,
		expectedLeaseId?: string,
		idempotencyKey?: string,
	): ProviderLease {
		if (this.#disposing) throw new Error("reverse runtime is disposing");
		const key = `${connectionId}\u0000${idempotencyKey ?? ""}`;
		const fingerprint = registrationFingerprint(capability, definitions, expectedLeaseId);
		const replay = idempotencyKey ? this.#idempotency.get(key) : undefined;
		if (replay) {
			if (replay.fingerprint !== fingerprint) throw new ReverseLeaseError("idempotency_conflict");
			const current = this.#leases.get(capability);
			if (replay.lease === current && current?.active && current.expiresAt > this.#now()) return cloneLease(replay.lease);
		}
		const now = this.#now();
		const existing = this.#leases.get(capability);
		if (existing && existing.expiresAt <= now) this.#removeDefinitions(existing.capability);
		const pendingHandoff = existing?.active === false && existing.expiresAt > now;
		if (pendingHandoff) {
			if (existing!.connectionId !== connectionId || existing!.leaseId !== expectedLeaseId)
				throw new ReverseLeaseError("provider_lease_conflict");
			const installedMethods = this.#installDefinitionsFor(capability, definitions);
			const lease: ProviderLease = {
				leaseId: existing!.leaseId,
				connectionId,
				capability,
				definitions,
				installedMethods,
				expiresAt: now + this.#leaseTtlMs,
				active: true,
			};
			this.#leases.set(capability, lease);
			if (idempotencyKey) this.#idempotency.set(key, { fingerprint, lease });
			return cloneLease(lease);
		}
		const reclaiming =
			existing?.leaseId === expectedLeaseId && existing?.graceUntil !== undefined && now <= existing.graceUntil;
		const refreshing =
			existing?.active !== false && existing?.connectionId === connectionId && existing.expiresAt > now;
		if (existing && !reclaiming && !refreshing && existing.connectionId !== connectionId && existing.expiresAt > now)
			throw new ReverseLeaseError("provider_lease_conflict");
		const installedMethods = this.#installDefinitionsFor(capability, definitions);
		const lease: ProviderLease = {
			leaseId: reclaiming || refreshing ? existing!.leaseId : randomUUID(),
			connectionId,
			capability,
			definitions,
			installedMethods,
			expiresAt: now + this.#leaseTtlMs,
			active: true,
		};
		this.#leases.set(capability, lease);
		if (idempotencyKey) this.#idempotency.set(key, { fingerprint, lease });
		return cloneLease(lease);
	}

	heartbeat(connectionId: string, leaseId: string): ProviderLease {
		const lease = this.#owner(connectionId, leaseId);
		if (lease.expiresAt <= this.#now()) {
			this.#removeDefinitions(lease.capability);
			throw new ReverseLeaseError("lease_expired");
		}
		lease.expiresAt = this.#now() + this.#leaseTtlMs;
		lease.graceUntil = undefined;
		return cloneLease(lease);
	}

	release(connectionId: string, leaseId: string, handoffTo?: string): ProviderLease {
		const lease = this.#owner(connectionId, leaseId);
		this.#cancelForConnection(connectionId, "lease_released");
		this.#removeDefinitions(lease.capability);
		if (handoffTo) {
			lease.connectionId = handoffTo;
			lease.expiresAt = this.#now() + REVERSE_RECLAIM_GRACE_MS;
			lease.graceUntil = undefined;
			lease.active = false;
			return cloneLease(lease);
		}
		this.#leases.delete(lease.capability);
		return cloneLease(lease);
	}

	disconnect(connectionId: string): void {
		const now = this.#now();
		for (const lease of this.#leases.values())
			if (lease.connectionId === connectionId) {
				lease.expiresAt = now;
				lease.graceUntil = now + REVERSE_RECLAIM_GRACE_MS;
				this.#removeDefinitions(lease.capability);
			}
		this.#cancelForConnection(connectionId, "provider_disconnected");
	}

	request(
		capability: string,
		method: string,
		payload: unknown,
		signal?: AbortSignal,
		validate?: ReverseResultValidator,
	): Promise<unknown> {
		if (this.#disposing) throw new Error("reverse runtime is disposing");
		this.#assertPayload(payload);
		const lease = this.#liveLease(capability);
		if (!lease) {
			const reservation = this.#leases.get(capability);
			if (reservation?.active === false && reservation.expiresAt > this.#now())
				throw new ReverseLeaseError("lease_unavailable");
			throw new ReverseLeaseError("provider_required");
		}
		if (signal?.aborted)
			return Promise.reject(Object.assign(new Error("request_cancelled"), { name: "request_cancelled" }));
		this.#sweepTerminal();
		if (this.#outstanding.size >= MAX_REVERSE_OUTSTANDING) throw new ReverseLeaseError("too_many_outstanding");
		if (this.#outstanding.size + this.#terminal.size >= MAX_REVERSE_TERMINAL_RECORDS)
			throw new ReverseLeaseError("terminal_capacity_exceeded");
		const id = randomUUID();
		return new Promise((resolve, reject) => {
			const outstanding: Outstanding = {
				connectionId: lease.connectionId,
				capability,
				method,
				leaseId: lease.leaseId,
				resolve,
				reject,
				...(validate ? { validate } : {}),
				...(signal ? { signal } : {}),
			};
			this.#outstanding.set(id, outstanding);
			if (signal) {
				outstanding.onAbort = () => {
					if (this.#takeOutstanding(id) !== outstanding) return;
					try {
						const cancellation = this.#sendFrame(lease.connectionId, {
							type: "reverse_cancel",
							id,
							connectionId: lease.connectionId,
							leaseId: lease.leaseId,
						});
						void Promise.resolve(cancellation).catch(() => {});
					} catch {
						// Cancellation is best effort; the caller is already settled locally.
					}
					reject(Object.assign(new Error("request_cancelled"), { name: "request_cancelled" }));
				};
				signal.addEventListener("abort", outstanding.onAbort, { once: true });
				if (signal.aborted) {
					outstanding.onAbort();
					return;
				}
			}
			let delivery: void | Promise<void>;
			try {
				delivery = this.#sendFrame(lease.connectionId, {
					type: "reverse_request",
					id,
					capability,
					connectionId: lease.connectionId,
					leaseId: lease.leaseId,
					payload: { method, payload },
				});
			} catch (error) {
				this.#takeOutstanding(id);
				reject(error instanceof Error ? error : new Error(String(error)));
				return;
			}
			Promise.resolve(delivery).catch(error => {
				if (this.#takeOutstanding(id) !== outstanding) return;
				reject(error instanceof Error ? error : new Error(String(error)));
			});
		});
	}

	respond(
		connectionId: string,
		id: string,
		leaseId: string,
		result: unknown,
		error?: { code: string; message: string },
	): "accepted" | "replayed" {
		this.#assertPayload(error ?? result);
		this.#sweepTerminal();
		const terminal = this.#terminal.get(id);
		if (terminal) {
			const lease = this.#owner(connectionId, leaseId);
			if (lease.capability !== terminal.capability || terminal.leaseId !== leaseId)
				throw new ReverseLeaseError("not_lease_owner");
			const replay = validateSettlement(terminal.validate, result, error);
			if (terminal.fingerprint !== replay.fingerprint) throw new ReverseLeaseError("idempotency_conflict");
			return "replayed";
		}
		const request = this.#outstanding.get(id);
		if (!request) throw new ReverseLeaseError("unknown_request");
		if (request.connectionId !== connectionId || request.leaseId !== leaseId)
			throw new ReverseLeaseError("not_lease_owner");

		const accepted = validateSettlement(request.validate, result, error);
		this.#assertPayload(accepted.error ?? accepted.result);
		this.#terminal.set(id, {
			capability: request.capability,
			method: request.method,
			leaseId: request.leaseId,
			fingerprint: accepted.fingerprint,
			...(request.validate ? { validate: request.validate } : {}),
			expiresAt: this.#now() + REVERSE_TOMBSTONE_TTL_MS,
		});
		this.#takeOutstanding(id);
		if (accepted.error) {
			const rejection = new Error(accepted.error.message);
			rejection.name = accepted.error.code;
			request.reject(rejection);
		} else request.resolve(accepted.result);
		return "accepted";
	}

	getLease(capability: string): ProviderLease | undefined {
		const lease = this.#liveLease(capability);
		return lease && cloneLease(lease);
	}

	/** Installed definitions are observable only while their provider lease is live. */
	getInstalledDefinitions(capability: string): unknown | undefined {
		return this.#liveLease(capability)?.definitions;
	}
	getInstalledMethods(capability: string): readonly string[] | undefined {
		const lease = this.#liveLease(capability);
		return lease ? [...lease.installedMethods] : undefined;
	}

	dispose(): void {
		if (this.#disposing) return;
		this.#disposing = true;
		clearInterval(this.#sweepTimer);
		const outstanding = [...this.#outstanding.entries()];
		const installedCapabilities = [...this.#installedCapabilities];
		this.#outstanding.clear();
		this.#installedCapabilities.clear();
		this.#leases.clear();
		this.#idempotency.clear();
		this.#terminal.clear();
		for (const request of outstanding.map(([, request]) => request))
			if (request.signal && request.onAbort) request.signal.removeEventListener("abort", request.onAbort);
		for (const [id, request] of outstanding) {
			request.reject(new Error("request_cancelled"));
			try {
				this.#onCancel?.(id, "lease_released");
			} catch {}
		}
		for (const capability of installedCapabilities) {
			try {
				this.#onDefinitionsRemoved?.(capability);
			} catch {}
		}
		this.#disposing = false;
	}

	#owner(connectionId: string, leaseId: string): ProviderLease {
		const lease = [...this.#leases.values()].find(candidate => candidate.leaseId === leaseId);
		if (!lease?.active || lease.connectionId !== connectionId) throw new ReverseLeaseError("not_lease_owner");
		return lease;
	}
	#liveLease(capability: string): ProviderLease | undefined {
		const lease = this.#leases.get(capability);
		if (!lease?.active || lease.expiresAt <= this.#now()) {
			if (lease?.expiresAt !== undefined && lease.expiresAt <= this.#now()) this.#removeDefinitions(capability);
			return undefined;
		}
		return lease;
	}
	#expireStaleLeases(): void {
		for (const lease of this.#leases.values())
			if (lease.expiresAt <= this.#now()) this.#removeDefinitions(lease.capability);
		this.#sweepTerminal();
	}
	#cancelForConnection(connectionId: string, reason: "provider_disconnected" | "lease_released"): void {
		for (const [id, request] of this.#outstanding)
			if (request.connectionId === connectionId) {
				this.#takeOutstanding(id);
				request.reject(new Error("request_cancelled"));
				this.#onCancel?.(id, reason);
			}
	}
	#takeOutstanding(id: string): Outstanding | undefined {
		const request = this.#outstanding.get(id);
		if (!request) return undefined;
		this.#outstanding.delete(id);
		if (request.signal && request.onAbort) request.signal.removeEventListener("abort", request.onAbort);
		return request;
	}
	#sweepTerminal(): void {
		const now = this.#now();
		for (const [id, terminal] of this.#terminal)
			if (terminal.expiresAt <= now) this.#terminal.delete(id);
	}
	#installDefinitionsFor(capability: string, definitions: unknown): string[] {
		const installedMethods = normalizedMethods(this.#installDefinitions?.(capability, definitions));
		this.#installedCapabilities.add(capability);
		return installedMethods;
	}
	#removeDefinitions(capability: string): void {
		if (!this.#installedCapabilities.delete(capability)) return;
		this.#onDefinitionsRemoved?.(capability);
	}
	#assertPayload(payload: unknown): void {
		const encoded = JSON.stringify(payload);
		if (encoded !== undefined && Buffer.byteLength(encoded) > MAX_REVERSE_PAYLOAD_BYTES)
			throw new ReverseLeaseError("payload_too_large");
	}
}
