import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import {
	ackCodexWakeEvent,
	listCodexWakeEvents,
	readCodexHandoff,
	recordCodexWakeEvent,
	registerCodexHandoff,
	updateCodexWakeEvent,
} from "../src/coordinator-mcp/codex-handoff";

const tempDirs: string[] = [];

async function tempRoot(): Promise<string> {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-codex-handoff-"));
	tempDirs.push(root);
	return root;
}

async function persistedText(root: string): Promise<string> {
	const entries = await fs.readdir(root, { withFileTypes: true });
	const values = await Promise.all(
		entries.map(async entry => {
			const file = path.join(root, entry.name);
			return entry.isDirectory() ? persistedText(file) : fs.readFile(file, "utf8");
		}),
	);
	return values.join("\n");
}

afterEach(async () => {
	await Promise.all(tempDirs.splice(0).map(root => fs.rm(root, { recursive: true, force: true })));
});

describe("Codex handoff durable state", () => {
	it("suppresses duplicate wake events without changing the original event", async () => {
		const root = await tempRoot();
		const input = {
			work_unit: "session-1",
			event_seq: 3,
			event_kind: "turn.completed" as const,
			summary: "first completion",
		};
		const first = await recordCodexWakeEvent(root, input);
		const duplicate = await recordCodexWakeEvent(root, { ...input, summary: "changed summary" });

		expect(first.created).toBe(true);
		expect(duplicate.created).toBe(false);
		expect(JSON.stringify(duplicate.event)).toBe(JSON.stringify(first.event));
	});

	it("persists registrations and wake state across fresh reads", async () => {
		const root = await tempRoot();
		await registerCodexHandoff(root, {
			work_unit: "session-2",
			thread_id: "thread-2",
			endpoint: { kind: "unix", path: "/tmp/codex.sock" },
		});
		const wake = await recordCodexWakeEvent(root, {
			work_unit: "session-2",
			event_seq: 4,
			event_kind: "question.opened",
			question_id: "question-2",
			summary: "A question needs an answer.",
		});

		expect((await readCodexHandoff(root, "session-2"))?.thread_id).toBe("thread-2");
		expect((await listCodexWakeEvents(root, "session-2"))[0]?.status).toBe("pending");
		await ackCodexWakeEvent(root, wake.event.key);
		expect((await listCodexWakeEvents(root, "session-2"))[0]?.status).toBe("acked");
	});
	it("enforces terminal wake state and bounds durable wake summaries", async () => {
		const root = await tempRoot();
		const wake = await recordCodexWakeEvent(root, {
			work_unit: "session-3",
			event_seq: 5,
			event_kind: "turn.completed",
			summary: `completed\n${"x".repeat(300)}`,
		});
		expect(wake.event.summary).toHaveLength(240);
		expect(wake.event.summary).not.toContain("\n");

		await updateCodexWakeEvent(root, wake.event.key, { status: "published" });
		const published = await updateCodexWakeEvent(root, wake.event.key, {
			status: "pending",
			attempts_delta: 1,
		});
		expect(published).toMatchObject({ status: "published", attempts: 1 });
		const acked = await ackCodexWakeEvent(root, wake.event.key);
		expect(await updateCodexWakeEvent(root, wake.event.key, { status: "failed", attempts_delta: 1 })).toEqual(acked);
	});

	it("rejects invalid work units and missing wake acknowledgements", async () => {
		const root = await tempRoot();
		await expect(
			recordCodexWakeEvent(root, {
				work_unit: "../not-safe",
				event_seq: 1,
				event_kind: "turn.failed",
				summary: "failed",
			}),
		).rejects.toThrow("invalid_work_unit");
		await expect(ackCodexWakeEvent(root, "missing:1")).rejects.toThrow("resource_gone");
	});

	it("stores token-file references without persisting token material", async () => {
		const root = await tempRoot();
		const token = "actual-codex-token-material";
		const tokenDir = await tempRoot();
		const tokenFile = path.join(tokenDir, "token.txt");
		await fs.writeFile(tokenFile, token, { mode: 0o600 });
		await registerCodexHandoff(root, {
			work_unit: "session-3",
			thread_id: "thread-3",
			endpoint: { kind: "unix", path: "/tmp/codex.sock" },
			token_file: tokenFile,
		});

		const state = await persistedText(root);
		expect(state).not.toContain(token);
		expect(state).toContain(tokenFile);
		await expect(
			registerCodexHandoff(root, {
				work_unit: "session-4",
				thread_id: "thread-4",
				endpoint: { kind: "unix", path: "/tmp/codex.sock" },
				token_file: "./token.txt",
			}),
		).rejects.toThrow("token_material_not_allowed");
	});
});
