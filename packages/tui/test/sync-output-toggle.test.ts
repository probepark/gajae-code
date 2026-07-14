import { describe, expect, it } from "bun:test";
import { type Component, TUI } from "@gajae-code/tui";
import { VirtualTerminal } from "./virtual-terminal";

const SYNC_BEGIN = "\x1b[?2026h";
const SYNC_END = "\x1b[?2026l";

class MutableLines implements Component {
	#lines: string[];
	constructor(lines: string[]) {
		this.#lines = lines.slice();
	}
	setLine(index: number, value: string): void {
		const next = this.#lines.slice();
		next[index] = value;
		this.#lines = next;
	}
	invalidate(): void {}
	render(_width: number): string[] {
		return this.#lines;
	}
}

async function settle(term: VirtualTerminal): Promise<void> {
	await new Promise<void>(resolve => process.nextTick(resolve));
	await Bun.sleep(30);
	await term.flush();
}

/**
 * Render a Hangul line and then a differential update to another Hangul line,
 * returning the emitted terminal bytes and xterm's rendered viewport. The
 * `#syncOutput` flag is read in the TUI constructor, so the env var is set
 * before construction to exercise each branch in-process.
 */
async function renderHangulDiff(flag: string | undefined): Promise<{ writeLog: string; viewport: string[] }> {
	const prev = Bun.env.GJC_TUI_SYNC_OUTPUT;
	const prevTmux = Bun.env.TMUX;
	const prevSty = Bun.env.STY;
	const prevZellij = Bun.env.ZELLIJ;
	if (flag === undefined) delete Bun.env.GJC_TUI_SYNC_OUTPUT;
	else Bun.env.GJC_TUI_SYNC_OUTPUT = flag;
	delete Bun.env.TMUX;
	delete Bun.env.STY;
	delete Bun.env.ZELLIJ;
	try {
		const term = new VirtualTerminal(40, 8);
		const tui = new TUI(term);
		const component = new MutableLines(["다다다다", "line-b", "line-c"]);
		tui.addChild(component);
		tui.start();
		await settle(term);
		term.clearWriteLog();
		component.setLine(0, "라라라라");
		tui.requestRender();
		await settle(term);
		const writeLog = term.getWriteLog().join("");
		const viewport = term.getViewport();
		tui.stop();
		return { writeLog, viewport };
	} finally {
		if (prev === undefined) delete Bun.env.GJC_TUI_SYNC_OUTPUT;
		else Bun.env.GJC_TUI_SYNC_OUTPUT = prev;
		if (prevTmux === undefined) delete Bun.env.TMUX;
		else Bun.env.TMUX = prevTmux;
		if (prevSty === undefined) delete Bun.env.STY;
		else Bun.env.STY = prevSty;
		if (prevZellij === undefined) delete Bun.env.ZELLIJ;
		else Bun.env.ZELLIJ = prevZellij;
	}
}

describe("GJC_TUI_SYNC_OUTPUT toggle", () => {
	it("wraps frames in mode 2026 by default and renders Hangul correctly", async () => {
		const { writeLog, viewport } = await renderHangulDiff(undefined);
		expect(writeLog).toContain(SYNC_BEGIN);
		expect(writeLog).toContain(SYNC_END);
		expect(viewport[0]?.trimEnd()).toBe("라라라라");
	});

	it("omits mode 2026 when opted out, changing only the wrapper bytes", async () => {
		const on = await renderHangulDiff(undefined);
		const off = await renderHangulDiff("0");

		// The opt-out path must not emit the synchronized-output markers...
		expect(off.writeLog).not.toContain(SYNC_BEGIN);
		expect(off.writeLog).not.toContain(SYNC_END);

		// ...and the ONLY difference from the default frame is those markers:
		// content between them (clears, cursor moves, Hangul) is byte-identical.
		expect(off.writeLog).toBe(on.writeLog.replaceAll(SYNC_BEGIN, "").replaceAll(SYNC_END, ""));

		// A correct emulator renders the same visible result either way.
		expect(off.viewport).toEqual(on.viewport);
		expect(off.viewport[0]?.trimEnd()).toBe("라라라라");
	});
});
