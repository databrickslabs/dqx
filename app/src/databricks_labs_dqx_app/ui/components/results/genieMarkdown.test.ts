import { describe, expect, it } from "bun:test";
import { isValidElement, type ReactElement } from "react";
import { renderInline, toBlocks } from "./GenieMarkdown";

// Pure-logic tests for the safe markdown renderer: block grouping and inline
// tokenisation. React elements are inspected as plain objects — no DOM.

describe("toBlocks", () => {
  it("groups consecutive text lines into one paragraph", () => {
    expect(toBlocks("line one\nline two")).toEqual([
      { kind: "p", lines: ["line one", "line two"] },
    ]);
  });

  it("splits paragraphs on blank lines", () => {
    expect(toBlocks("a\n\nb")).toEqual([
      { kind: "p", lines: ["a"] },
      { kind: "p", lines: ["b"] },
    ]);
  });

  it("keeps a headline and its breakdown as separate paragraph blocks (P6.2 answer shape)", () => {
    // The space instructions demand: one-sentence headline, blank line,
    // breakdown paragraphs. The renderer must keep them as distinct <p>
    // blocks (the space-y container gives them visible spacing).
    const answer =
      "The pass rate is 91.5%.\n\nCompleteness drives the gap: 1,250 of 50,000 tests failed (2.5%).\nTwo rules worsened since the prior run.";
    expect(toBlocks(answer)).toEqual([
      { kind: "p", lines: ["The pass rate is 91.5%."] },
      {
        kind: "p",
        lines: [
          "Completeness drives the gap: 1,250 of 50,000 tests failed (2.5%).",
          "Two rules worsened since the prior run.",
        ],
      },
    ]);
  });

  it("collects - and * bullets into one ul", () => {
    expect(toBlocks("- one\n* two")).toEqual([
      { kind: "ul", items: ["one", "two"] },
    ]);
  });

  it("collects numbered lines into one ol", () => {
    expect(toBlocks("1. first\n2. second")).toEqual([
      { kind: "ol", items: ["first", "second"] },
    ]);
  });

  it("interleaves paragraphs and lists", () => {
    expect(toBlocks("intro\n- a\n- b\noutro")).toEqual([
      { kind: "p", lines: ["intro"] },
      { kind: "ul", items: ["a", "b"] },
      { kind: "p", lines: ["outro"] },
    ]);
  });

  it("normalises CRLF and drops empty trailing blocks", () => {
    expect(toBlocks("a\r\n\r\n")).toEqual([{ kind: "p", lines: ["a"] }]);
    expect(toBlocks("")).toEqual([]);
  });
});

/** Tag name of an inline node, or null for a plain string. */
function tagOf(node: unknown): string | null {
  return isValidElement(node) ? String(node.type) : null;
}

/** The rendered text content of an inline node. */
function textOf(node: unknown): string {
  if (typeof node === "string") return node;
  if (isValidElement(node)) {
    return String((node as ReactElement<{ children?: unknown }>).props.children);
  }
  return "";
}

describe("renderInline", () => {
  it("passes plain text through untouched", () => {
    const out = renderInline("plain text", "k");
    expect(out).toEqual(["plain text"]);
  });

  it("renders **bold** and __bold__ as <strong>", () => {
    for (const src of ["**bold**", "__bold__"]) {
      const out = renderInline(src, "k");
      expect(out).toHaveLength(1);
      expect(tagOf(out[0])).toBe("strong");
      expect(textOf(out[0])).toBe("bold");
    }
  });

  it("renders *italic* and _italic_ as <em>", () => {
    for (const src of ["*italic*", "_italic_"]) {
      const out = renderInline(src, "k");
      expect(tagOf(out[0])).toBe("em");
      expect(textOf(out[0])).toBe("italic");
    }
  });

  it("renders `code` as <code> and keeps * literal inside backticks", () => {
    const out = renderInline("run `SELECT *` now", "k");
    expect(out.map(tagOf)).toEqual([null, "code", null]);
    expect(textOf(out[1])).toBe("SELECT *");
  });

  it("mixes tokens and surrounding text in order", () => {
    const out = renderInline("a **b** c *d*", "k");
    expect(out.map(tagOf)).toEqual([null, "strong", null, "em"]);
    expect(out.map(textOf)).toEqual(["a ", "b", " c ", "d"]);
  });

  it("never emits raw HTML (angle brackets stay literal strings)", () => {
    const out = renderInline("<img src=x onerror=alert(1)>", "k");
    expect(out).toEqual(["<img src=x onerror=alert(1)>"]);
  });
});
