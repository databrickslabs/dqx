import { Fragment, type ReactNode } from "react";

// Ported from dqlake's `components/results/GenieMarkdown.tsx` (the spec —
// keep the supported subset and safety posture aligned with it). The pure
// helpers (`toBlocks`, `renderInline`) are exported for the bun tests.

/**
 * A deliberately tiny, safe Markdown renderer for Genie answer text. It covers
 * the subset Genie actually emits — **bold**, *italic*, `inline code`, bullet
 * and numbered lists, and paragraph/line breaks — and nothing else. We render
 * React nodes directly (never dangerouslySetInnerHTML), so there is no HTML
 * injection surface. A heavier dependency isn't justified for this scope.
 */

/** Split a single line of text into bold / italic / inline-code spans. */
export function renderInline(text: string, keyPrefix: string): ReactNode[] {
  // Order matters: code first (so * and _ inside backticks are literal), then
  // bold (** or __), then italic (* or _).
  const tokenRe = /(`[^`]+`|\*\*[^*]+\*\*|__[^_]+__|\*[^*]+\*|_[^_]+_)/g;
  const out: ReactNode[] = [];
  let last = 0;
  let m: RegExpExecArray | null;
  let i = 0;
  while ((m = tokenRe.exec(text)) !== null) {
    if (m.index > last) out.push(text.slice(last, m.index));
    const tok = m[0];
    const key = `${keyPrefix}-${i++}`;
    if (tok.startsWith("`")) {
      out.push(
        <code
          key={key}
          className="rounded bg-background px-1 py-0.5 font-mono text-[0.85em]"
        >
          {tok.slice(1, -1)}
        </code>,
      );
    } else if (tok.startsWith("**") || tok.startsWith("__")) {
      out.push(
        <strong key={key} className="font-medium">
          {tok.slice(2, -2)}
        </strong>,
      );
    } else {
      out.push(<em key={key}>{tok.slice(1, -1)}</em>);
    }
    last = m.index + tok.length;
  }
  if (last < text.length) out.push(text.slice(last));
  return out;
}

export type Block =
  | { kind: "p"; lines: string[] }
  | { kind: "ul"; items: string[] }
  | { kind: "ol"; items: string[] };

/** Group raw lines into paragraph / unordered / ordered blocks. */
export function toBlocks(src: string): Block[] {
  const lines = src.replace(/\r\n/g, "\n").split("\n");
  const blocks: Block[] = [];
  for (const raw of lines) {
    const line = raw.trimEnd();
    const bullet = /^\s*[-*]\s+(.*)$/.exec(line);
    const numbered = /^\s*\d+\.\s+(.*)$/.exec(line);
    if (bullet) {
      const prev = blocks[blocks.length - 1];
      if (prev && prev.kind === "ul") prev.items.push(bullet[1]);
      else blocks.push({ kind: "ul", items: [bullet[1]] });
    } else if (numbered) {
      const prev = blocks[blocks.length - 1];
      if (prev && prev.kind === "ol") prev.items.push(numbered[1]);
      else blocks.push({ kind: "ol", items: [numbered[1]] });
    } else if (line.trim() === "") {
      // Blank line ends the current paragraph.
      const prev = blocks[blocks.length - 1];
      if (prev && prev.kind === "p") blocks.push({ kind: "p", lines: [] });
    } else {
      const prev = blocks[blocks.length - 1];
      if (prev && prev.kind === "p") prev.lines.push(line);
      else blocks.push({ kind: "p", lines: [line] });
    }
  }
  return blocks.filter((b) => (b.kind === "p" ? b.lines.length > 0 : b.items.length > 0));
}

export function GenieMarkdown({ text }: { text: string }) {
  const blocks = toBlocks(text);
  return (
    <div className="space-y-2 font-light leading-relaxed">
      {blocks.map((b, i) => {
        if (b.kind === "ul") {
          return (
            <ul key={i} className="list-disc space-y-0.5 pl-5">
              {b.items.map((it, j) => (
                <li key={j}>{renderInline(it, `${i}-${j}`)}</li>
              ))}
            </ul>
          );
        }
        if (b.kind === "ol") {
          return (
            <ol key={i} className="list-decimal space-y-0.5 pl-5">
              {b.items.map((it, j) => (
                <li key={j}>{renderInline(it, `${i}-${j}`)}</li>
              ))}
            </ol>
          );
        }
        return (
          <p key={i}>
            {b.lines.map((ln, j) => (
              <Fragment key={j}>
                {j > 0 && <br />}
                {renderInline(ln, `${i}-${j}`)}
              </Fragment>
            ))}
          </p>
        );
      })}
    </div>
  );
}
