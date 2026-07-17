import { useState, type ReactNode } from "react";
import { createPortal } from "react-dom";

/**
 * A lightweight tooltip that FOLLOWS THE CURSOR rather than anchoring to the
 * center of its trigger (which is what a Radix Tooltip does). Used for the
 * gated controls in the rule editor — "+ Add column" / "Advanced" / the
 * disabled column picker — where the trigger can be wide and a centered tooltip
 * reads as detached from the pointer.
 *
 * Renders *children* inline (wrapped in a span that captures mouse movement)
 * and, while hovered, portals a small styled bubble to `document.body`
 * positioned a few px off the cursor. `text` is required; when empty the
 * tooltip is inert (children render with no hover behavior), so callers can
 * pass `undefined`/"" to disable it without branching.
 */
export function CursorTooltip({ text, children }: { text?: string; children: ReactNode }) {
  const [pos, setPos] = useState<{ x: number; y: number } | null>(null);

  if (!text) return <>{children}</>;

  return (
    <>
      <span
        className="contents"
        onMouseMove={(e) => setPos({ x: e.clientX, y: e.clientY })}
        onMouseLeave={() => setPos(null)}
      >
        {children}
      </span>
      {pos &&
        createPortal(
          <div
            role="tooltip"
            // `fixed` + viewport coords track the cursor; offset down-right so
            // the pointer never covers the text. pointer-events-none so the
            // bubble never steals the hover it depends on.
            style={{ position: "fixed", top: pos.y + 14, left: pos.x + 12, zIndex: 100 }}
            className="pointer-events-none max-w-xs rounded-md bg-primary px-2.5 py-1.5 text-xs text-primary-foreground shadow-md"
          >
            {text}
          </div>,
          document.body,
        )}
    </>
  );
}
