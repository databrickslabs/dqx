import { useEffect, useState, type ReactNode } from "react";
import { useReducedMotion } from "motion/react";
import { cn } from "@/lib/utils";

/**
 * Animated expand / collapse for rows of an overview `<table>` (grouped
 * views and per-row expanders such as Applied Rules / Applied tables).
 *
 * A `<tr>` can't animate its height, so each cell's content is wrapped in a
 * one-track CSS grid whose row track transitions `0fr` to `1fr` (plus
 * opacity). The cell itself keeps its horizontal padding and the vertical
 * padding moves inside the clipped wrapper, so a collapsed row is zero
 * height. Column widths are untouched: the table stays `table-fixed` with
 * its `<colgroup>`, and the wrappers sit inside the existing cells.
 *
 * Usage: wrap the conditionally rendered row(s) in {@link RowCollapse}, put
 * {@link rowCollapseRowClass} on each `<TableRow>`, move each cell's vertical
 * padding into {@link CollapsibleCellContent}.
 */

/** Transition length; matches Tailwind's `duration-200` used on the wrapper. */
export const ROW_COLLAPSE_DURATION_MS = 200;

/**
 * Groups with more rows than this on the current page expand and collapse
 * instantly: animating hundreds of cells at once costs more than it adds.
 */
export const MAX_ANIMATED_GROUP_ROWS = 50;

/**
 * - *closed*: not rendered.
 * - *entering*: mounted in the collapsed state for one frame, so the
 *   transition has a start value.
 * - *expanding*: transitioning to full height, content clipped.
 * - *open*: settled; nothing clipped (focus rings, badges overflow freely).
 * - *exiting*: transitioning to zero height, then unmounted.
 */
export type RowCollapsePhase = "closed" | "entering" | "expanding" | "open" | "exiting";

/** Phase to enter when *open* flips; skipping the animation jumps straight to the end state. */
export function phaseOnToggle(open: boolean, animate: boolean): RowCollapsePhase {
  if (!animate) return open ? "open" : "closed";
  return open ? "entering" : "exiting";
}

/** The phase a timed step moves to, or *null* when the phase is at rest. */
export function nextPhase(phase: RowCollapsePhase): RowCollapsePhase | null {
  switch (phase) {
    case "entering":
      return "expanding";
    case "expanding":
      return "open";
    case "exiting":
      return "closed";
    default:
      return null;
  }
}

/**
 * The phase a nested expander (e.g. a row's Applied Rules panel) should
 * render with: it follows its parent row while the parent is animating, and
 * its own phase once the parent has settled open.
 */
export function combinePhases(parent: RowCollapsePhase, child: RowCollapsePhase): RowCollapsePhase {
  return parent === "open" ? child : parent;
}

interface CollapseState {
  open: boolean;
  instant: boolean;
  phase: RowCollapsePhase;
}

/**
 * Tracks the collapse phase for *open*. The animation is skipped when
 * *instant* is set now or was set on the previous render (so both turning
 * "expand all while searching" on and clearing it snap rather than
 * mass-animate), and when the user prefers reduced motion.
 */
export function useRowCollapse(open: boolean, instant = false): RowCollapsePhase {
  const reduceMotion = useReducedMotion() ?? false;
  const [state, setState] = useState<CollapseState>(() => ({
    open,
    instant,
    phase: open ? "open" : "closed",
  }));

  // Adjust state during render (React's documented pattern for deriving
  // state from a prop change) so the first committed frame already has the
  // right phase — no flash of the fully open row before the transition.
  let current = state;
  if (state.open !== open || state.instant !== instant) {
    const phase =
      state.open !== open ? phaseOnToggle(open, !(instant || state.instant || reduceMotion)) : state.phase;
    current = { open, instant, phase };
    setState(current);
  }
  const phase = current.phase;

  useEffect(() => {
    const target = nextPhase(phase);
    if (!target) return;
    const advance = () => setState((s) => (s.phase === phase ? { ...s, phase: target } : s));
    if (phase === "entering") {
      // Two frames: the first commits the collapsed styles, the second
      // starts the transition from them.
      let inner = 0;
      const outer = requestAnimationFrame(() => {
        inner = requestAnimationFrame(advance);
      });
      return () => {
        cancelAnimationFrame(outer);
        cancelAnimationFrame(inner);
      };
    }
    const timer = setTimeout(advance, ROW_COLLAPSE_DURATION_MS);
    return () => clearTimeout(timer);
  }, [phase]);

  return phase;
}

export interface RowCollapseProps {
  open: boolean;
  /** Skip the animation (see {@link useRowCollapse}). */
  instant?: boolean;
  /** Renders the row(s) for the current (mounted) phase. */
  children: (phase: RowCollapsePhase) => ReactNode;
}

/** Keeps row(s) mounted while they animate closed, and unmounts them afterwards. */
export function RowCollapse({ open, instant = false, children }: RowCollapseProps) {
  const phase = useRowCollapse(open, instant);
  if (phase === "closed") return null;
  return <>{children(phase)}</>;
}

/** Extra `<TableRow>` classes: an exiting row no longer takes clicks or hover. */
export function rowCollapseRowClass(phase: RowCollapsePhase): string | undefined {
  return phase === "exiting" ? "pointer-events-none" : undefined;
}

export interface CollapsibleCellContentProps {
  phase: RowCollapsePhase;
  /** Classes for the content box — put the cell's vertical padding here. */
  className?: string;
  children?: ReactNode;
}

/** Height + opacity animated wrapper for one cell's content. */
export function CollapsibleCellContent({ phase, className, children }: CollapsibleCellContentProps) {
  const shown = phase === "expanding" || phase === "open";
  return (
    <div
      className={cn(
        // minmax(0, 1fr) keeps the track at the cell's width so truncated
        // text still truncates instead of widening the track.
        "grid grid-cols-[minmax(0,1fr)] transition-[grid-template-rows,opacity] duration-200 ease-out motion-reduce:transition-none",
        shown ? "grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0",
      )}
    >
      <div className={cn("min-h-0", phase !== "open" && "overflow-hidden")}>
        <div className={className}>{children}</div>
      </div>
    </div>
  );
}
