import type * as React from "react";

/**
 * A controlled show/hide region. The trigger lives outside — callers drive
 * `open` themselves (e.g. a chevron in a chart/table heading).
 *
 * No height animation: the height-transition attempts (grid-template-rows,
 * JS-measured height) all came out jerky for the chart-in-a-card bodies, so we
 * just toggle visibility instantly. The body stays mounted but is hidden +
 * inert when closed.
 */
export function CollapseRegion({
  open,
  children,
}: {
  open: boolean;
  children: React.ReactNode;
}) {
  return (
    <div
      data-collapse-region=""
      data-state={open ? "open" : "closed"}
      hidden={!open}
      aria-hidden={!open}
      inert={!open ? true : undefined}
    >
      {children}
    </div>
  );
}
