import { useCallback, useEffect, useRef, useState } from "react";

// Ported from dqlake's `components/results/useGeniePanelWidth.ts` (the spec —
// keep the drag/clamp/persist semantics aligned with it). Sanctioned
// deviation: the localStorage key is renamed dqlake.* -> dqxStudio.*.

const STORAGE_KEY = "dqxStudio.genie.panelWidth";
const DEFAULT_WIDTH = 640; // wider by default so Ask Genie has room for tables/charts
const MIN_WIDTH = 360;
const MAX_WIDTH = 1100;

function clamp(px: number): number {
  // Never wider than (almost) the viewport, regardless of the stored value.
  const viewportCap =
    typeof window !== "undefined"
      ? Math.max(MIN_WIDTH, window.innerWidth - 80)
      : MAX_WIDTH;
  return Math.min(Math.max(px, MIN_WIDTH), Math.min(MAX_WIDTH, viewportCap));
}

function load(): number {
  if (typeof localStorage === "undefined") return DEFAULT_WIDTH;
  const raw = Number(localStorage.getItem(STORAGE_KEY));
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_WIDTH;
}

function persist(px: number) {
  try {
    localStorage.setItem(STORAGE_KEY, String(px));
  } catch {
    // Quota / private mode — keep the in-memory width.
  }
}

/**
 * Drag-to-resize width for the Genie side panel. Returns the current width, a
 * pointer-down handler to start a drag from the inner edge, and whether a drag
 * is in progress. The chosen width persists to localStorage so it sticks across
 * reopens. The panel is right-anchored, so dragging the LEFT handle leftward
 * widens it: width grows as the pointer moves left.
 */
export function useGeniePanelWidth() {
  const [width, setWidth] = useState<number>(() => clamp(load()));
  const [dragging, setDragging] = useState(false);
  const startX = useRef(0);
  const startWidth = useRef(0);

  const onPointerDown = useCallback(
    (e: React.PointerEvent) => {
      e.preventDefault();
      startX.current = e.clientX;
      startWidth.current = width;
      setDragging(true);
    },
    [width],
  );

  useEffect(() => {
    if (!dragging) return;
    const onMove = (e: PointerEvent) => {
      // Right-anchored panel: moving left (negative delta) should widen it.
      setWidth(clamp(startWidth.current + (startX.current - e.clientX)));
    };
    const onUp = () => setDragging(false);
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp, { once: true });
    return () => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };
  }, [dragging]);

  // Persist the width whenever it settles (cheap; localStorage write).
  useEffect(() => {
    if (!dragging) persist(width);
  }, [dragging, width]);

  return { width, dragging, onPointerDown };
}
