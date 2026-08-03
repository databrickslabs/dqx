import { useEffect, useRef, useState } from "react";
import { cn } from "@/lib/utils";

/**
 * A number that "ticks" to its new value one step at a time (e.g. 9 → 8 → 7 →
 * 6) rather than snapping. Used for the "N / M selected" pack counts so a
 * filter change that shifts a total reads as a count-down / count-up. Steps
 * are ~45ms apart, and a large jump is paced into ~12 steps so it still
 * resolves quickly.
 */
export function AnimatedCount({ value, className }: { value: number; className?: string }) {
  const [display, setDisplay] = useState(value);
  // Mirror the latest displayed value so the interval reads it without making
  // `display` an effect dependency (which would restart the tick every frame).
  const displayRef = useRef(value);
  displayRef.current = display;

  useEffect(() => {
    if (displayRef.current === value) return undefined;
    const stepSize = Math.max(1, Math.ceil(Math.abs(value - displayRef.current) / 12));

    const id = setInterval(() => {
      const cur = displayRef.current;
      const dir = Math.sign(value - cur);
      if (dir === 0) {
        clearInterval(id);
        return;
      }
      const next = cur + dir * stepSize;
      setDisplay(dir > 0 ? Math.min(next, value) : Math.max(next, value));
    }, 45);

    return () => clearInterval(id);
  }, [value]);

  return <span className={cn("tabular-nums", className)}>{display}</span>;
}
