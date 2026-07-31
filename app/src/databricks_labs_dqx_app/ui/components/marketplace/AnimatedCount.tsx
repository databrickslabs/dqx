import { useEffect, useRef, useState } from "react";
import { cn } from "@/lib/utils";

/**
 * Renders a number that briefly pops (scale + colour) whenever its value
 * changes — used for the "N / M selected" pack counts so a filter change that
 * shifts the totals is visible rather than silently swapping digits.
 * Respects prefers-reduced-motion via the CSS transition only.
 */
export function AnimatedCount({ value, className }: { value: number; className?: string }) {
  const [pop, setPop] = useState(false);
  const prev = useRef(value);

  useEffect(() => {
    if (prev.current !== value) {
      prev.current = value;
      setPop(true);
      const id = setTimeout(() => setPop(false), 220);
      return () => clearTimeout(id);
    }
  }, [value]);

  return (
    <span
      className={cn(
        "inline-block tabular-nums transition-all duration-200 ease-out",
        pop ? "-translate-y-0.5 scale-110 text-foreground" : "scale-100",
        className,
      )}
    >
      {value}
    </span>
  );
}
