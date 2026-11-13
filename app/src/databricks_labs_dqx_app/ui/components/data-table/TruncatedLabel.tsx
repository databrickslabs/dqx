import { useState } from "react";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { isClipped } from "@/components/data-table/truncation";

/**
 * Single-line truncating label that shows an instant tooltip with the full
 * text, but only when the text is actually clipped (replaces slow native
 * `title=` hovers).
 */
export function TruncatedLabel({ text, className }: { text: string; className?: string }) {
  const [open, setOpen] = useState(false);

  return (
    <Tooltip open={open} delayDuration={0}>
      <TooltipTrigger asChild>
        <span
          className={cn("truncate", className)}
          onPointerEnter={(e) => setOpen(isClipped(e.currentTarget))}
          onPointerLeave={() => setOpen(false)}
        >
          {text}
        </span>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs break-words">{text}</TooltipContent>
    </Tooltip>
  );
}
