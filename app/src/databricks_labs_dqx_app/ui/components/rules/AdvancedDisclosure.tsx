import { useState, type ReactNode } from "react";
import { ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";

interface AdvancedDisclosureProps {
  defaultOpen?: boolean;
  label: string;
  children: ReactNode;
}

/**
 * Generic collapsible "Advanced" section, ported from dqlake's
 * `AdvancedDisclosure`. Reusable wrapper for optional fields an author
 * doesn't need to see by default — houses only whatever the caller passes
 * in, so it stays agnostic to what's "advanced" for a given rule mode.
 */
export function AdvancedDisclosure({ defaultOpen = false, label, children }: AdvancedDisclosureProps) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="border rounded-lg">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center gap-2 px-4 py-2 text-sm font-medium text-left hover:bg-muted/40"
        aria-expanded={open}
      >
        <ChevronRight className={cn("h-4 w-4 transition-transform", open && "rotate-90")} />
        {label}
      </button>
      {open && <div className="px-4 pb-4 pt-4 border-t space-y-3">{children}</div>}
    </div>
  );
}
