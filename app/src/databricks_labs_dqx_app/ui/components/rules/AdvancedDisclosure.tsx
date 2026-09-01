import { useState, type ReactNode } from "react";
import { ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";

interface AdvancedDisclosureProps {
  defaultOpen?: boolean;
  label: string;
  children: ReactNode;
  /** When true the section can't be expanded — the header shows greyed-out and
   * non-interactive. The caller supplies any explanatory tooltip (e.g. a
   * cursor-following one) by wrapping this component. Used to gate "Advanced"
   * until the author has picked a rule condition. */
  disabled?: boolean;
}

/**
 * Generic collapsible "Advanced" section, ported from dqlake's
 * `AdvancedDisclosure`. Reusable wrapper for optional fields an author
 * doesn't need to see by default — houses only whatever the caller passes
 * in, so it stays agnostic to what's "advanced" for a given rule mode.
 */
export function AdvancedDisclosure({ defaultOpen = false, label, children, disabled }: AdvancedDisclosureProps) {
  const [open, setOpen] = useState(defaultOpen);
  const effectiveOpen = open && !disabled;
  return (
    <div className="border rounded-lg">
      <button
        type="button"
        onClick={() => !disabled && setOpen((v) => !v)}
        disabled={disabled}
        className={cn(
          "w-full flex items-center gap-2 px-4 py-2 text-xs font-medium text-left",
          disabled ? "opacity-50 cursor-not-allowed" : "hover:bg-muted/40",
        )}
        aria-expanded={effectiveOpen}
      >
        <ChevronRight className={cn("h-4 w-4 transition-transform", effectiveOpen && "rotate-90")} />
        {label}
      </button>
      {/* grid-rows transition trick — same pattern as RuleConfigCard /
          RulesByColumn's disclosures — animates both open AND closed, unlike
          a plain `{open && <div>}` conditional render. */}
      <div
        className={cn(
          "grid transition-[grid-template-rows] duration-200 ease-out",
          open ? "grid-rows-[1fr]" : "grid-rows-[0fr]",
        )}
      >
        <div className="overflow-hidden">
          <div className="px-4 pb-4 pt-4 border-t space-y-3">{children}</div>
        </div>
      </div>
    </div>
  );
}
