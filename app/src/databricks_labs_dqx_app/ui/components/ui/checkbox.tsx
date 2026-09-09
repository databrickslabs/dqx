import * as React from "react";
import { cn } from "@/lib/utils";
import { Check, Minus } from "lucide-react";

interface CheckboxProps extends Omit<React.InputHTMLAttributes<HTMLInputElement>, "checked" | "onChange"> {
  checked?: boolean | "indeterminate";
  onCheckedChange?: (checked: boolean) => void;
}

const Checkbox = React.forwardRef<HTMLButtonElement, CheckboxProps>(
  ({ className, checked = false, onCheckedChange, disabled, onClick, ...props }, ref) => {
    const isChecked = checked === true;
    const isIndeterminate = checked === "indeterminate";

    return (
      <button
        ref={ref}
        type="button"
        role="checkbox"
        aria-checked={isIndeterminate ? "mixed" : isChecked}
        disabled={disabled}
        onClick={(e) => {
          onClick?.(e as any);
          onCheckedChange?.(!isChecked);
        }}
        className={cn(
          "peer h-4 w-4 shrink-0 rounded-[4px] border border-primary shadow-xs",
          "focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring",
          "disabled:cursor-not-allowed disabled:opacity-50",
          (isChecked || isIndeterminate) && "bg-primary text-primary-foreground",
          className,
        )}
        {...(props as any)}
      >
        {/* Glyph deliberately smaller than the h-4 w-4 box (item 5 — the
            "Add tables" wizard's checkboxes read as oversized): a 14px
            glyph in a 16px box only has the 1px border to breathe
            against, so at strokeWidth 3 the tick/dash fills the box
            edge-to-edge. 12px at a lighter stroke leaves visible padding
            on every side, matching how shadcn's checkbox actually reads
            rather than its literal className. */}
        {isChecked && <Check className="h-3 w-3 mx-auto" strokeWidth={2.5} />}
        {isIndeterminate && <Minus className="h-3 w-3 mx-auto" strokeWidth={2.5} />}
      </button>
    );
  },
);
Checkbox.displayName = "Checkbox";

export { Checkbox };
