import { Checkbox } from "@/components/ui/checkbox";
import { cn } from "@/lib/utils";

export function OptionRow({
  checked,
  onChange,
  disabled,
  label,
  hint,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  disabled?: boolean;
  label: string;
  hint: string;
}) {
  return (
    <label
      className={cn(
        "flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-colors",
        disabled ? "opacity-60 cursor-not-allowed" : "hover:bg-muted/30",
      )}
    >
      <Checkbox
        checked={checked}
        onCheckedChange={(v) => onChange(Boolean(v))}
        disabled={disabled}
        className="mt-0.5"
      />
      <div className="flex-1 space-y-1">
        <div className="text-sm font-medium">{label}</div>
        <div className="text-xs text-muted-foreground">{hint}</div>
      </div>
    </label>
  );
}
