import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";
import { parseHexInput, type ColorGroup, type GroupColors } from "@/lib/branding";

/** Display order, top of the app to the content. */
const PICKER_ORDER: readonly ColorGroup[] = ["header", "sidebar", "page_background", "brand", "text"];

interface HexFieldProps {
  id: string;
  value: string;
  disabled?: boolean;
  label: string;
  placeholder?: string;
  onCommit: (hex: string) => void;
}

/**
 * Free-text hex entry. Accepts #RRGGBB, 3-digit shorthand and pasted values with spaces;
 * anything else reverts to the current value and shows an inline hint.
 */
function HexField({ id, value, disabled, label, placeholder, onCommit }: HexFieldProps) {
  const { t } = useTranslation();
  const [text, setText] = useState(value);
  const [invalid, setInvalid] = useState(false);
  const errorId = `${id}-error`;
  const commit = () => {
    if (!text.trim()) {
      setInvalid(false);
      setText(value);
      return;
    }
    const next = parseHexInput(text);
    if (next) {
      setInvalid(false);
      setText(next);
      if (next !== value) onCommit(next);
    } else {
      setInvalid(true);
      setText(value);
    }
  };
  return (
    <div className="flex flex-col items-end gap-1">
      <Input
        id={id}
        value={text}
        disabled={disabled}
        aria-label={label}
        aria-invalid={invalid}
        aria-describedby={invalid ? errorId : undefined}
        spellCheck={false}
        placeholder={placeholder}
        maxLength={32}
        onChange={(e) => {
          setText(e.target.value);
          setInvalid(false);
        }}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === "Enter") commit();
        }}
        className="h-8 w-24 font-mono text-xs uppercase"
      />
      {invalid && (
        <p id={errorId} className="text-[11px] text-destructive">
          {t("config.styling.hexInvalid")}
        </p>
      )}
    </div>
  );
}

interface ColorGroupPickerProps {
  /** Prefix keeping input ids unique per mode. */
  idPrefix: string;
  colors: GroupColors;
  /** Shown for groups the draft leaves unset (or, with *showUnset*, where the colour picker starts). */
  defaults: Record<ColorGroup, string>;
  /** Show unset groups as "Not set" instead of their default colour (a new theme). */
  showUnset?: boolean;
  disabled?: boolean;
  onChange: (group: ColorGroup, hex: string) => void;
  className?: string;
}

/** Rows share any extra height evenly, so the list can stretch to match a taller neighbour. */
export function ColorGroupPicker({ idPrefix, colors, defaults, showUnset, disabled, onChange, className }: ColorGroupPickerProps) {
  const { t } = useTranslation();
  return (
    <div className={cn("flex flex-col divide-y rounded-md border", className)}>
      {PICKER_ORDER.map((group) => {
        const unset = !!showUnset && colors[group] === undefined;
        const value = (colors[group] ?? defaults[group]).toUpperCase();
        const label = t(`config.styling.group_${group}`);
        const id = `${idPrefix}-${group}`;
        return (
          <div key={group} className="flex flex-1 items-center justify-between gap-4 px-3 py-2">
            <div className="min-w-0 space-y-0.5">
              <Label htmlFor={id} className="text-sm">
                {label}
              </Label>
              <p className="text-[11px] text-muted-foreground">{t(`config.styling.group_${group}_help`)}</p>
            </div>
            <div className="flex items-start gap-2">
              <div className="relative h-8 w-10">
                {unset && (
                  <span
                    aria-hidden="true"
                    className="absolute inset-0 rounded-md border border-dashed border-muted-foreground/60"
                  />
                )}
                <input
                  id={id}
                  type="color"
                  value={value.toLowerCase()}
                  disabled={disabled}
                  aria-label={unset ? t("config.styling.colorNotSet", { group: label }) : undefined}
                  onChange={(e) => onChange(group, e.target.value.toUpperCase())}
                  className={cn(
                    "h-8 w-10 cursor-pointer rounded-md border bg-transparent p-0.5 disabled:cursor-not-allowed disabled:opacity-50",
                    // Still clickable, but the dashed "not set" swatch shows instead of the start colour.
                    unset && "relative opacity-0",
                  )}
                />
              </div>
              <HexField
                key={unset ? `unset-${group}` : value}
                id={`${id}-hex`}
                value={unset ? "" : value}
                disabled={disabled}
                label={t("config.styling.hexValueLabel", { group: label })}
                placeholder={t("config.styling.notSet")}
                onCommit={(hex) => onChange(group, hex)}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
