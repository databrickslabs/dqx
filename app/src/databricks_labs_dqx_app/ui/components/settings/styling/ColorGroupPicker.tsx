import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { COLOR_GROUPS, parseHexInput, type ColorGroup, type GroupColors } from "@/lib/branding";

interface HexFieldProps {
  id: string;
  value: string;
  disabled?: boolean;
  label: string;
  onCommit: (hex: string) => void;
}

/**
 * Free-text hex entry. Accepts #RRGGBB, 3-digit shorthand and pasted values with spaces;
 * anything else reverts to the current value and shows an inline hint.
 */
function HexField({ id, value, disabled, label, onCommit }: HexFieldProps) {
  const { t } = useTranslation();
  const [text, setText] = useState(value);
  const [invalid, setInvalid] = useState(false);
  const errorId = `${id}-error`;
  const commit = () => {
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
  /** Shown for groups the draft leaves unset. */
  defaults: Record<ColorGroup, string>;
  disabled?: boolean;
  onChange: (group: ColorGroup, hex: string) => void;
}

export function ColorGroupPicker({ idPrefix, colors, defaults, disabled, onChange }: ColorGroupPickerProps) {
  const { t } = useTranslation();
  return (
    <div className="divide-y rounded-md border">
      {COLOR_GROUPS.map((group) => {
        const value = (colors[group] ?? defaults[group]).toUpperCase();
        const label = t(`config.styling.group_${group}`);
        const id = `${idPrefix}-${group}`;
        return (
          <div key={group} className="flex items-center justify-between gap-4 px-3 py-2">
            <div className="min-w-0 space-y-0.5">
              <Label htmlFor={id} className="text-sm">
                {label}
              </Label>
              <p className="text-[11px] text-muted-foreground">{t(`config.styling.group_${group}_help`)}</p>
            </div>
            <div className="flex items-start gap-2">
              <input
                id={id}
                type="color"
                value={value.toLowerCase()}
                disabled={disabled}
                onChange={(e) => onChange(group, e.target.value.toUpperCase())}
                className="h-8 w-10 cursor-pointer rounded-md border bg-transparent p-0.5 disabled:cursor-not-allowed disabled:opacity-50"
              />
              <HexField key={value} id={`${id}-hex`} value={value} disabled={disabled} label={t("config.styling.hexValueLabel", { group: label })} onCommit={(hex) => onChange(group, hex)} />
            </div>
          </div>
        );
      })}
    </div>
  );
}
