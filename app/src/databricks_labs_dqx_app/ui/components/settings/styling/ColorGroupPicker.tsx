import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { COLOR_GROUPS, isHex, type ColorGroup, type GroupColors } from "@/lib/branding";

interface HexFieldProps {
  id: string;
  value: string;
  disabled?: boolean;
  label: string;
  onCommit: (hex: string) => void;
}

/** Free-text hex entry; invalid input reverts to the current value on blur. */
function HexField({ id, value, disabled, label, onCommit }: HexFieldProps) {
  const [text, setText] = useState(value);
  const commit = () => {
    const next = text.trim().startsWith("#") ? text.trim() : `#${text.trim()}`;
    if (isHex(next)) onCommit(next.toUpperCase());
    else setText(value);
  };
  return (
    <Input
      id={id}
      value={text}
      disabled={disabled}
      aria-label={label}
      spellCheck={false}
      maxLength={7}
      onChange={(e) => setText(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === "Enter") commit();
      }}
      className="h-8 w-24 font-mono text-xs uppercase"
    />
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
            <Label htmlFor={id} className="text-sm">
              {label}
            </Label>
            <div className="flex items-center gap-2">
              <input
                id={id}
                type="color"
                value={value.toLowerCase()}
                disabled={disabled}
                onChange={(e) => onChange(group, e.target.value.toUpperCase())}
                className="h-8 w-10 cursor-pointer rounded-md border bg-transparent p-0.5 disabled:cursor-not-allowed disabled:opacity-50"
              />
              <HexField key={value} id={`${id}-hex`} value={value} disabled={disabled} label={label} onCommit={(hex) => onChange(group, hex)} />
            </div>
          </div>
        );
      })}
    </div>
  );
}
