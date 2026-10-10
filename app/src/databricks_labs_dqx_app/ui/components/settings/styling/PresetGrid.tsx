import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Check, Plus, Trash2 } from "lucide-react";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Badge } from "@/components/ui/badge";
import type { BrandingCustomPresetOut } from "@/lib/api";
import { PRESETS, customPresetNumber, deriveAllTokens, effectiveDark, type GroupColors } from "@/lib/branding";
import { cn } from "@/lib/utils";
import { ThemeMock } from "./ThemeMock";

/** Grid id of the "Add new" tile, selected while a new theme is being made. */
export const NEW_THEME_ID = "new";
const MAX_NAME = 40;

type Thumb = {
  id: string;
  light: Record<string, string>;
  dark: Record<string, string>;
  custom: boolean;
  name: string | null;
};

/** Built-in thumbnails are static, so derive them once. */
const BUILT_IN_THUMBS: Thumb[] = PRESETS.map((p) => ({
  id: p.id,
  light: deriveAllTokens("light", p.light),
  dark: deriveAllTokens("dark", p.dark),
  custom: false,
  name: null,
}));

/** Name shown in place; click to edit, Enter or blur saves, Escape cancels. */
function EditableName({
  name,
  placeholder,
  disabled,
  onRename,
}: {
  name: string;
  placeholder: string;
  disabled?: boolean;
  onRename: (name: string) => void;
}) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(false);
  const [text, setText] = useState(name);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!editing) return;
    inputRef.current?.focus();
    inputRef.current?.select();
  }, [editing]);

  const finish = (save: boolean) => {
    setEditing(false);
    const next = text.trim();
    if (save && next !== name) onRename(next);
    else setText(name);
  };

  if (!editing) {
    return (
      <button
        type="button"
        disabled={disabled}
        title={t("config.styling.customPresetRename")}
        onClick={() => {
          setText(name);
          setEditing(true);
        }}
        className="min-w-0 truncate rounded-md px-1.5 py-0.5 -mx-1.5 text-left text-sm font-medium transition hover:bg-muted disabled:pointer-events-none"
      >
        {name || placeholder}
      </button>
    );
  }
  return (
    <input
      ref={inputRef}
      value={text}
      maxLength={MAX_NAME}
      placeholder={placeholder}
      aria-label={t("config.styling.customPresetNameLabel")}
      size={Math.max(text.length, placeholder.length, 4)}
      onChange={(e) => setText(e.target.value)}
      onBlur={() => finish(true)}
      onKeyDown={(e) => {
        if (e.key === "Enter") finish(true);
        if (e.key === "Escape") finish(false);
      }}
      className="-mx-1.5 min-w-0 max-w-full rounded-md border bg-muted px-1.5 py-0.5 text-sm font-medium outline-none focus-visible:ring-2 focus-visible:ring-ring"
    />
  );
}

interface PresetGridProps {
  selected: string | null;
  custom: BrandingCustomPresetOut[];
  disabled?: boolean;
  onSelect: (id: string) => void;
  onAddNew: () => void;
  onRename: (id: string, name: string) => void;
  onDelete: (id: string) => void;
}

export function PresetGrid({ selected, custom, disabled, onSelect, onAddNew, onRename, onDelete }: PresetGridProps) {
  const { t } = useTranslation();
  const thumbs = useMemo<Thumb[]>(
    () => [
      ...BUILT_IN_THUMBS,
      ...custom.map((c) => {
        const light = c.light.colors as GroupColors;
        return {
          id: c.id,
          light: deriveAllTokens("light", light),
          dark: deriveAllTokens("dark", effectiveDark(light, !!c.dark.customised, c.dark.colors as GroupColors)),
          custom: true,
          name: c.name ?? null,
        };
      }),
    ],
    [custom],
  );
  const defaultName = (id: string) => {
    const n = customPresetNumber(id);
    return n === null ? t(`config.styling.presets.${id}`) : t("config.styling.customPresetName", { number: n });
  };
  const displayName = (p: Thumb) => p.name || defaultName(p.id);
  const addSelected = selected === NEW_THEME_ID;

  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {thumbs.map((p) => {
        const isSelected = selected === p.id;
        return (
          <div
            key={p.id}
            className={cn(
              "relative rounded-lg border p-2 transition hover:border-primary",
              isSelected && "ring-2 ring-primary",
              disabled && "opacity-60",
            )}
          >
            <button
              type="button"
              aria-pressed={isSelected}
              aria-label={displayName(p)}
              disabled={disabled}
              onClick={() => onSelect(p.id)}
              className="block w-full rounded-md text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            >
              <div className="grid grid-cols-2 gap-1.5">
                <ThemeMock tokens={p.light} size="thumb" />
                <ThemeMock tokens={p.dark} size="thumb" />
              </div>
            </button>
            {isSelected && (
              <span className="pointer-events-none absolute right-1.5 top-1.5 flex h-5 w-5 items-center justify-center rounded-full bg-primary text-primary-foreground">
                <Check className="h-3.5 w-3.5" />
              </span>
            )}
            <div className="mt-2 flex h-6 items-center gap-2 pr-7 text-sm font-medium">
              {p.custom ? (
                <EditableName
                  name={p.name ?? ""}
                  placeholder={defaultName(p.id)}
                  disabled={disabled}
                  onRename={(name) => onRename(p.id, name)}
                />
              ) : (
                <button
                  type="button"
                  disabled={disabled}
                  onClick={() => onSelect(p.id)}
                  tabIndex={-1}
                  className="truncate text-left disabled:pointer-events-none"
                >
                  {displayName(p)}
                </button>
              )}
              {p.id === "dqx-default" && <Badge variant="secondary">{t("config.styling.presetDefaultBadge")}</Badge>}
            </div>
            {p.custom && (
              <AlertDialog>
                <AlertDialogTrigger asChild>
                  <button
                    type="button"
                    disabled={disabled}
                    aria-label={t("config.styling.customPresetDelete", { name: displayName(p) })}
                    title={t("config.styling.customPresetDelete", { name: displayName(p) })}
                    className="absolute bottom-2 right-2 flex h-6 w-6 items-center justify-center rounded-md text-muted-foreground transition hover:bg-destructive/10 hover:text-destructive disabled:pointer-events-none"
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </button>
                </AlertDialogTrigger>
                <AlertDialogContent>
                  <AlertDialogHeader>
                    <AlertDialogTitle>{t("config.styling.customPresetDelete", { name: displayName(p) })}</AlertDialogTitle>
                    <AlertDialogDescription>{t("config.styling.customPresetDeleteConfirm")}</AlertDialogDescription>
                  </AlertDialogHeader>
                  <AlertDialogFooter>
                    <AlertDialogCancel>{t("config.styling.cancel")}</AlertDialogCancel>
                    <AlertDialogAction onClick={() => onDelete(p.id)}>{t("config.styling.delete")}</AlertDialogAction>
                  </AlertDialogFooter>
                </AlertDialogContent>
              </AlertDialog>
            )}
          </div>
        );
      })}
      <button
        type="button"
        aria-pressed={addSelected}
        disabled={disabled}
        onClick={onAddNew}
        className={cn(
          "flex min-h-28 flex-col items-center justify-center gap-1.5 rounded-lg border border-dashed p-2 text-sm font-medium text-muted-foreground transition hover:border-primary hover:text-foreground disabled:pointer-events-none disabled:opacity-60",
          addSelected && "border-solid text-foreground ring-2 ring-primary",
        )}
      >
        <Plus className="h-5 w-5" />
        {t("config.styling.addNewTheme")}
      </button>
    </div>
  );
}
