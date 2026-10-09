import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Check, Trash2 } from "lucide-react";
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

type Thumb = { id: string; light: Record<string, string>; dark: Record<string, string>; custom: boolean };

/** Built-in thumbnails are static, so derive them once. */
const BUILT_IN_THUMBS: Thumb[] = PRESETS.map((p) => ({
  id: p.id,
  light: deriveAllTokens("light", p.light),
  dark: deriveAllTokens("dark", p.dark),
  custom: false,
}));

interface PresetGridProps {
  selected: string | null;
  custom: BrandingCustomPresetOut[];
  disabled?: boolean;
  onSelect: (id: string) => void;
  onDelete: (id: string) => void;
}

export function PresetGrid({ selected, custom, disabled, onSelect, onDelete }: PresetGridProps) {
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
        };
      }),
    ],
    [custom],
  );
  const name = (id: string) => {
    const n = customPresetNumber(id);
    return n === null ? t(`config.styling.presets.${id}`) : t("config.styling.customPresetName", { number: n });
  };

  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {thumbs.map((p) => {
        const isSelected = selected === p.id;
        return (
          <div key={p.id} className="relative">
            <button
              type="button"
              aria-pressed={isSelected}
              disabled={disabled}
              onClick={() => onSelect(p.id)}
              className={cn(
                "relative w-full rounded-lg border p-2 text-left transition hover:border-primary disabled:pointer-events-none disabled:opacity-60",
                isSelected && "ring-2 ring-primary",
              )}
            >
              {isSelected && (
                <span className="absolute right-1.5 top-1.5 flex h-5 w-5 items-center justify-center rounded-full bg-primary text-primary-foreground">
                  <Check className="h-3.5 w-3.5" />
                </span>
              )}
              <div className="grid grid-cols-2 gap-1.5">
                <ThemeMock tokens={p.light} size="thumb" />
                <ThemeMock tokens={p.dark} size="thumb" />
              </div>
              <div className="mt-2 flex h-6 items-center gap-2 pr-8 text-sm font-medium">
                {name(p.id)}
                {p.id === "dqx-default" && <Badge variant="secondary">{t("config.styling.presetDefaultBadge")}</Badge>}
              </div>
            </button>
            {p.custom && (
              <AlertDialog>
                <AlertDialogTrigger asChild>
                  <button
                    type="button"
                    disabled={disabled}
                    aria-label={t("config.styling.customPresetDelete", { name: name(p.id) })}
                    title={t("config.styling.customPresetDelete", { name: name(p.id) })}
                    className="absolute bottom-2 right-2 flex h-6 w-6 items-center justify-center rounded-md text-muted-foreground transition hover:bg-destructive/10 hover:text-destructive disabled:pointer-events-none disabled:opacity-60"
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </button>
                </AlertDialogTrigger>
                <AlertDialogContent>
                  <AlertDialogHeader>
                    <AlertDialogTitle>{t("config.styling.customPresetDelete", { name: name(p.id) })}</AlertDialogTitle>
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
    </div>
  );
}
