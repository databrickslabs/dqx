import { useTranslation } from "react-i18next";
import { Check } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { PRESETS, deriveAllTokens } from "@/lib/branding";
import { cn } from "@/lib/utils";
import { ThemeMock } from "./ThemeMock";

/** Thumbnails are static per preset, so derive them once. */
const PRESET_THUMBS = PRESETS.map((p) => ({
  id: p.id,
  light: deriveAllTokens("light", p.light),
  dark: deriveAllTokens("dark", p.dark),
}));

interface PresetGridProps {
  selected: string | null;
  disabled?: boolean;
  onSelect: (id: string) => void;
}

export function PresetGrid({ selected, disabled, onSelect }: PresetGridProps) {
  const { t } = useTranslation();
  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {PRESET_THUMBS.map((p) => {
        const isSelected = selected === p.id;
        return (
          <button
            key={p.id}
            type="button"
            aria-pressed={isSelected}
            disabled={disabled}
            onClick={() => onSelect(p.id)}
            className={cn(
              "relative rounded-lg border p-2 text-left transition hover:border-primary disabled:pointer-events-none disabled:opacity-60",
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
            <div className="mt-2 flex items-center gap-2 text-sm font-medium">
              {t(`config.styling.presets.${p.id}`)}
              {p.id === "dqx-default" && <Badge variant="secondary">{t("config.styling.presetDefaultBadge")}</Badge>}
            </div>
          </button>
        );
      })}
    </div>
  );
}
