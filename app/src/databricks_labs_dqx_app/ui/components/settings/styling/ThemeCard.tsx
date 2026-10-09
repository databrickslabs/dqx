import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { Paintbrush } from "lucide-react";
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
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useIsDarkMode } from "@/hooks/use-is-dark-mode";
import { usePermissions } from "@/hooks/use-permissions";
import {
  useDeleteBrandingCustomPreset,
  useGetBranding,
  useResetBranding,
  useSaveBrandingTheme,
  type BrandingOut,
} from "@/lib/api";
import {
  DEFAULT_GROUPS,
  applyCustomPreset,
  applyPreset,
  customPresetNumber,
  deriveAllTokens,
  draftDark,
  draftFromApi,
  draftToTheme,
  draftWarnings,
  isDirty,
  setGroup,
  type Mode,
  type ThemeDraft,
} from "@/lib/branding";
import { logoUrl, pickHeaderLogo } from "@/lib/branding/header";
import selector from "@/lib/selector";
import { cn } from "@/lib/utils";
import { ColorGroupPicker } from "./ColorGroupPicker";
import { ContrastWarnings } from "./ContrastWarnings";
import { PresetGrid } from "./PresetGrid";
import { publishPreview, usePreviewState } from "./preview-store";
import { ThemeMock } from "./ThemeMock";
import { toastSaveError, useBrandingUpdate } from "./use-branding-update";

/** A draft with no colours at all is DQX Default, whether or not the preset was recorded. */
function selectedPreset(d: ThemeDraft): string | null {
  if (d.preset) return d.preset;
  return Object.keys(d.light).length === 0 && Object.keys(d.dark).length === 0 ? "dqx-default" : null;
}

function SectionLabel({ children }: { children: string }) {
  return <p className="text-sm font-medium">{children}</p>;
}

const MODES: readonly Mode[] = ["light", "dark"];

function ThemeEditor({ server }: { server: BrandingOut }) {
  const { t } = useTranslation();
  const { isAdmin } = usePermissions();
  const appIsDark = useIsDarkMode();
  const preview = usePreviewState();
  const saveMutation = useSaveBrandingTheme();
  const resetMutation = useResetBranding();
  const deletePresetMutation = useDeleteBrandingCustomPreset();
  const { applyResponse, applyReset } = useBrandingUpdate();

  const saved = useMemo(() => draftFromApi(server), [server]);
  const [draft, setDraft] = useState<ThemeDraft>(saved);
  // One mode switch drives the colour pickers; the preview always shows both modes.
  const [mode, setMode] = useState<Mode>(appIsDark ? "dark" : "light");

  const colors = useMemo(() => ({ light: draft.light, dark: draftDark(draft) }), [draft]);
  const tokens = useMemo(
    () => ({ light: deriveAllTokens("light", colors.light), dark: deriveAllTokens("dark", colors.dark) }),
    [colors],
  );
  const warnings = useMemo(() => draftWarnings(draft), [draft]);
  const dirty = isDirty(draft, saved);
  const busy = !isAdmin || saveMutation.isPending || resetMutation.isPending || deletePresetMutation.isPending;
  const customPresets = server.custom_presets ?? [];

  // Share the unsaved theme with the Branding card's logo previews.
  useEffect(() => {
    const theme = draftToTheme(draft);
    publishPreview({ theme: { light: theme.light.colors, darkCustomised: theme.dark.customised, dark: theme.dark.colors } });
  }, [draft]);
  useEffect(() => () => publishPreview({ theme: undefined }), []);

  const companyName = preview.companyName ?? server.company_name;
  const logoState = { logoMode: server.logo_mode ?? "shared", logos: { light: server.logos.light ?? null, dark: server.logos.dark ?? null } };

  const save = () => {
    saveMutation.mutate(
      { data: draftToTheme(draft) },
      {
        onSuccess: (response) => {
          applyResponse(response);
          setDraft(draftFromApi(response.data));
          const number = draft.preset === null && response.data.preset ? customPresetNumber(response.data.preset) : null;
          toast.success(
            number === null ? t("config.styling.themeSaved") : t("config.styling.themeSavedAsCustom", { number }),
          );
        },
        onError: (err) => toastSaveError(t, err),
      },
    );
  };

  const selectPreset = (id: string) => {
    const custom = customPresets.find((c) => c.id === id);
    setDraft(custom ? applyCustomPreset(custom) : applyPreset(id));
  };

  const deletePreset = (id: string) => {
    deletePresetMutation.mutate(
      { presetId: id },
      {
        onSuccess: (response) => {
          applyResponse(response);
          // Keep unsaved edits; only forget the deleted preset.
          setDraft((d) => (d.preset === id ? { ...d, preset: null } : d));
          toast.success(t("config.styling.customPresetDeleted"));
        },
        onError: (err) => toastSaveError(t, err),
      },
    );
  };

  const reset = () => {
    resetMutation.mutate(undefined, {
      onSuccess: (response) => {
        applyReset(response);
        setDraft(draftFromApi(response.data));
        toast.success(t("config.styling.themeReset"));
      },
      onError: (err) => toastSaveError(t, err),
    });
  };

  const modeLabel = (m: Mode) => t(m === "light" ? "config.styling.modeLight" : "config.styling.modeDark");

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Paintbrush className="h-5 w-5" />
          {t("config.styling.themeTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">{t("config.styling.themeDescription")}</p>

        <div className="space-y-2">
          <SectionLabel>{t("config.styling.presetsLabel")}</SectionLabel>
          <PresetGrid
            selected={selectedPreset(draft)}
            custom={customPresets}
            disabled={busy}
            onSelect={selectPreset}
            onDelete={deletePreset}
          />
        </div>

        <div className="grid gap-6 lg:grid-cols-2">
          <div className="space-y-2">
            <SectionLabel>{t("config.styling.colorsLabelMode", { mode: modeLabel(mode) })}</SectionLabel>
            <ColorGroupPicker
              key={mode}
              idPrefix={`branding-${mode}`}
              colors={colors[mode]}
              defaults={DEFAULT_GROUPS[mode]}
              disabled={busy}
              onChange={(group, hex) => setDraft((d) => setGroup(d, mode, group, hex))}
            />
          </div>

          <div className="space-y-2">
            <SectionLabel>{t("config.styling.previewLabel")}</SectionLabel>
            <div className="space-y-2">
              {MODES.map((m) => {
                const logo = pickHeaderLogo(logoState, m === "dark");
                return (
                  <button
                    key={m}
                    type="button"
                    aria-pressed={mode === m}
                    aria-label={t("config.styling.editMode", { mode: modeLabel(m) })}
                    onClick={() => setMode(m)}
                    className={cn(
                      "block w-full rounded-md text-left transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                      mode === m ? "ring-2 ring-primary ring-offset-2 ring-offset-background" : "opacity-80 hover:opacity-100",
                    )}
                  >
                    <ThemeMock
                      tokens={tokens[m]}
                      companyName={companyName}
                      logoSrc={logo ? logoUrl(logo.slot, logo.hash) : null}
                      size="preview"
                    />
                  </button>
                );
              })}
            </div>
            <div className="flex gap-1 pt-1" role="group" aria-label={t("config.styling.modeToggleLabel")}>
              {MODES.map((m) => (
                <Button
                  key={m}
                  type="button"
                  size="sm"
                  variant={mode === m ? "secondary" : "ghost"}
                  aria-pressed={mode === m}
                  className="h-7 px-3 text-xs"
                  onClick={() => setMode(m)}
                >
                  {modeLabel(m)}
                </Button>
              ))}
            </div>
          </div>
        </div>

        <ContrastWarnings warnings={warnings} />

        <div className="flex flex-wrap items-center gap-2">
          <Button size="sm" onClick={save} disabled={busy || !dirty}>
            {t("config.styling.save")}
          </Button>
          <Button size="sm" variant="outline" onClick={() => setDraft(saved)} disabled={!dirty || saveMutation.isPending}>
            {t("config.styling.cancel")}
          </Button>
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button size="sm" variant="ghost" className="ml-auto" disabled={busy}>
                {t("config.styling.resetDefault")}
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>{t("config.styling.resetDefault")}</AlertDialogTitle>
                <AlertDialogDescription>{t("config.styling.resetConfirm")}</AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>{t("config.styling.cancel")}</AlertDialogCancel>
                <AlertDialogAction onClick={reset}>{t("config.styling.resetDefault")}</AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </div>
      </CardContent>
    </Card>
  );
}

export function ThemeCard() {
  const { data } = useGetBranding(selector<BrandingOut>());
  if (!data) return <Skeleton className="h-96 w-full" />;
  return <ThemeEditor server={data} />;
}
