import { useMemo, useState } from "react";
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
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useIsDarkMode } from "@/hooks/use-is-dark-mode";
import { usePermissions } from "@/hooks/use-permissions";
import { useGetBranding, useResetBranding, useSaveBrandingTheme, type BrandingOut } from "@/lib/api";
import {
  DEFAULT_GROUPS,
  applyPreset,
  deriveAllTokens,
  draftFromApi,
  draftWarnings,
  effectiveDark,
  isDirty,
  setGroup,
  type ColorGroup,
  type Mode,
  type ThemeDraft,
} from "@/lib/branding";
import { headerTitle, logoUrl, pickHeaderLogo } from "@/lib/branding/header";
import selector from "@/lib/selector";
import { ColorGroupPicker } from "./ColorGroupPicker";
import { ContrastWarnings } from "./ContrastWarnings";
import { PresetGrid } from "./PresetGrid";
import { ThemeMock } from "./ThemeMock";
import { toastSaveError, useBrandingUpdate } from "./use-branding-update";

/** A draft with no colours at all is DQX Default, whether or not the preset was recorded. */
function selectedPreset(d: ThemeDraft): string | null {
  if (d.preset) return d.preset;
  return Object.keys(d.light).length === 0 && !d.darkCustomised ? "dqx-default" : null;
}

function SectionLabel({ children }: { children: string }) {
  return <p className="text-sm font-medium">{children}</p>;
}

function BrandingEditor({ server }: { server: BrandingOut }) {
  const { t } = useTranslation();
  const { isAdmin } = usePermissions();
  const appIsDark = useIsDarkMode();
  const saveMutation = useSaveBrandingTheme();
  const resetMutation = useResetBranding();
  const { applyResponse, applyReset } = useBrandingUpdate();

  const saved = useMemo(() => draftFromApi(server), [server]);
  const [draft, setDraft] = useState<ThemeDraft>(saved);
  const [previewMode, setPreviewMode] = useState<Mode>(appIsDark ? "dark" : "light");

  const darkColors = useMemo(() => effectiveDark(draft.light, draft.darkCustomised, draft.dark), [draft]);
  const warnings = useMemo(() => draftWarnings(draft), [draft]);
  const previewTokens = useMemo(
    () => (previewMode === "light" ? deriveAllTokens("light", draft.light) : deriveAllTokens("dark", darkColors)),
    [previewMode, draft.light, darkColors],
  );
  const dirty = isDirty(draft, saved);
  const busy = !isAdmin || saveMutation.isPending || resetMutation.isPending;

  const logo = pickHeaderLogo(
    { logoMode: server.logo_mode ?? "shared", logos: { light: server.logos.light ?? null, dark: server.logos.dark ?? null } },
    previewMode === "dark",
  );

  const onColor = (mode: Mode) => (group: ColorGroup, hex: string) => setDraft((d) => setGroup(d, mode, group, hex));

  const onMatchLight = (auto: boolean) =>
    setDraft((d) =>
      auto
        ? { ...d, preset: null, darkCustomised: false, dark: {} }
        : { ...d, darkCustomised: true, dark: effectiveDark(d.light, false, {}) },
    );

  const save = () => {
    saveMutation.mutate(
      {
        data: {
          preset: draft.preset,
          light: { colors: draft.light },
          dark: { customised: draft.darkCustomised, colors: draft.darkCustomised ? draft.dark : {} },
        },
      },
      {
        onSuccess: (response) => {
          applyResponse(response);
          setDraft(draftFromApi(response.data));
          toast.success(t("config.styling.themeSaved"));
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

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Paintbrush className="h-5 w-5" />
          {t("config.styling.brandingTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">{t("config.styling.brandingDescription")}</p>

        <div className="space-y-2">
          <SectionLabel>{t("config.styling.presetsLabel")}</SectionLabel>
          <PresetGrid selected={selectedPreset(draft)} disabled={busy} onSelect={(id) => setDraft(applyPreset(id))} />
        </div>

        <div className="grid gap-6 lg:grid-cols-2">
          <div className="space-y-2">
            <SectionLabel>{t("config.styling.colorsLabel")}</SectionLabel>
            <Tabs defaultValue="light">
              <TabsList>
                <TabsTrigger value="light">{t("config.styling.modeLight")}</TabsTrigger>
                <TabsTrigger value="dark">{t("config.styling.modeDark")}</TabsTrigger>
              </TabsList>
              <TabsContent value="light" className="mt-2">
                <ColorGroupPicker
                  idPrefix="branding-light"
                  colors={draft.light}
                  defaults={DEFAULT_GROUPS.light}
                  disabled={busy}
                  onChange={onColor("light")}
                />
              </TabsContent>
              <TabsContent value="dark" className="mt-2 space-y-3">
                <div className="flex items-center justify-between gap-4 rounded-md border p-3">
                  <div className="space-y-0.5 pr-4">
                    <Label htmlFor="branding-match-light" className="text-sm">
                      {t("config.styling.matchLight")}
                    </Label>
                    <p className="text-[11px] text-muted-foreground">{t("config.styling.matchLightHelp")}</p>
                  </div>
                  <Switch
                    id="branding-match-light"
                    checked={!draft.darkCustomised}
                    disabled={busy}
                    onCheckedChange={onMatchLight}
                  />
                </div>
                <ColorGroupPicker
                  idPrefix="branding-dark"
                  colors={darkColors}
                  defaults={DEFAULT_GROUPS.dark}
                  disabled={busy || !draft.darkCustomised}
                  onChange={onColor("dark")}
                />
              </TabsContent>
            </Tabs>
          </div>

          <div className="space-y-2">
            <div className="flex items-center justify-between gap-2">
              <SectionLabel>{t("config.styling.previewLabel")}</SectionLabel>
              <div className="flex gap-1" role="group" aria-label={t("config.styling.previewLabel")}>
                {(["light", "dark"] as const).map((m) => (
                  <Button
                    key={m}
                    type="button"
                    size="sm"
                    variant={previewMode === m ? "secondary" : "ghost"}
                    aria-pressed={previewMode === m}
                    className="h-7 px-2 text-xs"
                    onClick={() => setPreviewMode(m)}
                  >
                    {t(m === "light" ? "config.styling.modeLight" : "config.styling.modeDark")}
                  </Button>
                ))}
              </div>
            </div>
            <ThemeMock
              tokens={previewTokens}
              companyName={headerTitle(server.company_name).company}
              logoSrc={logo ? logoUrl(logo.slot, logo.hash) : null}
              size="preview"
            />
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

export function BrandingCard() {
  const { data } = useGetBranding(selector<BrandingOut>());
  if (!data) return <Skeleton className="h-96 w-full" />;
  return <BrandingEditor server={data} />;
}
