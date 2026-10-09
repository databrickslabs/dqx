import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { Building2, Loader2, RotateCcw, Upload } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { usePermissions } from "@/hooks/use-permissions";
import {
  useDeleteBrandingLogo,
  useGetBranding,
  useSaveBrandingCompanyName,
  useUploadBrandingLogo,
  type BrandingOut,
} from "@/lib/api";
import { LOGO_ACCEPT, fileToLogoPayload, type GroupColors, type Mode } from "@/lib/branding";
import { headerSwatches, headerTitle, logoUrl, pickHeaderLogo, type HeaderSwatch } from "@/lib/branding/header";
import selector from "@/lib/selector";
import { cn } from "@/lib/utils";
import { publishPreview, usePreviewState } from "./preview-store";
import { toastSaveError, useBrandingUpdate } from "./use-branding-update";

const MAX_COMPANY_NAME = 60;
const NAME_SAVE_DELAY_MS = 800;
const MODES: readonly Mode[] = ["light", "dark"];

function Row({ children }: { children: React.ReactNode }) {
  return <div className="flex flex-wrap items-center justify-between gap-4 rounded-md border p-3">{children}</div>;
}

function RowLabel({ htmlFor, label, hint }: { htmlFor?: string; label: string; hint?: React.ReactNode }) {
  return (
    <div className="min-w-0 space-y-0.5 pr-4">
      <Label htmlFor={htmlFor} className="text-sm">
        {label}
      </Label>
      {hint && <div className="text-[11px] text-muted-foreground">{hint}</div>}
    </div>
  );
}

/** Company name input that saves itself a moment after typing stops (and on blur). */
function CompanyNameRow({ serverName, disabled }: { serverName: string; disabled: boolean }) {
  const { t } = useTranslation();
  const saveMutation = useSaveBrandingCompanyName();
  const { applyResponse } = useBrandingUpdate();
  const [value, setValue] = useState(serverName);
  const savedRef = useRef(serverName);

  // Follow server changes (e.g. a full reset) unless the admin has unsaved typing.
  useEffect(() => {
    if (serverName === savedRef.current) return;
    const previous = savedRef.current;
    savedRef.current = serverName;
    setValue((v) => (v.trim() === previous.trim() ? serverName : v));
  }, [serverName]);

  useEffect(() => {
    publishPreview({ companyName: value });
    return () => publishPreview({ companyName: undefined });
  }, [value]);

  const save = (next: string) => {
    const trimmed = next.trim();
    if (trimmed === savedRef.current.trim()) return;
    saveMutation.mutate(
      { data: { company_name: trimmed || null } },
      {
        onSuccess: (response) => {
          savedRef.current = response.data.company_name ?? "";
          applyResponse(response);
        },
        onError: (err) => toastSaveError(t, err),
      },
    );
  };

  // The debounce re-arms on each keystroke; the ref always holds the latest save.
  const saveRef = useRef(save);
  saveRef.current = save;
  useEffect(() => {
    const id = window.setTimeout(() => saveRef.current(value), NAME_SAVE_DELAY_MS);
    return () => window.clearTimeout(id);
  }, [value]);

  const { product, company } = headerTitle(value);
  const title = company ? `${product} | ${company}` : product;

  return (
    <Row>
      <RowLabel
        htmlFor="branding-company-name"
        label={t("config.styling.companyNameLabel")}
        hint={t("config.styling.companyNamePreview", { title })}
      />
      <div className="flex items-center gap-2">
        {saveMutation.isPending && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
        <Input
          id="branding-company-name"
          value={value}
          maxLength={MAX_COMPANY_NAME}
          placeholder={t("config.styling.companyNamePlaceholder")}
          disabled={disabled}
          onChange={(e) => setValue(e.target.value)}
          onBlur={() => save(value)}
          className="h-8 w-64"
        />
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="h-8 w-8"
          aria-label={t("config.styling.companyNameReset")}
          title={t("config.styling.companyNameReset")}
          disabled={disabled || !value}
          onClick={() => {
            setValue("");
            save("");
          }}
        >
          <RotateCcw className="h-3.5 w-3.5" />
        </Button>
      </div>
    </Row>
  );
}

interface LogoTileProps {
  mode: Mode;
  src: string | null;
  swatch: HeaderSwatch;
  disabled: boolean;
  busy: boolean;
  onFile: (file: File) => void;
}

/** Header-coloured logo preview; click or drop an image onto it to upload. */
function LogoTile({ mode, src, swatch, disabled, busy, onFile }: LogoTileProps) {
  const { t } = useTranslation();
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);
  const [failedSrc, setFailedSrc] = useState<string | null>(null);
  const label = t(mode === "light" ? "config.styling.logoTileLight" : "config.styling.logoTileDark");
  const blocked = disabled || busy;

  const onDrop = (e: DragEvent<HTMLButtonElement>) => {
    e.preventDefault();
    setDragging(false);
    const file = e.dataTransfer.files?.[0];
    if (file && !blocked) onFile(file);
  };

  return (
    <div className="flex flex-col items-center gap-1">
      <input
        ref={inputRef}
        type="file"
        accept={LOGO_ACCEPT}
        className="hidden"
        onChange={(e) => {
          const file = e.target.files?.[0];
          e.target.value = "";
          if (file) onFile(file);
        }}
      />
      <button
        type="button"
        disabled={blocked}
        aria-label={t("config.styling.logoUploadTo", { mode: label })}
        title={t("config.styling.logoUploadHint")}
        onClick={() => inputRef.current?.click()}
        onDragOver={(e) => {
          e.preventDefault();
          if (!blocked) setDragging(true);
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={onDrop}
        className={cn(
          "group relative flex h-12 w-40 items-center justify-center overflow-hidden rounded-md border transition",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed",
          !blocked && "cursor-pointer hover:ring-2 hover:ring-primary/50",
          dragging && "ring-2 ring-primary",
        )}
        style={{ backgroundColor: swatch.background, color: swatch.foreground }}
      >
        {busy ? (
          <Loader2 className="h-4 w-4 animate-spin" />
        ) : src && src !== failedSrc ? (
          <img src={src} alt="" className="h-6 w-auto max-w-36 object-contain" onError={() => setFailedSrc(src)} />
        ) : (
          <img src="/dqx-logo.svg" alt="" className="h-6 w-6" />
        )}
        {!blocked && (
          <span
            aria-hidden="true"
            className={cn(
              "absolute inset-0 flex items-center justify-center bg-black/45 text-white opacity-0 transition group-hover:opacity-100",
              dragging && "opacity-100",
            )}
          >
            <Upload className="h-4 w-4" />
          </span>
        )}
      </button>
      <span className="text-[11px] text-muted-foreground">{label}</span>
    </div>
  );
}

function BrandingEditor({ data }: { data: BrandingOut }) {
  const { t } = useTranslation();
  const { isAdmin } = usePermissions();
  const preview = usePreviewState();
  const uploadMutation = useUploadBrandingLogo();
  const deleteMutation = useDeleteBrandingLogo();
  const { applyResponse } = useBrandingUpdate();
  const [error, setError] = useState<string | null>(null);
  const [resetting, setResetting] = useState(false);

  const logos = { light: data.logos.light ?? null, dark: data.logos.dark ?? null };
  const hasLogo = !!logos.light || !!logos.dark;
  const disabled = !isAdmin;
  const busy = uploadMutation.isPending || deleteMutation.isPending || resetting;

  // Live theme edits from the Styling card win over the saved theme.
  const swatches = useMemo(
    () =>
      headerSwatches(
        preview.theme ?? {
          light: data.light.colors as GroupColors,
          darkCustomised: !!data.dark.customised,
          dark: data.dark.colors as GroupColors,
        },
      ),
    [preview.theme, data],
  );

  const upload = async (mode: Mode, file: File) => {
    setError(null);
    let payload: { content_type: string; data_base64: string };
    try {
      payload = await fileToLogoPayload(file);
    } catch (err) {
      const code = err instanceof Error ? err.message : "";
      setError(t(code === "size" ? "config.styling.logoErrorSize" : "config.styling.logoErrorType"));
      return;
    }
    // The server shares the first logo across both modes; later uploads change only this mode.
    uploadMutation.mutate(
      { slot: mode, data: payload },
      {
        onSuccess: (response) => {
          applyResponse(response);
          toast.success(t("config.styling.logoSaved"));
        },
        onError: (err) => toastSaveError(t, err),
      },
    );
  };

  const resetLogos = async () => {
    setError(null);
    setResetting(true);
    try {
      for (const slot of MODES) {
        if (!logos[slot]) continue;
        applyResponse(await deleteMutation.mutateAsync({ slot }));
      }
      toast.success(t("config.styling.logoRemoved"));
    } catch (err) {
      toastSaveError(t, err);
    } finally {
      setResetting(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Building2 className="h-5 w-5" />
          {t("config.styling.brandingTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <CompanyNameRow serverName={data.company_name ?? ""} disabled={disabled} />
        <Row>
          <RowLabel
            label={t("config.styling.logoLabel")}
            hint={
              <>
                <p>{t("config.styling.logoRequirements")}</p>
                {error && <p className="text-destructive">{error}</p>}
              </>
            }
          />
          <div className="flex items-start gap-3">
            {MODES.map((mode) => {
              const logo = pickHeaderLogo({ logoMode: data.logo_mode ?? "shared", logos }, mode === "dark");
              return (
                <LogoTile
                  key={mode}
                  mode={mode}
                  src={logo ? logoUrl(logo.slot, logo.hash) : null}
                  swatch={swatches[mode]}
                  disabled={disabled}
                  busy={busy}
                  onFile={(file) => void upload(mode, file)}
                />
              );
            })}
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="mt-2 h-8 w-8"
              aria-label={t("config.styling.logoReset")}
              title={t("config.styling.logoReset")}
              disabled={disabled || busy || !hasLogo}
              onClick={() => void resetLogos()}
            >
              <RotateCcw className="h-3.5 w-3.5" />
            </Button>
          </div>
        </Row>
      </CardContent>
    </Card>
  );
}

export function BrandingCard() {
  const { data } = useGetBranding(selector<BrandingOut>());
  if (!data) return <Skeleton className="h-48 w-full" />;
  return <BrandingEditor data={data} />;
}
