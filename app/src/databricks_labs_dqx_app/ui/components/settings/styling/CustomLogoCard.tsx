import { useRef, useState, type ChangeEvent } from "react";
import { useIsMutating } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { Image as ImageIcon, Trash2, Upload } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { usePermissions } from "@/hooks/use-permissions";
import {
  useDeleteBrandingLogo,
  useGetBranding,
  useSaveBrandingLogoMode,
  useUploadBrandingLogo,
  type BrandingOut,
} from "@/lib/api";
import { LOGO_ACCEPT, fileToLogoPayload } from "@/lib/branding";
import { logoUrl, type HeaderLogoSlot } from "@/lib/branding/header";
import selector from "@/lib/selector";
import { toastSaveError, useBrandingUpdate } from "./use-branding-update";

type LogoMode = "shared" | "separate";

interface LogoSlotProps {
  slot: HeaderLogoSlot;
  label: string;
  hash: string | null;
  disabled: boolean;
}

function LogoSlot({ slot, label, hash, disabled }: LogoSlotProps) {
  const { t } = useTranslation();
  const inputRef = useRef<HTMLInputElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [failedSrc, setFailedSrc] = useState<string | null>(null);
  const uploadMutation = useUploadBrandingLogo();
  const deleteMutation = useDeleteBrandingLogo();
  const { applyResponse } = useBrandingUpdate();
  const busy = disabled || uploadMutation.isPending || deleteMutation.isPending;
  const src = hash ? logoUrl(slot, hash) : null;

  const onFile = async (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;
    setError(null);
    let payload: { content_type: string; data_base64: string };
    try {
      payload = await fileToLogoPayload(file);
    } catch (err) {
      const code = err instanceof Error ? err.message : "";
      setError(t(code === "size" ? "config.styling.logoErrorSize" : "config.styling.logoErrorType"));
      return;
    }
    uploadMutation.mutate(
      { slot, data: payload },
      {
        onSuccess: (response) => {
          applyResponse(response);
          toast.success(t("config.styling.logoSaved"));
        },
        onError: (err2) => toastSaveError(t, err2),
      },
    );
  };

  const remove = () => {
    setError(null);
    deleteMutation.mutate(
      { slot },
      {
        onSuccess: (response) => {
          applyResponse(response);
          toast.success(t("config.styling.logoRemoved"));
        },
        onError: (err) => toastSaveError(t, err),
      },
    );
  };

  return (
    <div className="space-y-2">
      <p className="text-sm font-medium">{label}</p>
      <div className="flex h-16 items-center justify-center rounded-md border bg-muted">
        {src && src !== failedSrc ? (
          <img src={src} alt={label} className="h-6 w-auto max-w-40 object-contain" onError={() => setFailedSrc(src)} />
        ) : (
          <img src="/dqx-logo.svg" alt="" className="h-6 w-6 opacity-40" />
        )}
      </div>
      <div className="flex items-center gap-2">
        <input ref={inputRef} type="file" accept={LOGO_ACCEPT} className="hidden" onChange={onFile} />
        <Button type="button" variant="outline" size="sm" disabled={busy} onClick={() => inputRef.current?.click()}>
          <Upload className="h-3.5 w-3.5" />
          {t(hash ? "config.styling.logoReplace" : "config.styling.logoUpload")}
        </Button>
        {hash && (
          <Button type="button" variant="ghost" size="sm" disabled={busy} onClick={remove}>
            <Trash2 className="h-3.5 w-3.5" />
            {t("config.styling.logoRemove")}
          </Button>
        )}
      </div>
      {error && <p className="text-sm text-destructive">{error}</p>}
    </div>
  );
}

export function CustomLogoCard() {
  const { t } = useTranslation();
  const { isAdmin } = usePermissions();
  const { data } = useGetBranding(selector<BrandingOut>());
  const modeMutation = useSaveBrandingLogoMode();
  const { applyResponse } = useBrandingUpdate();
  // Logo uploads/removals run in the slots; block mode changes until they settle.
  const logoBusy =
    useIsMutating({ mutationKey: ["uploadBrandingLogo"] }) + useIsMutating({ mutationKey: ["deleteBrandingLogo"] }) > 0;

  if (!data) return <Skeleton className="h-40 w-full" />;

  const mode: LogoMode = data.logo_mode === "separate" ? "separate" : "shared";
  const disabled = !isAdmin;

  const saveMode = (next: LogoMode) => {
    if (next === mode) return;
    modeMutation.mutate(
      { data: { logo_mode: next } },
      {
        onSuccess: (response) => {
          applyResponse(response);
          toast.success(t("config.styling.logoModeSaved"));
        },
        onError: (err) => toastSaveError(t, err),
      },
    );
  };

  const slots: { slot: HeaderLogoSlot; label: string }[] =
    mode === "separate"
      ? [
          { slot: "light", label: t("config.styling.logoSlotLight") },
          { slot: "dark", label: t("config.styling.logoSlotDark") },
        ]
      : [{ slot: "light", label: t("config.styling.logoSlotShared") }];

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ImageIcon className="h-5 w-5" />
          {t("config.styling.customLogoTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">{t("config.styling.customLogoDescription")}</p>
        <div className="flex flex-wrap gap-2" role="group" aria-label={t("config.styling.customLogoTitle")}>
          {(["shared", "separate"] as const).map((m) => (
            <Button
              key={m}
              type="button"
              size="sm"
              variant={mode === m ? "default" : "outline"}
              aria-pressed={mode === m}
              disabled={disabled || modeMutation.isPending || logoBusy}
              onClick={() => saveMode(m)}
            >
              {t(m === "shared" ? "config.styling.logoModeShared" : "config.styling.logoModeSeparate")}
            </Button>
          ))}
        </div>
        <div className="grid gap-4 sm:grid-cols-2">
          {slots.map((s) => (
            <LogoSlot key={s.slot} slot={s.slot} label={s.label} hash={data.logos[s.slot] ?? null} disabled={disabled} />
          ))}
        </div>
        <p className="text-sm text-muted-foreground">{t("config.styling.logoRequirements")}</p>
      </CardContent>
    </Card>
  );
}
