import { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { Building2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { usePermissions } from "@/hooks/use-permissions";
import { useGetBranding, useSaveBrandingCompanyName, type BrandingOut } from "@/lib/api";
import { headerTitle } from "@/lib/branding/header";
import selector from "@/lib/selector";
import { toastSaveError, useBrandingUpdate } from "./use-branding-update";

const MAX_COMPANY_NAME = 60;

function CompanyNameForm({ initial }: { initial: string }) {
  const { t } = useTranslation();
  const { isAdmin } = usePermissions();
  const saveMutation = useSaveBrandingCompanyName();
  const { applyResponse } = useBrandingUpdate();
  const [value, setValue] = useState(initial);
  const [saved, setSaved] = useState(initial);

  const { product, company } = headerTitle(value);
  const title = company ? `${product} | ${company}` : product;
  const trimmed = value.trim();

  const save = () => {
    saveMutation.mutate(
      { data: { company_name: trimmed || null } },
      {
        onSuccess: (response) => {
          applyResponse(response);
          const next = response.data.company_name ?? "";
          setValue(next);
          setSaved(next);
          toast.success(t(next ? "config.styling.companyNameSaved" : "config.styling.companyNameCleared"));
        },
        onError: (err) => toastSaveError(t, err),
      },
    );
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Building2 className="h-5 w-5" />
          {t("config.styling.companyNameTitle")}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground leading-relaxed">
          {t("config.styling.companyNameDescription")}
        </p>
        <div className="space-y-2">
          <Label htmlFor="branding-company-name" className="text-sm">
            {t("config.styling.companyNameLabel")}
          </Label>
          <Input
            id="branding-company-name"
            value={value}
            maxLength={MAX_COMPANY_NAME}
            placeholder={t("config.styling.companyNamePlaceholder")}
            disabled={!isAdmin || saveMutation.isPending}
            onChange={(e) => setValue(e.target.value)}
            className="max-w-sm"
          />
          <p className="text-xs text-muted-foreground">{t("config.styling.companyNamePreview", { title })}</p>
        </div>
        <Button
          size="sm"
          onClick={save}
          disabled={!isAdmin || saveMutation.isPending || trimmed === saved.trim()}
        >
          {t("config.styling.save")}
        </Button>
      </CardContent>
    </Card>
  );
}

export function CompanyNameCard() {
  const { data } = useGetBranding(selector<BrandingOut>());
  if (!data) return <Skeleton className="h-40 w-full" />;
  return <CompanyNameForm initial={data.company_name ?? ""} />;
}
