/**
 * Ported from dqlake's `components/products/ProductSharingTab.tsx`. dqlake
 * backs its Steward field with a principal-search `StewardPicker`; DQX's
 * data model stores `steward` as a plain string (mirroring how the Rules
 * Registry's Sharing tab edits `steward` — see
 * `RegistryRuleFormDialog.tsx`'s `sharingTabContent`), so this reuses that
 * same plain-Input control rather than introducing a new picker component.
 */
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { HelpTooltip } from "@/components/HelpTooltip";
import type { EditProductState } from "@/components/data-products/useEditProductState";

interface Props {
  editState: EditProductState;
  canEdit: boolean;
}

export function ProductSharingTab({ editState, canEdit }: Props) {
  const { t } = useTranslation();
  const { steward, setSteward } = editState;

  if (!canEdit) {
    return (
      <div className="space-y-6 max-w-2xl">
        <section className="flex flex-col gap-3">
          <p className="text-sm font-medium leading-none">{t("dataProducts.colSteward")}</p>
          {steward ? (
            <p className="text-sm">{steward}</p>
          ) : (
            <p className="text-sm text-muted-foreground italic">{t("dataProducts.aboutStewardNone")}</p>
          )}
        </section>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-2xl">
      <section className="flex flex-col gap-3">
        <div className="flex items-center gap-1.5">
          <Label htmlFor="product-steward">{t("dataProducts.colSteward")}</Label>
          <HelpTooltip text={t("dataProducts.stewardHelp")} />
        </div>
        <Input
          id="product-steward"
          className="max-w-xs"
          value={steward}
          onChange={(e) => setSteward(e.target.value)}
          placeholder={t("dataProducts.stewardPlaceholder")}
        />
      </section>
    </div>
  );
}
