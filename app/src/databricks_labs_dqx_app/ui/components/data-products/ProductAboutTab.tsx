/** Ported from dqlake's `components/products/ProductAboutTab.tsx`. */
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import type { EditProductState } from "@/components/data-products/useEditProductState";

interface Props {
  editState: EditProductState;
  canEdit: boolean;
}

export function ProductAboutTab({ editState, canEdit }: Props) {
  const { t } = useTranslation();
  const { name, description, setName, setDescription } = editState;

  if (!canEdit) {
    return (
      <div className="space-y-6 max-w-2xl">
        <section className="flex flex-col gap-3">
          <p className="text-sm font-medium leading-none">{t("dataProducts.nameLabel")}</p>
          <p className="text-sm">{name}</p>
        </section>
        <section className="flex flex-col gap-3">
          <p className="text-sm font-medium leading-none">{t("dataProducts.descriptionLabel")}</p>
          {description ? (
            <p className="text-sm whitespace-pre-wrap">{description}</p>
          ) : (
            <p className="text-sm text-muted-foreground italic">{t("dataProducts.aboutNoDescription")}</p>
          )}
        </section>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-2xl">
      <section className="flex flex-col gap-3">
        <Label htmlFor="product-name">{t("dataProducts.nameLabel")}</Label>
        <Input id="product-name" value={name} onChange={(e) => setName(e.target.value)} />
      </section>
      <section className="flex flex-col gap-3">
        <Label htmlFor="product-description">{t("dataProducts.descriptionLabel")}</Label>
        <Textarea
          id="product-description"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          rows={4}
          placeholder={t("dataProducts.descriptionPlaceholder")}
        />
      </section>
    </div>
  );
}
