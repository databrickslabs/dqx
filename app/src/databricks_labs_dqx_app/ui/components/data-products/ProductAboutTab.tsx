/** Ported from dqlake's `components/products/ProductAboutTab.tsx`. */
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { PrincipalPicker, type PickedPrincipal } from "@/components/permissions/PrincipalPicker";
import type { EditProductState } from "@/components/data-products/useEditProductState";
import { formatDateShort } from "@/lib/format-utils";
import type { DataProductOut } from "@/lib/api";

interface Props {
  product: DataProductOut;
  editState: EditProductState;
  canEdit: boolean;
}

function DataProductStatusBadge({ status }: { status: string }) {
  const { t } = useTranslation();
  switch (status) {
    case "approved":
      return (
        <Badge variant="default" className="text-[10px]">
          {t("dataProducts.statusApproved")}
        </Badge>
      );
    case "pending_approval":
      return (
        <Badge variant="outline" className="text-[10px] border-amber-500 text-amber-600">
          {t("dataProducts.statusPendingApproval")}
        </Badge>
      );
    case "rejected":
      return (
        <Badge variant="outline" className="text-[10px] border-red-500 text-red-600">
          {t("dataProducts.statusRejected")}
        </Badge>
      );
    case "modified":
      return (
        <Badge variant="outline" className="text-[10px] border-amber-500 text-amber-600">
          {t("dataProducts.statusModified")}
        </Badge>
      );
    default:
      return (
        <Badge variant="secondary" className="text-[10px]">
          {t("dataProducts.statusDraft")}
        </Badge>
      );
  }
}

export function ProductAboutTab({ product, editState, canEdit }: Props) {
  const { t } = useTranslation();
  const { name, description, setName, setDescription, steward, setSteward } = editState;

  const effectiveOwner = steward || product.created_by || "";
  const ownerValue: PickedPrincipal | null = effectiveOwner
    ? { principal_id: "", principal_type: "user", principal_name: effectiveOwner }
    : null;

  const metadataSection = (
    <section className="space-y-3">
      <h2 className="text-sm font-semibold">{t("dataProducts.aboutMetadataTitle")}</h2>
      <dl className="grid grid-cols-[160px_1fr] gap-x-4 gap-y-2 text-xs">
        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutStatus")}</dt>
        <dd>
          <DataProductStatusBadge status={product.display_status} />
        </dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutOwner")}</dt>
        <dd>
          {canEdit ? (
            <PrincipalPicker
              value={ownerValue}
              className="w-full max-w-xs h-8 text-xs"
              suggestion={
                product.created_by && product.created_by !== effectiveOwner
                  ? {
                      displayName: product.created_by,
                      onPick: () => setSteward(product.created_by!),
                    }
                  : null
              }
              onSelect={(p) => setSteward(p.display_name)}
              onClear={() => {
                if (product.created_by) setSteward(product.created_by);
              }}
            />
          ) : effectiveOwner ? (
            effectiveOwner
          ) : (
            <span className="text-muted-foreground italic">{t("dataProducts.aboutOwnerNone")}</span>
          )}
        </dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutVersion")}</dt>
        <dd>
          {product.version > 0 ? (
            <Badge variant="secondary" className="font-mono text-[10px]">
              {t("dataProducts.versionBadge", { version: product.version })}
            </Badge>
          ) : (
            <span className="text-muted-foreground">—</span>
          )}
        </dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutTables")}</dt>
        <dd>{product.member_count}</dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutCreatedBy")}</dt>
        <dd>{product.created_by || t("dataProducts.aboutUnknown")}</dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutCreatedAt")}</dt>
        <dd>{product.created_at ? formatDateShort(product.created_at) : t("dataProducts.aboutUnknown")}</dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutUpdatedBy")}</dt>
        <dd>{product.updated_by || t("dataProducts.aboutUnknown")}</dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutUpdatedAt")}</dt>
        <dd>{product.updated_at ? formatDateShort(product.updated_at) : t("dataProducts.aboutUnknown")}</dd>
      </dl>
    </section>
  );

  if (!canEdit) {
    return (
      <div className="space-y-6 max-w-2xl pt-4">
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
        {metadataSection}
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-2xl pt-4">
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
      {metadataSection}
    </div>
  );
}
