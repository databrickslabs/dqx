/** Ported from dqlake's `components/products/ProductAboutTab.tsx`. */
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { HelpTooltip } from "@/components/HelpTooltip";
import { formatDateShort } from "@/lib/format-utils";
import type { DataProductOut } from "@/lib/api";
import type { EditProductState } from "@/components/data-products/useEditProductState";

interface Props {
  product: DataProductOut;
  editState: EditProductState;
  canEdit: boolean;
}

/** Status pill for the Details panel — same wording as the collections table. */
function StatusPill({ status }: { status: string }) {
  const { t } = useTranslation();
  switch (status) {
    case "approved":
      return <Badge variant="default" className="text-[10px]">{t("dataProducts.statusApproved")}</Badge>;
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
      return <Badge variant="secondary" className="text-[10px]">{t("dataProducts.statusDraft")}</Badge>;
  }
}

/** Read-only provenance panel — mirrors the rule About tab's Details column.
 *  Owner is deliberately absent: it's edited on the Permissions tab. */
function DetailsPanel({ product }: { product: DataProductOut }) {
  const { t } = useTranslation();
  return (
    <section className="space-y-3 lg:w-96 lg:shrink-0">
      <h2 className="text-sm font-semibold">{t("dataProducts.aboutMetadataTitle")}</h2>
      <dl className="grid grid-cols-[130px_1fr] gap-x-4 gap-y-2 text-xs">
        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutStatus")}</dt>
        <dd>
          <StatusPill status={product.display_status} />
        </dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutVersion")}</dt>
        <dd>
          {product.version > 0 ? (
            t("dataProducts.versionBadge", { version: product.version })
          ) : (
            <span className="text-muted-foreground">—</span>
          )}
        </dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutCreatedBy")}</dt>
        <dd>{product.created_by || t("dataProducts.aboutUnknown")}</dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutCreatedAt")}</dt>
        <dd>{product.created_at ? formatDateShort(product.created_at) : t("dataProducts.aboutUnknown")}</dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutUpdatedBy")}</dt>
        <dd>{product.updated_by || t("dataProducts.aboutUnknown")}</dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutUpdatedAt")}</dt>
        <dd>{product.updated_at ? formatDateShort(product.updated_at) : t("dataProducts.aboutUnknown")}</dd>

        <dt className="text-muted-foreground uppercase tracking-wide">{t("dataProducts.aboutCollectionId")}</dt>
        <dd className="font-mono break-all">{product.product_id}</dd>
      </dl>
    </section>
  );
}

export function ProductAboutTab({ product, editState, canEdit }: Props) {
  const { t } = useTranslation();
  const { name, description, notes, setName, setDescription, setNotes } = editState;

  // Two columns: editable/read fields on the left, the read-only Details
  // provenance panel on the right — mirroring the rule About tab. Owner is
  // NOT shown here; it's edited on the Permissions tab.
  return (
    <div className="flex flex-col gap-6 pt-4 lg:flex-row lg:items-start">
      <div className="space-y-6 lg:min-w-0 lg:max-w-2xl lg:flex-1">
        {canEdit ? (
          <>
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
            <section className="flex flex-col gap-3">
              <div className="flex items-center gap-1.5">
                <Label htmlFor="product-notes">{t("dataProducts.notesLabel")}</Label>
                <HelpTooltip text={t("dataProducts.notesTooltip")} />
              </div>
              <Textarea
                id="product-notes"
                value={notes}
                onChange={(e) => setNotes(e.target.value)}
                rows={3}
                placeholder={t("dataProducts.notesPlaceholder")}
              />
            </section>
          </>
        ) : (
          <>
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
            <section className="flex flex-col gap-3">
              <p className="text-sm font-medium leading-none">{t("dataProducts.notesLabel")}</p>
              {notes ? (
                <p className="text-sm whitespace-pre-wrap">{notes}</p>
              ) : (
                <p className="text-sm text-muted-foreground italic">{t("dataProducts.aboutNoNotes")}</p>
              )}
            </section>
          </>
        )}
      </div>

      <DetailsPanel product={product} />
    </div>
  );
}
