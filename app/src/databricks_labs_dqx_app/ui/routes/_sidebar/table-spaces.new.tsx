import { createFileRoute, Link, Navigate, useNavigate } from "@tanstack/react-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { ArrowLeft } from "lucide-react";
import { useCreateDataProduct } from "@/lib/api";
import { usePermissions } from "@/hooks/use-permissions";
import { ProductTabsShell, type ProductTabKey } from "@/components/data-products/ProductTabsShell";
import { PermissionsTab } from "@/components/permissions/PermissionsTab";

export const Route = createFileRoute("/_sidebar/table-spaces/new")({
  component: NewDataProductPage,
});

// Tables and Runs stay locked until the product exists — there's no
// binding/schedule/run state to show yet. Permissions, however, is
// reachable pre-save: `PermissionsTab` renders its own "save first"
// empty shell when `objectId` is empty, so the tab has real content to
// show. Reuses the real (Task 7) `ProductTabsShell` rather than a bespoke
// stand-in, matching dqlake's `new.tsx` pattern of disabling tabs via
// `disabledTabs` instead of re-approximating the strip.
//
// Monitored tables have no equivalent create form — they're created via a
// one-shot modal, not a page with tabs — so this empty-shell wiring is
// N/A there; only table spaces and registry rules have a tabbed create
// page that needs it.
const NEW_PRODUCT_DISABLED_TABS = new Set<ProductTabKey>(["tables", "runs"]);

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

function NewDataProductPage() {
  const { t } = useTranslation();
  const perms = usePermissions();
  const navigate = useNavigate();
  const create = useCreateDataProduct();

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [activeTab, setActiveTab] = useState<ProductTabKey>("about");

  // Creating requires RULE_AUTHOR+ (`canCreateRules`) — the "New Data
  // Product" button on the list is hidden for viewers; this blocks direct
  // navigation to the page too, matching the client-side redirect pattern
  // used across the app (e.g. `rules.create.tsx`).
  if (!perms.canCreateRules) return <Navigate to="/table-spaces" replace />;

  const canSubmit = !!name.trim() && !submitting;

  const handleCreate = () => {
    if (!canSubmit) return;
    setSubmitting(true);
    create.mutate(
      { data: { name: name.trim(), description: description.trim() || undefined } },
      {
        onSuccess: (result) => {
          toast.success(t("dataProducts.toastCreated"));
          navigate({ to: "/table-spaces/$productId", params: { productId: result.data.product_id } });
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("dataProducts.toastCreateFailed")), { duration: 6000 });
        },
        onSettled: () => setSubmitting(false),
      },
    );
  };

  return (
    <FadeIn>
      <div className="space-y-4 max-w-5xl">
        <PageBreadcrumb items={[{ label: t("dataProducts.title"), to: "/table-spaces" }]} page={t("dataProducts.newProduct")} />

        <div className="border-b pb-4 flex items-start justify-between gap-4">
          <div className="space-y-1">
            <h1 className="text-xl text-muted-foreground italic">{t("dataProducts.untitledProduct")}</h1>
            <Link
              to="/table-spaces"
              className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground"
            >
              <ArrowLeft className="h-4 w-4 mr-1" /> {t("dataProducts.backToList")}
            </Link>
          </div>
        </div>

        {/* The create form renders INSIDE the shell's About slot so the gap
            between the tab strip and the first field is the shell's own
            `mt-6` — identical to the existing-space detail page. Rendering
            it as a sibling below an empty About TabsContent stacked that
            `mt-6` with the form's own vertical padding and pushed the
            content noticeably further down on this page only (P23 item 16). */}
        <ProductTabsShell activeTab={activeTab} onTabChange={setActiveTab} disabledTabs={NEW_PRODUCT_DISABLED_TABS}>
          {{
            permissions: <PermissionsTab objectType="data_product" objectId="" />,
            about: (
              <div className="space-y-6 max-w-xl">
                <div className="flex flex-col gap-3">
                  <Label htmlFor="dp-name">
                    {t("dataProducts.nameLabel")} <span className="text-destructive">*</span>
                  </Label>
                  <Input
                    id="dp-name"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    placeholder={t("dataProducts.namePlaceholder")}
                    autoFocus
                    onKeyDown={(e) => {
                      if (e.key === "Enter") handleCreate();
                    }}
                  />
                </div>
                <div className="flex flex-col gap-3">
                  <Label htmlFor="dp-desc">{t("dataProducts.descriptionLabel")}</Label>
                  <Textarea
                    id="dp-desc"
                    value={description}
                    onChange={(e) => setDescription(e.target.value)}
                    placeholder={t("dataProducts.descriptionPlaceholder")}
                  />
                </div>

                <div className="flex gap-2">
                  <Button onClick={handleCreate} disabled={!canSubmit}>
                    {submitting ? t("dataProducts.creating") : t("dataProducts.createButton")}
                  </Button>
                  <Button type="button" variant="outline" asChild>
                    <Link to="/table-spaces">{t("common.cancel")}</Link>
                  </Button>
                </div>
              </div>
            ),
          }}
        </ProductTabsShell>
      </div>
    </FadeIn>
  );
}
