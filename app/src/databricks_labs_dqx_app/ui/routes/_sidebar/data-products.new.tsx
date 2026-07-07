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
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ArrowLeft } from "lucide-react";
import { useCreateDataProduct } from "@/lib/api";
import { usePermissions } from "@/hooks/use-permissions";

export const Route = createFileRoute("/_sidebar/data-products/new")({
  component: NewDataProductPage,
});

/**
 * Simplified stand-in for dqlake's `ProductTabsShell` — only the visual tab
 * strip is ported here (grouping: About | Sharing, Tables | Runs ‖
 * Schedule, matching the design spec's tab layout minus the cut History
 * tab). Task 7 replaces this with the real shared `ProductTabsShell`
 * component once the detail page needs it too; every non-About tab is
 * locked (`aria-disabled`) since none of that state exists until the
 * product is created.
 */
function LockedTabStrip() {
  const { t } = useTranslation();
  return (
    <Tabs value="about">
      <div className="w-full max-w-5xl flex items-center justify-between">
        <TabsList className="inline-flex items-center h-auto p-1">
          <TabsTrigger value="about">{t("dataProducts.tabAbout")}</TabsTrigger>
          <span className="text-muted-foreground/40 select-none px-1" aria-hidden>|</span>
          <TabsTrigger value="sharing" disabled aria-disabled className="opacity-50 cursor-not-allowed">
            {t("dataProducts.tabSharing")}
          </TabsTrigger>
          <TabsTrigger value="tables" disabled aria-disabled className="opacity-50 cursor-not-allowed">
            {t("dataProducts.tabTables")}
          </TabsTrigger>
          <span className="text-muted-foreground/40 select-none px-1" aria-hidden>|</span>
          <TabsTrigger value="runs" disabled aria-disabled className="opacity-50 cursor-not-allowed">
            {t("dataProducts.tabRuns")}
          </TabsTrigger>
        </TabsList>
        <TabsList className="inline-flex items-center h-auto p-1">
          <TabsTrigger value="scheduling" disabled aria-disabled className="opacity-50 cursor-not-allowed">
            {t("dataProducts.tabSchedule")}
          </TabsTrigger>
        </TabsList>
      </div>
      <TabsContent value="about" className="mt-6" />
    </Tabs>
  );
}

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

  // Creating requires RULE_AUTHOR+ (`canCreateRules`) — the "New Data
  // Product" button on the list is hidden for viewers; this blocks direct
  // navigation to the page too, matching the client-side redirect pattern
  // used across the app (e.g. `rules.create.tsx`).
  if (!perms.canCreateRules) return <Navigate to="/data-products" replace />;

  const canSubmit = !!name.trim() && !submitting;

  const handleCreate = () => {
    if (!canSubmit) return;
    setSubmitting(true);
    create.mutate(
      { data: { name: name.trim(), description: description.trim() || undefined } },
      {
        onSuccess: (result) => {
          toast.success(t("dataProducts.toastCreated"));
          navigate({ to: "/data-products/$productId", params: { productId: result.data.product_id } });
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
        <PageBreadcrumb items={[{ label: t("dataProducts.title"), to: "/data-products" }]} page={t("dataProducts.newProduct")} />

        <div className="border-b pb-4 flex items-start justify-between gap-4">
          <div className="space-y-1">
            <h1 className="text-xl text-muted-foreground italic">{t("dataProducts.untitledProduct")}</h1>
            <Link
              to="/data-products"
              className="inline-flex items-center text-sm text-muted-foreground hover:text-foreground"
            >
              <ArrowLeft className="h-4 w-4 mr-1" /> {t("dataProducts.backToList")}
            </Link>
          </div>
        </div>

        <LockedTabStrip />

        <div className="space-y-6 py-4 max-w-xl">
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
              <Link to="/data-products">{t("common.cancel")}</Link>
            </Button>
          </div>
        </div>
      </div>
    </FadeIn>
  );
}
