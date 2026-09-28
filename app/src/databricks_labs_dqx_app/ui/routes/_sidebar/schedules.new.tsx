import { useMemo, useState } from "react";
import { createFileRoute, Link, Navigate, useNavigate } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { AlertTriangle, LayoutGrid, Loader2, Save, Table2, X } from "lucide-react";
import { FadeIn } from "@/components/anim/FadeIn";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ScheduleEditor, DEFAULT_SCHEDULE_KIND, type ScheduleKind } from "@/components/common/ScheduleEditor";
import { ScheduleGrantWarning } from "@/components/common/ScheduleGrantWarning";
import { ScheduleTargetPickerDialog } from "@/components/schedules/ScheduleTargetPickerDialog";
import { ScheduleTypeBadge } from "@/components/schedules/SchedulesTable";
import { collectionTarget, tableTarget, type ScheduleTarget } from "@/components/schedules/schedule-target";
import { useScheduleGrantPreflight } from "@/components/schedules/useScheduleGrantPreflight";
import { usePermissions } from "@/hooks/use-permissions";
import {
  getGetDataProductQueryKey,
  getListDataProductsQueryKey,
  getListScheduleOverviewQueryKey,
  updateDataProduct,
  updateMonitoredTableSchedule,
  useListDataProducts,
  useListMonitoredTables,
} from "@/lib/api";
import { cronHint, simpleToCron } from "@/lib/cron";
import { invalidateAfterMonitoredTableChange } from "@/lib/monitored-table-invalidation";

export const Route = createFileRoute("/_sidebar/schedules/new")({
  component: NewSchedulePage,
});

const DEFAULT_CRON = simpleToCron("daily", "06:00");
const DEFAULT_TZ = "UTC";

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

function NewSchedulePage() {
  const perms = usePermissions();
  // Setting a table/collection schedule needs RULE_AUTHOR+ — the same gate as
  // the Schedule tab's Save button and the list page's "New schedule" button.
  if (!perms.canCreateRules) return <Navigate to="/schedules" replace />;
  return <NewScheduleForm />;
}

function NewScheduleForm() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [target, setTarget] = useState<ScheduleTarget | null>(null);
  const [picker, setPicker] = useState<ScheduleTarget["kind"] | null>(null);

  const [cron, setCron] = useState<string>(DEFAULT_CRON);
  const [tz, setTz] = useState<string>(DEFAULT_TZ);
  const [scheduleKind, setScheduleKind] = useState<ScheduleKind>(DEFAULT_SCHEDULE_KIND);
  // 0 = scan the whole table.
  const [sampleSize, setSampleSize] = useState<number>(0);
  const [cronInvalid, setCronInvalid] = useState(false);
  const [saving, setSaving] = useState(false);

  // Picker sources load only once the matching picker has been opened.
  const tablesQuery = useListMonitoredTables(undefined, { query: { enabled: picker === "table" } });
  const productsQuery = useListDataProducts({ query: { enabled: picker === "collection" } });
  const tableTargets = useMemo(
    () => (tablesQuery.data?.data ?? []).map((summary) => tableTarget(summary.table)),
    [tablesQuery.data],
  );
  const collectionTargets = useMemo(
    () =>
      (productsQuery.data?.data ?? []).map((product) =>
        collectionTarget(product, t("schedules.collectionTables", { count: product.member_count ?? 0 })),
      ),
    [productsQuery.data, t],
  );

  const preflight = useScheduleGrantPreflight(target?.tableFqns ?? [], target !== null);
  const blockForSchedule = preflight.blockedTables.length > 0;
  const canSave = target !== null && !cronInvalid && !blockForSchedule && !preflight.isFetching && !saving;

  const chooseTarget = (next: ScheduleTarget) => {
    setTarget(next);
    // Seed from the target's current schedule so saving is an edit of it,
    // not a blind overwrite; unscheduled targets keep what's been set here.
    if (next.existing) {
      setCron(next.existing.cron);
      setTz(next.existing.timezone);
      setScheduleKind(next.existing.kind ?? DEFAULT_SCHEDULE_KIND);
      setSampleSize(next.existing.sampleSize);
      setCronInvalid(false);
    }
  };

  const handleSave = () => {
    if (!target || !canSave) return;
    const schedule = {
      schedule_cron: cron,
      schedule_tz: tz,
      schedule_kind: scheduleKind,
      schedule_sample_size: sampleSize,
    };
    setSaving(true);
    const request =
      target.kind === "table"
        ? updateMonitoredTableSchedule(target.id, schedule)
        : updateDataProduct(target.id, schedule);
    request
      .then(() => {
        if (target.kind === "table") {
          invalidateAfterMonitoredTableChange(queryClient, target.id);
        } else {
          void queryClient.invalidateQueries({ queryKey: getListDataProductsQueryKey() });
          void queryClient.invalidateQueries({ queryKey: getGetDataProductQueryKey(target.id) });
        }
        void queryClient.invalidateQueries({ queryKey: getListScheduleOverviewQueryKey() });
        toast.success(t("schedules.createdToast"));
        void navigate({ to: "/schedules" });
      })
      .catch((err: unknown) => {
        toast.error(extractApiError(err, t("monitoredTables.scheduleToastSaveFailed")), { duration: 6000 });
      })
      .finally(() => setSaving(false));
  };

  return (
    <FadeIn>
      <div className="max-w-5xl space-y-6">
        <PageBreadcrumb items={[{ label: t("schedules.title"), to: "/schedules" }]} page={t("schedules.newSchedule")} />
        <h1 className="text-2xl font-semibold tracking-tight">{t("schedules.newSchedule")}</h1>

        <div className="max-w-xl space-y-8">
          <section className="space-y-3">
            <Label>{t("schedules.targetLabel")}</Label>
            {target ? (
              <>
                <div className="flex items-center justify-between gap-3 rounded-md border bg-muted/30 px-3 py-2">
                  <div className="min-w-0 space-y-0.5">
                    <div className="flex min-w-0 items-center gap-2">
                      <ScheduleTypeBadge type={target.kind} />
                      <span className="truncate text-sm font-medium" title={target.name}>
                        {target.name}
                      </span>
                    </div>
                    <div className="truncate text-xs text-muted-foreground" title={target.detail}>
                      {target.detail}
                    </div>
                  </div>
                  <div className="flex shrink-0 items-center gap-1">
                    <Button variant="outline" size="sm" onClick={() => setPicker(target.kind)}>
                      {t("schedules.changeTarget")}
                    </Button>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-8 w-8 p-0"
                          aria-label={t("schedules.clearTarget")}
                          onClick={() => setTarget(null)}
                        >
                          <X className="h-4 w-4" />
                        </Button>
                      </TooltipTrigger>
                      <TooltipContent>{t("schedules.clearTarget")}</TooltipContent>
                    </Tooltip>
                  </div>
                </div>
                {target.existing && (
                  <p className="flex items-start gap-2 text-xs text-amber-700 dark:text-amber-400">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                    <span>
                      {t(
                        target.kind === "table"
                          ? "schedules.replacesTableSchedule"
                          : "schedules.replacesCollectionSchedule",
                        { schedule: cronHint(target.existing.cron, target.existing.timezone, t) },
                      )}
                    </span>
                  </p>
                )}
              </>
            ) : (
              <div className="flex flex-wrap gap-2">
                <Button variant="outline" size="sm" className="gap-2" onClick={() => setPicker("table")}>
                  <Table2 className="h-4 w-4" />
                  {t("schedules.addTable")}
                </Button>
                <Button variant="outline" size="sm" className="gap-2" onClick={() => setPicker("collection")}>
                  <LayoutGrid className="h-4 w-4" />
                  {t("schedules.addCollection")}
                </Button>
              </div>
            )}
          </section>

          <ScheduleEditor
            key={target ? `${target.kind}:${target.id}` : "none"}
            cron={cron}
            timezone={tz}
            canEdit
            removable={false}
            scheduleKind={scheduleKind}
            sampleSize={sampleSize}
            onChange={(nextCron, nextTz) => {
              setCron(nextCron);
              setTz(nextTz);
            }}
            onKindChange={setScheduleKind}
            onSampleSizeChange={setSampleSize}
            onRemove={() => undefined}
            onValidityChange={(valid) => setCronInvalid(!valid)}
            banner={
              target && blockForSchedule ? (
                <ScheduleGrantWarning entity={target.kind} blockedTables={preflight.blockedTables} />
              ) : undefined
            }
            footerNote={t("monitoredTables.scheduleFooterNote")}
            emptyText={t("monitoredTables.scheduleEmptyText")}
            actions={
              <>
                <Button size="sm" onClick={handleSave} disabled={!canSave} className="gap-2">
                  {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                  {t("monitoredTables.scheduleSaveButton")}
                </Button>
                <Button size="sm" variant="outline" asChild>
                  <Link to="/schedules">{t("common.cancel")}</Link>
                </Button>
              </>
            }
          />
        </div>
      </div>

      <ScheduleTargetPickerDialog
        open={picker === "table"}
        onOpenChange={(open) => !open && setPicker(null)}
        title={t("schedules.pickTableTitle")}
        description={t("schedules.pickTableDescription")}
        searchPlaceholder={t("schedules.pickTableSearch")}
        nameLabel={t("schedules.typeTable")}
        emptyText={t("schedules.pickTableEmpty")}
        targets={tableTargets}
        isLoading={tablesQuery.isLoading}
        selectedId={target?.kind === "table" ? target.id : null}
        onSelect={chooseTarget}
      />
      <ScheduleTargetPickerDialog
        open={picker === "collection"}
        onOpenChange={(open) => !open && setPicker(null)}
        title={t("schedules.pickCollectionTitle")}
        description={t("schedules.pickCollectionDescription")}
        searchPlaceholder={t("schedules.pickCollectionSearch")}
        nameLabel={t("schedules.typeCollection")}
        emptyText={t("schedules.pickCollectionEmpty")}
        targets={collectionTargets}
        isLoading={productsQuery.isLoading}
        selectedId={target?.kind === "collection" ? target.id : null}
        onSelect={chooseTarget}
      />
    </FadeIn>
  );
}
