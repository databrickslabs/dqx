/** Monitored Table Schedule tab (P21 item 14; back in the tab strip per P25
 *  item 1, reverting P23 item 13's move into the header ⋮ menu). Reuses the
 *  shared `ScheduleEditor` composition (empty-state → picker → remove) that
 *  the Table Space Schedule tab uses, but persists on its own: schedule is
 *  operational config orthogonal to the applied-rules draft/submit lifecycle,
 *  so it has a dedicated Save button (PATCH `/monitored-tables/{id}/schedule`)
 *  instead of riding the header's Save-as-draft. Only approved tables with a
 *  schedule fire on the in-app scheduler — the footer note says so.
 */
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { toast } from "sonner";
import { CalendarClock, Loader2, Save } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScheduleEditor, DEFAULT_SCHEDULE_KIND, type ScheduleKind } from "@/components/common/ScheduleEditor";
import { useListDataProducts, useUpdateMonitoredTableSchedule, type MonitoredTableOut } from "@/lib/api";
import { invalidateAfterMonitoredTableChange } from "@/lib/monitored-table-invalidation";
import { cronHint } from "@/lib/cron";

const DEFAULT_TZ = "UTC";

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

export function MonitoredTableSchedulingTab({
  table,
  canEdit,
}: {
  table: MonitoredTableOut;
  canEdit: boolean;
}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const bindingId = table.binding_id;

  // Local buffer, seeded from the persisted schedule. `null` cron = no schedule.
  const [cron, setCron] = useState<string | null>(table.schedule_cron ?? null);
  const [tz, setTz] = useState<string>(table.schedule_tz ?? DEFAULT_TZ);
  const [scheduleKind, setScheduleKind] = useState<ScheduleKind>(table.schedule_kind ?? DEFAULT_SCHEDULE_KIND);
  const [cronInvalid, setCronInvalid] = useState(false);

  const serverCron = table.schedule_cron ?? null;
  const serverTz = table.schedule_tz ?? DEFAULT_TZ;
  const serverKind: ScheduleKind = table.schedule_kind ?? DEFAULT_SCHEDULE_KIND;
  const dirty =
    cron !== serverCron || (cron !== null && (tz !== serverTz || scheduleKind !== serverKind));
  const canSave = dirty && !cronInvalid;

  const updateMut = useUpdateMonitoredTableSchedule({ mutation: { onError: () => {} } });

  // Cross-schedule visibility (item 21): a parent Table Space that includes
  // this table can carry its own schedule, which the in-app scheduler runs
  // against every member — including this table — independently of any
  // schedule set here. Surface those so the steward isn't surprised by
  // "extra" runs. Resolved client-side from the products list, which already
  // returns every product's members fully populated (see
  // `DataProductService.list_products`), so no dedicated reverse-lookup
  // endpoint is needed. Ported from dqlake's `BindingSchedulingTab`
  // "Scheduled by data products" section (dqlake surfaces the same list via a
  // backend `scheduling_data_products` field; DQX derives it here instead).
  const { data: productsData } = useListDataProducts();
  const schedulingSpaces = useMemo(
    () =>
      (productsData?.data ?? []).filter(
        (p) => !!p.schedule_cron && (p.members ?? []).some((m) => m.binding_id === bindingId),
      ),
    [productsData, bindingId],
  );

  const handleSave = () => {
    updateMut.mutate(
      { bindingId, data: { schedule_cron: cron, schedule_tz: cron !== null ? tz : null, schedule_kind: scheduleKind } },
      {
        onSuccess: () => {
          toast.success(t("monitoredTables.scheduleToastSaved"));
          invalidateAfterMonitoredTableChange(queryClient, bindingId);
        },
        onError: (err) => {
          toast.error(extractApiError(err, t("monitoredTables.scheduleToastSaveFailed")), { duration: 6000 });
        },
      },
    );
  };

  return (
    <div className="pt-4 space-y-8">
      <ScheduleEditor
        cron={cron}
        timezone={tz}
        canEdit={canEdit}
        scheduleKind={scheduleKind}
        onChange={(nextCron, nextTz) => {
          setCron(nextCron);
          setTz(nextTz);
        }}
        onKindChange={setScheduleKind}
        onRemove={() => setCron(null)}
        onValidityChange={(valid) => setCronInvalid(!valid)}
        footerNote={t("monitoredTables.scheduleFooterNote")}
        emptyText={t("monitoredTables.scheduleEmptyText")}
        actions={
          canEdit ? (
            <Button size="sm" onClick={handleSave} disabled={!canSave || updateMut.isPending} className="gap-2">
              {updateMut.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
              {t("monitoredTables.scheduleSaveButton")}
            </Button>
          ) : undefined
        }
      />

      {schedulingSpaces.length > 0 && (
        <section className="space-y-3 border-t pt-6 max-w-xl">
          <h3 className="text-sm font-medium">{t("monitoredTables.scheduleCrossTitle")}</h3>
          <p className="text-xs text-muted-foreground">{t("monitoredTables.scheduleCrossDescription")}</p>
          <ul className="space-y-2">
            {schedulingSpaces.map((sp) => (
              <li
                key={sp.product_id}
                className="flex items-center justify-between gap-3 rounded-md border bg-muted/30 px-3 py-2"
              >
                <div className="min-w-0 space-y-0.5">
                  <div className="flex items-center gap-1.5 text-sm font-medium">
                    <CalendarClock className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                    <span className="truncate">{sp.name}</span>
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {cronHint(sp.schedule_cron ?? "", sp.schedule_tz, t)}
                  </div>
                </div>
                <Button variant="outline" size="sm" asChild className="shrink-0">
                  <Link to="/collections/$productId" params={{ productId: sp.product_id }} search={{ tab: "scheduling" }}>
                    {t("monitoredTables.scheduleCrossManage")}
                  </Link>
                </Button>
              </li>
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}
