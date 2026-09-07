/** Table Space Schedule tab. Ported from dqlake's
 *  `components/products/ProductSchedulingTab.tsx`; now a thin wrapper over the
 *  shared `ScheduleEditor` (P21 item 14 extracted the empty-state + picker +
 *  remove composition so the monitored-table Schedule tab can reuse it).
 *  Back in the tab strip per P25 item 1 (P23 item 13 had moved it into the
 *  header ⋮ menu's dialog). Buffers into `useEditProductState`'s flat
 *  `scheduleCron`/`scheduleTz` pair and reports raw-cron validity up so the
 *  header's Save buttons stay disabled until it's fixed.
 *
 *  Task 12: scheduled runs read each member table as a service account, so a
 *  collection schedule can only be set by a caller who can grant those SPs
 *  SELECT on every member. We preflight all members; if the caller lacks
 *  MANAGE on any, we show a warning naming who can (per blocked table) and feed
 *  that into the same validity channel (`setScheduleCronInvalid`) so the header
 *  Save is disabled — the hard block.
 */
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQuery } from "@tanstack/react-query";
import { ScheduleEditor } from "@/components/common/ScheduleEditor";
import { ScheduleGrantWarning } from "@/components/common/ScheduleGrantWarning";
import { preflightScheduleGrants } from "@/lib/api";
import type { EditProductState } from "@/components/data-products/useEditProductState";

interface Props {
  editState: EditProductState;
  canEdit: boolean;
}

export function ProductSchedulingTab({ editState, canEdit }: Props) {
  const { t } = useTranslation();
  const {
    scheduleCron,
    scheduleTz,
    setSchedule,
    scheduleKind,
    setScheduleKind,
    scheduleSampleSize,
    setScheduleSampleSize,
    setScheduleCronInvalid,
    members,
  } = editState;

  // Real member table FQNs (synthetic cross-table checks have no source table).
  const memberFqns = useMemo(
    () => (members ?? []).map((m) => m.table_fqn).filter((f) => f && !f.startsWith("__sql_check__/")),
    [members],
  );

  const preflightQuery = useQuery({
    queryKey: ["scheduleGrantPreflight", "collection", memberFqns],
    queryFn: async () => (await preflightScheduleGrants({ table_fqns: memberFqns })).data,
    enabled: canEdit && memberFqns.length > 0,
    staleTime: 60_000,
  });
  const blockedTables = (preflightQuery.data?.tables ?? []).filter((tbl) => !tbl.can_manage);
  const cannotManage = blockedTables.length > 0;
  // Only block when a schedule is actually set/being set — clearing it (null
  // cron) needs no grant, matching the backend gate. Unrelated edits (name,
  // members) stay saveable.
  const blockForSchedule = cannotManage && scheduleCron !== null;

  // Combine raw-cron validity with grant-ability into the single validity
  // channel the header Save reads — either one being false disables the save.
  const [cronValid, setCronValid] = useState(true);
  useEffect(() => {
    setScheduleCronInvalid(!cronValid || blockForSchedule);
  }, [cronValid, blockForSchedule, setScheduleCronInvalid]);

  return (
    <ScheduleEditor
      cron={scheduleCron}
      timezone={scheduleTz}
      canEdit={canEdit}
      scheduleKind={scheduleKind}
      sampleSize={scheduleSampleSize}
      onChange={(cron, tz) => setSchedule(cron, tz)}
      onKindChange={setScheduleKind}
      onSampleSizeChange={setScheduleSampleSize}
      onRemove={() => setSchedule(null)}
      onValidityChange={setCronValid}
      banner={cannotManage ? <ScheduleGrantWarning entity="collection" blockedTables={blockedTables} /> : undefined}
      footerNote={t("dataProducts.scheduleFooterNote")}
      emptyText={t("dataProducts.scheduleEmptyText")}
    />
  );
}
