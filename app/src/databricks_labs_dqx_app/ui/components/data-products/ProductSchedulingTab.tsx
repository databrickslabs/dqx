/** Table Space Schedule tab. Ported from dqlake's
 *  `components/products/ProductSchedulingTab.tsx`; now a thin wrapper over the
 *  shared `ScheduleEditor` (P21 item 14 extracted the empty-state + picker +
 *  remove composition so the monitored-table Schedule tab can reuse it).
 *  Back in the tab strip per P25 item 1 (P23 item 13 had moved it into the
 *  header ⋮ menu's dialog). Buffers into `useEditProductState`'s flat
 *  `scheduleCron`/`scheduleTz` pair and reports raw-cron validity up so the
 *  header's Save buttons stay disabled until it's fixed.
 */
import { useTranslation } from "react-i18next";
import { ScheduleEditor } from "@/components/common/ScheduleEditor";
import type { EditProductState } from "@/components/data-products/useEditProductState";

interface Props {
  editState: EditProductState;
  canEdit: boolean;
}

export function ProductSchedulingTab({ editState, canEdit }: Props) {
  const { t } = useTranslation();
  const { scheduleCron, scheduleTz, setSchedule, scheduleKind, setScheduleKind, setScheduleCronInvalid } = editState;

  return (
    <ScheduleEditor
      cron={scheduleCron}
      timezone={scheduleTz}
      canEdit={canEdit}
      scheduleKind={scheduleKind}
      onChange={(cron, tz) => setSchedule(cron, tz)}
      onKindChange={setScheduleKind}
      onRemove={() => setSchedule(null)}
      onValidityChange={(valid) => setScheduleCronInvalid(!valid)}
      footerNote={t("dataProducts.scheduleFooterNote")}
      emptyText={t("dataProducts.scheduleEmptyText")}
    />
  );
}
