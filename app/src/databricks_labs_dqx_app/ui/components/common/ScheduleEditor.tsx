/** Shared schedule-editor body used by both the Table Space Schedule tab
 *  (`ProductSchedulingTab`) and the Monitored Table Schedule tab
 *  (`MonitoredTableSchedulingTab`). Extracts the empty-state → "Add schedule"
 *  → `SchedulePicker` + "Remove schedule" composition both need (P21 item 14)
 *  so the two flows can't drift. Callers own persistence: the Table Space
 *  version buffers into `useEditProductState` and saves via the header; the
 *  monitored-table version renders its own Save button through the `actions`
 *  slot below.
 */
import { useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { SchedulePicker } from "@/components/common/SchedulePicker";
import { simpleToCron } from "@/lib/cron";

const DEFAULT_CADENCE_CRON = simpleToCron("daily", "06:00");
const DEFAULT_TZ = "UTC";

interface Props {
  /** Current cron (5-field) or null when there is no schedule. */
  cron: string | null;
  timezone: string;
  canEdit: boolean;
  /** Called on every picker edit with the concrete cron + timezone. */
  onChange: (cron: string, timezone: string) => void;
  /** Called when the schedule is removed (cron cleared to null). */
  onRemove: () => void;
  /** Reports whether the displayed cron is one the backend scheduler accepts. */
  onValidityChange: (valid: boolean) => void;
  /** Extra controls (e.g. a Save button) rendered in the editor footer. */
  actions?: ReactNode;
  /** Optional footer note shown under the editor. */
  footerNote?: string;
}

export function ScheduleEditor({
  cron,
  timezone,
  canEdit,
  onChange,
  onRemove,
  onValidityChange,
  actions,
  footerNote,
}: Props) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(false);

  const showEditor = cron !== null || editing;

  if (!showEditor) {
    return (
      <div className="space-y-4 max-w-xl">
        <p className="text-sm text-muted-foreground">{t("dataProducts.scheduleEmptyText")}</p>
        {canEdit && (
          <Button
            size="sm"
            onClick={() => {
              setEditing(true);
              onChange(DEFAULT_CADENCE_CRON, DEFAULT_TZ);
              onValidityChange(true);
            }}
          >
            {t("dataProducts.scheduleAddButton")}
          </Button>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-xl">
      <SchedulePicker
        cron={cron ?? DEFAULT_CADENCE_CRON}
        timezone={timezone}
        onChange={onChange}
        onValidityChange={onValidityChange}
        canEdit={canEdit}
      />

      <div className="flex flex-wrap items-center gap-2">
        {canEdit && (
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              onRemove();
              onValidityChange(true);
              setEditing(false);
            }}
          >
            {t("dataProducts.scheduleRemoveButton")}
          </Button>
        )}
        {actions}
      </div>
      {footerNote && <p className="text-xs text-muted-foreground">{footerNote}</p>}
    </div>
  );
}
