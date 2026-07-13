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
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { SchedulePicker } from "@/components/common/SchedulePicker";
import { simpleToCron } from "@/lib/cron";

const DEFAULT_CADENCE_CRON = simpleToCron("daily", "06:00");
const DEFAULT_TZ = "UTC";

/** What a scheduled run does (B2-52). Kept in step with the backend
 *  ``schedule_kind`` enum; default is ``dq_only``. */
export type ScheduleKind = "profiling_only" | "dq_only" | "profiling_and_dq";

export const DEFAULT_SCHEDULE_KIND: ScheduleKind = "dq_only";

const SCHEDULE_KIND_ORDER: ScheduleKind[] = ["profiling_only", "dq_only", "profiling_and_dq"];

interface Props {
  /** Current cron (5-field) or null when there is no schedule. */
  cron: string | null;
  timezone: string;
  canEdit: boolean;
  /** What a due run does: profiling only, DQ only, or both (B2-52). */
  scheduleKind: ScheduleKind;
  /** Called on every picker edit with the concrete cron + timezone. */
  onChange: (cron: string, timezone: string) => void;
  /** Called when the schedule-scope dropdown changes. */
  onKindChange: (kind: ScheduleKind) => void;
  /** Called when the schedule is removed (cron cleared to null). */
  onRemove: () => void;
  /** Reports whether the displayed cron is one the backend scheduler accepts. */
  onValidityChange: (valid: boolean) => void;
  /** Extra controls (e.g. a Save button) rendered in the editor footer. */
  actions?: ReactNode;
  /** Optional footer note shown under the editor. */
  footerNote?: string;
  /** Empty-state copy, translated by the caller so it can name the right entity
   *  (e.g. "table space" vs "table") instead of hard-coding one noun for both tabs. */
  emptyText: string;
}

export function ScheduleEditor({
  cron,
  timezone,
  canEdit,
  scheduleKind,
  onChange,
  onKindChange,
  onRemove,
  onValidityChange,
  actions,
  footerNote,
  emptyText,
}: Props) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(false);

  const showEditor = cron !== null || editing;

  if (!showEditor) {
    return (
      <div className="space-y-4 max-w-xl">
        <p className="text-sm text-muted-foreground">{emptyText}</p>
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
      <div className="space-y-2">
        <Label htmlFor="schedule-kind">{t("schedule.kindLabel")}</Label>
        <Select
          value={scheduleKind}
          onValueChange={(value) => onKindChange(value as ScheduleKind)}
          disabled={!canEdit}
        >
          <SelectTrigger id="schedule-kind" className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {SCHEDULE_KIND_ORDER.map((kind) => (
              <SelectItem key={kind} value={kind}>
                {t(`schedule.kindOption.${kind}`)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

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
