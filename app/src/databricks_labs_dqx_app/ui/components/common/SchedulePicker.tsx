/** Ported from dqlake's `components/common/SchedulePicker.tsx`. Adapted:
 *  dqlake's `ScheduleBuffer` bundles `{mode, cadence, time, cron, timezone}`
 *  because dqlake's edit-state stores that whole shape; this app's
 *  `useEditProductState` (Task 7) buffers only a flat `schedule_cron` /
 *  `schedule_tz` pair, so `mode`/`cadence`/`time` live as local UI-only
 *  state here and get folded into a concrete cron string via
 *  `simpleToCron`/`cronToSimple` on every edit — the parent only ever sees
 *  the resulting cron + timezone. Cron dialect is also swapped: 5-field
 *  POSIX (see `lib/cron.ts`) instead of dqlake's 6/7-field Quartz.
 */
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { TimezonePicker } from "@/components/common/TimezonePicker";
import { cronHint, cronToSimple, isValidCron, simpleToCron, type ScheduleMode, type SimpleCadence } from "@/lib/cron";

interface Props {
  /** Current cron expression (5-field). Always a concrete string while the
   *  schedule editor is shown — the parent seeds a valid default on "Add
   *  schedule". */
  cron: string;
  timezone: string;
  onChange: (cron: string, timezone: string) => void;
  /** Reports whether the currently displayed cron is one the backend
   *  scheduler will accept, so the parent can gate Save. */
  onValidityChange: (valid: boolean) => void;
  canEdit: boolean;
}

const HOURS = Array.from({ length: 24 }, (_, i) => String(i).padStart(2, "0"));
const MINUTES = Array.from({ length: 60 }, (_, i) => String(i).padStart(2, "0"));

/** Cadence/at row + "Show cron syntax" checkbox + raw cron field + timezone
 *  picker. Mode/cadence/time are local UI state derived once from the
 *  incoming cron on mount; every edit re-derives a concrete cron and pushes
 *  it up via `onChange` so the parent's buffer is always the single source
 *  of truth for what gets persisted. */
export function SchedulePicker({ cron, timezone, onChange, onValidityChange, canEdit }: Props) {
  const { t } = useTranslation();

  const initialSimple = cronToSimple(cron);
  const [mode, setMode] = useState<ScheduleMode>(initialSimple ? "simple" : "custom");
  const [cadence, setCadence] = useState<SimpleCadence>(initialSimple?.cadence ?? "daily");
  const [time, setTime] = useState<string>(initialSimple?.time ?? "06:00");

  const cronTrimmed = cron.trim();
  const cronInvalid = mode === "custom" && !isValidCron(cronTrimmed);
  const valid = mode === "simple" || isValidCron(cronTrimmed);

  useEffect(() => {
    onValidityChange(valid);
    // Only re-report when the computed validity actually changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [valid]);

  const [hh, mm] = time.split(":");
  const hour = (hh ?? "06").padStart(2, "0");
  const minute = (mm ?? "00").padStart(2, "0");

  const pushSimple = (nextCadence: SimpleCadence, nextTime: string) => {
    onChange(simpleToCron(nextCadence, nextTime), timezone);
  };

  const setCadenceAndPush = (next: SimpleCadence) => {
    setCadence(next);
    pushSimple(next, time);
  };

  const setTimeAndPush = (nextHour: string, nextMinute: string) => {
    const next = `${nextHour}:${nextMinute}`;
    setTime(next);
    pushSimple(cadence, next);
  };

  /** Flip between simple and custom. Custom -> simple restores cadence/time
   *  from the current cron when it parses; otherwise falls back to the
   *  daily default and pushes it (keeping the buffer valid). Simple ->
   *  custom leaves the cron untouched (it already reflects the simple
   *  settings). Timezone is always preserved. */
  const toggleShowCron = (checked: boolean) => {
    if (checked) {
      setMode("custom");
      return;
    }
    const simple = cronToSimple(cron);
    if (simple) {
      setCadence(simple.cadence);
      setTime(simple.time);
      setMode("simple");
    } else {
      setCadence("daily");
      setTime("06:00");
      setMode("simple");
      onChange(simpleToCron("daily", "06:00"), timezone);
    }
  };

  const showCron = mode === "custom";

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div className="w-[26rem] max-w-full shrink-0">
          {showCron ? (
            <section className="flex flex-col gap-2">
              <Input
                value={cron}
                onChange={(e) => onChange(e.target.value, timezone)}
                disabled={!canEdit}
                placeholder="0 6 * * *"
                className="w-full font-mono"
                aria-invalid={cronInvalid}
                aria-label={t("dataProducts.scheduleCronAriaLabel")}
              />
              <p className="text-xs text-muted-foreground">{t("dataProducts.scheduleCronHelp")}</p>
              {cronInvalid ? (
                <p className="text-xs text-destructive">{t("dataProducts.scheduleCronInvalid")}</p>
              ) : cronTrimmed.length > 0 ? (
                <p className="text-xs text-muted-foreground">{cronHint(cronTrimmed, timezone, t)}</p>
              ) : null}
            </section>
          ) : (
            <section className="flex flex-wrap items-center gap-2 text-sm">
              <span className="text-sm font-medium leading-none">{t("dataProducts.scheduleEveryLabel")}</span>
              <Select
                value={cadence}
                onValueChange={(v) => setCadenceAndPush(v as SimpleCadence)}
                disabled={!canEdit}
              >
                <SelectTrigger className="w-28">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="daily">{t("dataProducts.scheduleCadenceDay")}</SelectItem>
                  <SelectItem value="hourly">{t("dataProducts.scheduleCadenceHour")}</SelectItem>
                  <SelectItem value="weekly">{t("dataProducts.scheduleCadenceWeek")}</SelectItem>
                </SelectContent>
              </Select>

              {cadence === "hourly" ? (
                <>
                  <span className="text-muted-foreground">{t("dataProducts.scheduleAtMinuteLabel")}</span>
                  <Select value={minute} onValueChange={(v) => setTimeAndPush(hour, v)} disabled={!canEdit}>
                    <SelectTrigger className="w-20">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {MINUTES.map((m) => (
                        <SelectItem key={m} value={m}>
                          {m}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </>
              ) : (
                <>
                  <span className="text-muted-foreground">{t("dataProducts.scheduleAtLabel")}</span>
                  <Select value={hour} onValueChange={(v) => setTimeAndPush(v, minute)} disabled={!canEdit}>
                    <SelectTrigger className="w-20">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {HOURS.map((h) => (
                        <SelectItem key={h} value={h}>
                          {h}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <span className="text-muted-foreground">:</span>
                  <Select value={minute} onValueChange={(v) => setTimeAndPush(hour, v)} disabled={!canEdit}>
                    <SelectTrigger className="w-20">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {MINUTES.map((m) => (
                        <SelectItem key={m} value={m}>
                          {m}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </>
              )}
            </section>
          )}
        </div>

        <label className="flex shrink-0 items-center gap-2 whitespace-nowrap pt-2 text-sm">
          <Checkbox checked={showCron} onCheckedChange={(c) => toggleShowCron(c === true)} disabled={!canEdit} />
          <span>{t("dataProducts.scheduleShowCronLabel")}</span>
        </label>
      </div>

      <section className="flex flex-col gap-3">
        <label className="text-sm font-medium leading-none">{t("dataProducts.scheduleTimezoneLabel")}</label>
        <TimezonePicker value={timezone} onChange={(v) => onChange(cron, v)} disabled={!canEdit} />
      </section>
    </div>
  );
}
