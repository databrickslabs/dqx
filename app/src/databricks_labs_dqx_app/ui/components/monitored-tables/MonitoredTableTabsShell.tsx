import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { BarChart3, LineChart, Columns3, Info } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

/**
 * Shared tab strip for the monitored-table pages — the "new monitor"
 * creation flow and (candidate for a future refactor) the binding detail
 * page. Ported from dqlake's `BindingTabsShell`, trimmed to the tab set
 * DQX actually has (About, Profile, Apply Rules, Results — no
 * Sharing/Scheduling/Runs/History).
 *
 * Divider grouping matches dqlake's `[About] | [Profile, Apply Rules] |
 * [Results]` clusters (dqlake's Sharing tab has no DQX equivalent, so
 * GROUP_A here is just About): a divider between About/Profile, and another
 * between Apply Rules/Results.
 */
export type MonitoredTableTabKey = "about" | "profile" | "apply" | "results";

interface Props {
  activeTab: MonitoredTableTabKey;
  onTabChange: (tab: MonitoredTableTabKey) => void;
  /** Tabs listed here are rendered visually muted and non-clickable — used
   *  by the "new monitor" flow before the table(s) have been created. */
  disabledTabs?: Set<MonitoredTableTabKey>;
  children: Partial<Record<MonitoredTableTabKey, ReactNode>>;
}

const ALL_TABS: { key: MonitoredTableTabKey; labelKey: string; icon: typeof Info }[] = [
  { key: "about", labelKey: "monitoredTables.tabAbout", icon: Info },
  { key: "profile", labelKey: "monitoredTables.tabProfile", icon: BarChart3 },
  { key: "apply", labelKey: "monitoredTables.tabApplyRules", icon: Columns3 },
  { key: "results", labelKey: "monitoredTables.tabResults", icon: LineChart },
];

// Groups define the visual separator layout — mirrors dqlake's
// BindingTabsShell GROUP_A/GROUP_B/GROUP_C split:
// [about]  →divider→  [profile, apply]  →divider→  [results]
const GROUP_A: MonitoredTableTabKey[] = ["about"];
const GROUP_B: MonitoredTableTabKey[] = ["profile", "apply"];
const GROUP_C: MonitoredTableTabKey[] = ["results"];

/**
 * Muted vertical rule between tab groups. Uses `muted-foreground` (not
 * dqlake's literal `|` glyph) because `TabsList`'s `bg-muted` background
 * resolves to the same oklch value as `border` in the dark theme — a plain
 * border color divider would be invisible. `muted-foreground` is calibrated
 * to contrast against `muted` in both themes, matching dqlake's separator
 * intent (a visible-but-quiet divider) without the contrast bug.
 */
function TabGroupDivider() {
  return <div aria-hidden="true" className="mx-1 self-stretch w-px my-1.5 bg-muted-foreground/40" />;
}

export function MonitoredTableTabsShell({ activeTab, onTabChange, disabledTabs = new Set(), children }: Props) {
  const { t } = useTranslation();

  function renderTrigger(key: MonitoredTableTabKey) {
    const { labelKey, icon: Icon } = ALL_TABS.find((tab) => tab.key === key)!;
    const disabled = disabledTabs.has(key);
    return (
      <TabsTrigger
        key={key}
        value={key}
        disabled={disabled}
        aria-disabled={disabled}
        title={disabled ? t("monitoredTables.wizard.lockedTabHint") : undefined}
        className={disabled ? "gap-1.5 opacity-50 cursor-not-allowed" : "gap-1.5"}
      >
        <Icon className="h-3.5 w-3.5" />
        {t(labelKey)}
      </TabsTrigger>
    );
  }

  return (
    <Tabs value={activeTab} onValueChange={(v) => onTabChange(v as MonitoredTableTabKey)}>
      <TabsList className="inline-flex items-center h-auto p-1">
        {GROUP_A.map(renderTrigger)}
        <TabGroupDivider />
        {GROUP_B.map(renderTrigger)}
        <TabGroupDivider />
        {GROUP_C.map(renderTrigger)}
      </TabsList>

      {(Object.keys(children) as MonitoredTableTabKey[]).map((key) => (
        <TabsContent key={key} value={key}>
          {children[key]}
        </TabsContent>
      ))}
    </Tabs>
  );
}
