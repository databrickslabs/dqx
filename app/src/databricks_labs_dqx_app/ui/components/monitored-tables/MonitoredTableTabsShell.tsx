import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { BarChart3, ClipboardList, Columns3, Info } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

/**
 * Shared tab strip for the monitored-table pages — the "new monitor"
 * creation flow and (candidate for a future refactor) the binding detail
 * page. Ported from dqlake's `BindingTabsShell`, trimmed to the tab set
 * DQX actually has (About, Profile, Apply Rules, Results — no
 * Sharing/Scheduling/Runs/History).
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
  { key: "results", labelKey: "monitoredTables.tabResults", icon: ClipboardList },
];

export function MonitoredTableTabsShell({ activeTab, onTabChange, disabledTabs = new Set(), children }: Props) {
  const { t } = useTranslation();
  return (
    <Tabs value={activeTab} onValueChange={(v) => onTabChange(v as MonitoredTableTabKey)}>
      <TabsList>
        {ALL_TABS.map(({ key, labelKey, icon: Icon }) => {
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
        })}
      </TabsList>

      {(Object.keys(children) as MonitoredTableTabKey[]).map((key) => (
        <TabsContent key={key} value={key}>
          {children[key]}
        </TabsContent>
      ))}
    </Tabs>
  );
}
