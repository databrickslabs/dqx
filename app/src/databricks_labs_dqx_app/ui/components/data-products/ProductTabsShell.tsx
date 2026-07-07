/**
 * ProductTabsShell — tab strip for the data product detail page.
 *
 * Ported from dqlake's `components/products/ProductTabsShell.tsx`. Adapted
 * per the Data Products design spec (§6): the History tab is cut entirely
 * (no history model in DQX), and — diverging deliberately from dqlake,
 * which tucks Runs into the header's ⋮ menu — Runs is a first-class visible
 * tab, taking the slot dqlake used for "Results".
 *
 * Tab order: About | Sharing, Tables | Runs  ‖  Schedule
 * `|` characters render as visible muted dividers inside the TabsList.
 */
import { useTranslation } from "react-i18next";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";

export type ProductTabKey = "about" | "sharing" | "tables" | "runs" | "scheduling";

interface Props {
  activeTab: ProductTabKey;
  onTabChange: (tab: ProductTabKey) => void;
  /** Tabs listed here are rendered visually muted and non-clickable. */
  disabledTabs?: Set<ProductTabKey>;
  children: Partial<Record<ProductTabKey, React.ReactNode>>;
}

/** All valid tab keys, in display order. The detail route validates the
 *  `?tab=` search param against this list. */
export const PRODUCT_TAB_KEYS: ProductTabKey[] = ["about", "sharing", "tables", "runs", "scheduling"];

function Separator() {
  return (
    <span className="text-muted-foreground/40 select-none px-1" aria-hidden>
      |
    </span>
  );
}

// Groups define the visual separator layout:
// [About] | [Sharing, Tables] | [Runs]  →gap→  [Schedule]
const GROUP_A: ProductTabKey[] = ["about"];
const GROUP_B: ProductTabKey[] = ["sharing", "tables"];
const GROUP_C: ProductTabKey[] = ["runs"];
const RIGHT_TABS: ProductTabKey[] = ["scheduling"];

function TabTrigger({ tabKey, label, disabled }: { tabKey: ProductTabKey; label: string; disabled: boolean }) {
  return (
    <TabsTrigger
      value={tabKey}
      disabled={disabled}
      aria-disabled={disabled}
      className={disabled ? "opacity-50 cursor-not-allowed" : undefined}
    >
      {label}
    </TabsTrigger>
  );
}

export function ProductTabsShell({ activeTab, onTabChange, disabledTabs = new Set(), children }: Props) {
  const { t } = useTranslation();
  const labelFor = (key: ProductTabKey): string =>
    ({
      about: t("dataProducts.tabAbout"),
      sharing: t("dataProducts.tabSharing"),
      tables: t("dataProducts.tabTables"),
      runs: t("dataProducts.tabRuns"),
      scheduling: t("dataProducts.tabSchedule"),
    })[key];

  return (
    <Tabs value={activeTab} onValueChange={(v) => onTabChange(v as ProductTabKey)}>
      <div className="w-full max-w-5xl flex items-center justify-between">
        <TabsList className="inline-flex items-center h-auto p-1">
          {GROUP_A.map((key) => (
            <TabTrigger key={key} tabKey={key} label={labelFor(key)} disabled={disabledTabs.has(key)} />
          ))}

          <Separator />

          {GROUP_B.map((key) => (
            <TabTrigger key={key} tabKey={key} label={labelFor(key)} disabled={disabledTabs.has(key)} />
          ))}

          <Separator />

          {GROUP_C.map((key) => (
            <TabTrigger key={key} tabKey={key} label={labelFor(key)} disabled={disabledTabs.has(key)} />
          ))}
        </TabsList>

        <TabsList className="inline-flex items-center h-auto p-1">
          {RIGHT_TABS.map((key) => (
            <TabTrigger key={key} tabKey={key} label={labelFor(key)} disabled={disabledTabs.has(key)} />
          ))}
        </TabsList>
      </div>

      {(Object.keys(children) as ProductTabKey[]).map((key) => (
        <TabsContent key={key} value={key} className="mt-6">
          {children[key]}
        </TabsContent>
      ))}
    </Tabs>
  );
}
