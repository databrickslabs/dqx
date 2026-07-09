/**
 * ProductTabsShell — tab strip for the data product detail page.
 *
 * Ported from dqlake's `components/products/ProductTabsShell.tsx`. Adapted
 * per the Data Products design spec (§6): the History tab is cut entirely
 * (no history model in DQX). Runs is dqlake-exact — it is NOT a visible tab;
 * it lives in the header's ⋮ menu (P21 item 29) and its content is still
 * reachable via `?tab=runs` deep links (the menu navigates there). Schedule
 * left the strip too (P23 item 13) — it opens as a dialog from the same ⋮
 * menu, so it has no tab key at all.
 *
 * Tab order: About | Sharing, Tables
 * `|` characters render as visible muted dividers inside the TabsList.
 */
import { useTranslation } from "react-i18next";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { History, Info, KeyRound, Table2, type LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

export type ProductTabKey = "about" | "permissions" | "tables" | "runs";

// Same icon-strip treatment as the Monitored Tables detail tab bar
// (routes/_sidebar/monitored-tables.$bindingId.tsx) — gap-1.5 + h-3.5 w-3.5
// icons ahead of the label.
const TAB_ICONS: Record<ProductTabKey, LucideIcon> = {
  about: Info,
  permissions: KeyRound,
  tables: Table2,
  runs: History,
};

interface Props {
  activeTab: ProductTabKey;
  onTabChange: (tab: ProductTabKey) => void;
  /** Tabs listed here are rendered visually muted and non-clickable. */
  disabledTabs?: Set<ProductTabKey>;
  children: Partial<Record<ProductTabKey, React.ReactNode>>;
}

/** All valid tab keys, in display order. The detail route validates the
 *  `?tab=` search param against this list. */
export const PRODUCT_TAB_KEYS: ProductTabKey[] = ["about", "permissions", "tables", "runs"];

function Separator() {
  return (
    <span className="text-muted-foreground/40 select-none px-1" aria-hidden>
      |
    </span>
  );
}

// Groups define the visual separator layout: [About] | [Sharing, Tables]
// Runs is intentionally absent from the strip — it lives in the header ⋮
// menu (P21 item 29) and is still reachable by `?tab=runs`. Schedule is
// absent too (P23 item 13): it opens as a dialog from the same ⋮ menu.
const GROUP_A: ProductTabKey[] = ["about"];
const GROUP_B: ProductTabKey[] = ["permissions", "tables"];

function TabTrigger({ tabKey, label, disabled }: { tabKey: ProductTabKey; label: string; disabled: boolean }) {
  const Icon = TAB_ICONS[tabKey];
  return (
    <TabsTrigger
      value={tabKey}
      disabled={disabled}
      aria-disabled={disabled}
      className={cn("gap-1.5", disabled && "opacity-50 cursor-not-allowed")}
    >
      <Icon className="h-3.5 w-3.5" />
      {label}
    </TabsTrigger>
  );
}

export function ProductTabsShell({ activeTab, onTabChange, disabledTabs = new Set(), children }: Props) {
  const { t } = useTranslation();
  const labelFor = (key: ProductTabKey): string =>
    ({
      about: t("dataProducts.tabAbout"),
      permissions: t("dataProducts.tabPermissions"),
      tables: t("dataProducts.tabTables"),
      runs: t("dataProducts.tabRuns"),
    })[key];

  return (
    <Tabs value={activeTab} onValueChange={(v) => onTabChange(v as ProductTabKey)}>
      <div className="w-full max-w-5xl flex items-center">
        <TabsList className="inline-flex items-center h-auto p-1">
          {GROUP_A.map((key) => (
            <TabTrigger key={key} tabKey={key} label={labelFor(key)} disabled={disabledTabs.has(key)} />
          ))}

          <Separator />

          {GROUP_B.map((key) => (
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
