import { Link } from "@tanstack/react-router";
import type { LucideIcon } from "lucide-react";
import { Boxes, LineChart, Library, Settings, Table2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Card, CardContent } from "@/components/ui/card";

/**
 * Faithful port of dqlake's `components/home/HomeGrid.tsx` — the "Get
 * Started" nav-card grid — with the card targets adapted to OUR routes
 * (Rules Registry, Monitored Tables, Table Spaces, Results, Config
 * instead of dqlake's Rules/Tables/Products/Issues/Admin), icons matching
 * our sidebar, and all display text through t().
 *
 * Card names reuse the sidebar.* keys so the grid can never drift from
 * the nav labels; only the descriptions are home-specific.
 */
const TABS: { nameKey: string; descriptionKey: string; href: string; icon: LucideIcon }[] = [
  {
    nameKey: "sidebar.rulesRegistry",
    descriptionKey: "home.grid.rulesRegistryDesc",
    href: "/registry-rules",
    icon: Library,
  },
  {
    nameKey: "sidebar.monitoredTables",
    descriptionKey: "home.grid.monitoredTablesDesc",
    href: "/monitored-tables",
    icon: Table2,
  },
  {
    nameKey: "sidebar.dataProducts",
    descriptionKey: "home.grid.tableSpacesDesc",
    href: "/table-spaces",
    icon: Boxes,
  },
  {
    nameKey: "sidebar.results",
    descriptionKey: "home.grid.resultsDesc",
    href: "/results",
    icon: LineChart,
  },
  {
    nameKey: "home.grid.config",
    descriptionKey: "home.grid.configDesc",
    href: "/config",
    icon: Settings,
  },
];

export function HomeGrid() {
  const { t } = useTranslation();
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
      {TABS.map((tab) => {
        const Icon = tab.icon;
        return (
          <Link key={tab.nameKey} to={tab.href}>
            <Card className="hover:bg-muted/50 transition-colors h-full">
              <CardContent className="p-5">
                <div className="flex items-center gap-3 mb-2">
                  <Icon className="h-5 w-5 text-muted-foreground" />
                  <h2 className="text-base font-semibold">{t(tab.nameKey)}</h2>
                </div>
                <p className="text-sm text-muted-foreground">{t(tab.descriptionKey)}</p>
              </CardContent>
            </Card>
          </Link>
        );
      })}
    </div>
  );
}
