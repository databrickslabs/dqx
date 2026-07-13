import { Suspense } from "react";
import { Link } from "@tanstack/react-router";
import type { LucideIcon } from "lucide-react";
import { Boxes, Library, Settings, Table2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Card, CardContent } from "@/components/ui/card";
import { usePermissions } from "@/hooks/use-permissions";

/**
 * Faithful port of dqlake's `components/home/HomeGrid.tsx` — the "Get
 * Started" nav-card grid — with the card targets adapted to OUR routes,
 * icons matching our sidebar, and all display text through t().
 *
 * Card names reuse the sidebar.* keys so the grid can never drift from
 * the nav labels; only the descriptions are home-specific.
 *
 * The Results tile was dropped (Results is reachable per-table, not a
 * primary get-started destination). The Admin Settings tile renders only
 * for admins and spans the full row width beneath the RR/MT/TS row.
 */
const BASE_TILES: { nameKey: string; descriptionKey: string; href: string; icon: LucideIcon }[] = [
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
];

// The admin-gated "Admin Settings" tile (Config route). Reuses the existing
// `home.grid.config` label key (relabelled to "Admin Settings" in the
// locales) so it stays consistent with the user-menu entry.
const ADMIN_TILE = {
  nameKey: "home.grid.config",
  descriptionKey: "home.grid.configDesc",
  href: "/config",
  icon: Settings,
};

function GridCard({
  nameKey,
  descriptionKey,
  href,
  icon: Icon,
  className,
}: {
  nameKey: string;
  descriptionKey: string;
  href: string;
  icon: LucideIcon;
  className?: string;
}) {
  const { t } = useTranslation();
  return (
    <Link to={href} className={className}>
      <Card className="hover:bg-muted/50 transition-colors h-full">
        <CardContent className="p-5">
          <div className="flex items-center gap-3 mb-2">
            <Icon className="h-5 w-5 text-muted-foreground" />
            <h2 className="text-base font-semibold">{t(nameKey)}</h2>
          </div>
          <p className="text-sm text-muted-foreground">{t(descriptionKey)}</p>
        </CardContent>
      </Card>
    </Link>
  );
}

/** Reads the current user's role and appends the admin-only Settings tile. */
function HomeGridContent() {
  const { isAdmin } = usePermissions();
  return <HomeGridView showAdminTile={isAdmin} />;
}

function HomeGridView({ showAdminTile }: { showAdminTile: boolean }) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
      {BASE_TILES.map((tile) => (
        <GridCard key={tile.nameKey} {...tile} />
      ))}
      {showAdminTile && (
        <GridCard {...ADMIN_TILE} className="sm:col-span-2 lg:col-span-3" />
      )}
    </div>
  );
}

export function HomeGrid() {
  // While the role query resolves, show the non-admin (base) row; the admin
  // tile pops in once the role confirms admin, avoiding a suspense flash of
  // the whole home page.
  return (
    <Suspense fallback={<HomeGridView showAdminTile={false} />}>
      <HomeGridContent />
    </Suspense>
  );
}
