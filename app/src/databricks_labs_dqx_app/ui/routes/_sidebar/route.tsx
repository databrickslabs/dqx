import SidebarLayout from "@/components/layout/SidebarLayout";
import { createFileRoute, Link, useLocation } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";
import {
  ClipboardCheck,
  Gauge,
  History,
  Home,
  LayoutDashboard,
  BookOpen,
  ExternalLink,
  Library,
  Table2,
  Boxes,
} from "lucide-react";
import {
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuItem,
} from "@/components/ui/sidebar";

export const Route = createFileRoute("/_sidebar")({
  component: () => <Layout />,
});

function Layout() {
  const location = useLocation();
  const { t } = useTranslation();

  // The old "Create Rules" expandable group (Single-table rules,
  // Cross-table rules, Profile & Generate) and the standalone "Active
  // Rules" item were removed as part of the nav-consolidation cleanup
  // (Phase 5) — authoring and browsing now live in Rules Registry +
  // Monitored Tables. The underlying route files still exist (some as
  // redirects, some as still-reachable-by-URL pages) so old bookmarks
  // don't 404; see ``rules.single-table.tsx``, ``rules.create-sql.tsx``,
  // ``rules.active.tsx``, and ``discovery.tsx``. Bulk import (YAML /
  // data contract) lives under Rules Registry at ``/registry-rules/import``;
  // legacy ``/rules/import`` and ``/rules/from-contract`` redirect there.

  return (
    <SidebarLayout>
      <SidebarGroup className="pt-2">
        <SidebarGroupContent>
          <SidebarMenu>
            {/* Home — the dqlake-style landing page (P3.5): welcome, "At a
                Glance" stat cards with the cached overall DQ score, and the
                Get Started nav grid. Top of the nav, matching dqlake. */}
            <SidebarMenuItem>
              <Link
                to="/home"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg",
                  location.pathname.startsWith("/home")
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <Home size={16} />
                <span>{t("sidebar.home")}</span>
              </Link>
            </SidebarMenuItem>

            {/* Rules Registry — reusable, versioned, governed rule
                definitions (Phase 2). Import (YAML / data contract) is a
                sub-route reached from the registry list header. */}
            <SidebarMenuItem>
              <Link
                to="/registry-rules"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg",
                  location.pathname.startsWith("/registry-rules")
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <Library size={16} />
                <span>{t("sidebar.rulesRegistry")}</span>
              </Link>
            </SidebarMenuItem>

            {/* Monitored Tables — apply registry rules to real tables
                (slot->column mapping), profile them, and publish to
                materialize into dq_quality_rules (Phase 3D). Sits directly
                below Rules Registry per the design spec's nav layout. */}
            <SidebarMenuItem>
              <Link
                to="/monitored-tables"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg",
                  location.pathname.startsWith("/monitored-tables")
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <Table2 size={16} />
                <span>{t("sidebar.monitoredTables")}</span>
              </Link>
            </SidebarMenuItem>

            {/* Table Spaces — group monitored tables into governed,
                versioned, schedulable bundles (Phase 11; renamed from
                "Data Products" in P21 item 28). Visible to all roles, same
                as Monitored Tables — run actions inside the space itself are
                gated to RUNNER (admins implicit). Replaces the old
                runner-only "Run Rules" entry; ``/runs`` and the old
                ``/data-products`` path now redirect here (Phase-5 redirect
                pattern) so old bookmarks don't 404. */}
            <SidebarMenuItem>
              <Link
                to="/table-spaces"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg",
                  location.pathname.startsWith("/table-spaces") ||
                    location.pathname.startsWith("/data-products") ||
                    (location.pathname.startsWith("/runs") &&
                      !location.pathname.startsWith("/runs-history"))
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <Boxes size={16} />
                <span>{t("sidebar.dataProducts")}</span>
              </Link>
            </SidebarMenuItem>

            {/* Drafts & Review — approvals for registry rules AND
                per-table applications. */}
            <SidebarMenuItem>
              <Link
                to="/rules/drafts"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg",
                  location.pathname === "/rules/drafts"
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <ClipboardCheck size={16} />
                <span>{t("sidebar.draftsAndReview")}</span>
              </Link>
            </SidebarMenuItem>

            <hr className="my-2 border-sidebar-border" />

            {/* Runs History — visible to all */}
            <SidebarMenuItem>
              <Link
                to="/runs-history"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg",
                  location.pathname.startsWith("/runs-history")
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <History size={16} />
                <span>{t("sidebar.runsHistory")}</span>
              </Link>
            </SidebarMenuItem>

            {/* Results — org-wide DQ results composition over all monitored
                tables (dq-results endpoints). Visible to all; the backend
                filters to the viewer's accessible catalogs. Sits
                between Runs History and Insights: it's the outcome view of
                the runs above it, at a higher altitude than the per-run
                history. */}
            <SidebarMenuItem>
              <Link
                to="/results"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg",
                  location.pathname.startsWith("/results")
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <Gauge size={16} />
                <span>{t("sidebar.results")}</span>
              </Link>
            </SidebarMenuItem>

            {/* Insights — embedded Databricks AI/BI dashboard. Visible to all;
                the dashboard itself enforces UC permissions on its data so a
                viewer who can't read e.g. dq_quarantine_records just sees an
                empty tile rather than being blocked at the app layer. */}
            <SidebarMenuItem>
              <Link
                to="/insights"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg",
                  location.pathname.startsWith("/insights")
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <LayoutDashboard size={16} />
                <span>{t("sidebar.insights")}</span>
              </Link>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarGroupContent>
      </SidebarGroup>

      {/* Bottom-pinned external links group. Because the parent
          ``SidebarContent`` uses ``flex flex-col justify-between``, this
          second group gets pushed to the bottom of the sidebar with no
          extra flex plumbing here.

          Documentation lives on the public DQX docs site — using a real
          <a target="_blank"> rather than the TanStack router <Link>
          avoids a no-op route match and keeps the docs in their own tab
          so the user doesn't lose their place in the studio. */}
      <SidebarGroup className="pb-2">
        <SidebarGroupContent>
          <SidebarMenu>
            <SidebarMenuItem>
              <a
                href="https://databrickslabs.github.io/dqx/docs/guide/dqx_studio/#accessing-dqx-studio"
                target="_blank"
                rel="noopener noreferrer"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg text-sm",
                  "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
                title={t("sidebar.documentationTitle")}
              >
                <BookOpen size={16} />
                <span className="flex-1">{t("sidebar.documentation")}</span>
                <ExternalLink
                  size={12}
                  className="text-muted-foreground"
                  aria-hidden
                />
              </a>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarGroupContent>
      </SidebarGroup>
    </SidebarLayout>
  );
}
