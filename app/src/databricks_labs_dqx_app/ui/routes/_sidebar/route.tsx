import SidebarLayout from "@/components/layout/SidebarLayout";
import { createFileRoute, Link, useLocation } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import {
  ClipboardCheck,
  LineChart,
  History,
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
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { useGlobalResultsEnabled } from "@/hooks/use-global-results-enabled";
import { useApprovalsMode } from "@/hooks/use-approvals-mode";

export const Route = createFileRoute("/_sidebar")({
  component: () => <Layout />,
});

function Layout() {
  const location = useLocation();
  const { t } = useTranslation();
  // The global, all-tables Results surface is admin-gated and OFF by default
  // (B2-20). Hide the nav item entirely until an admin enables it; per-object
  // MT/TS/RR results tabs are unaffected.
  const globalResultsEnabled = useGlobalResultsEnabled();
  // When approvals are disabled app-wide there is no review queue, so the
  // Review & Approve nav item (and its trailing divider) are hidden (B2-142).
  const { mode: approvalsMode } = useApprovalsMode();
  const approvalsEnabled = approvalsMode !== "disabled";

  // The old "Create Rules" expandable group (Single-table rules,
  // Cross-table rules, Profile & Generate) and the standalone "Active
  // Rules" item were removed as part of the nav-consolidation cleanup
  // (Phase 5) — authoring and browsing now live in Rules Registry +
  // Monitored Tables. The underlying route files still exist (some as
  // redirects, some as still-reachable-by-URL pages) so old bookmarks
  // don't 404; see ``rules.single-table.tsx``, ``rules.create-sql.tsx``,
  // ``rules.active.tsx``, and ``discovery.tsx``. Import Rules
  // (``/rules/import``, plus the legacy ``/rules/from-contract``
  // redirect) lives directly in the sidebar, immediately above Drafts
  // & Review — it's a bulk-registry operation that feeds drafts
  // awaiting approval, not a per-table authoring shortcut, so it no
  // longer needs a Config-page detour.

  return (
    <SidebarLayout>
      <SidebarGroup className="pt-2">
        <SidebarGroupContent>
          <SidebarMenu>
            {/* Home is intentionally not a sidebar item — the DQX Studio
                logo in the top bar links to /home, so the landing page
                stays reachable without a dedicated nav entry (#72).

                Each item uses SidebarMenuButton so that in the collapsed
                (icon-only) sidebar it shrinks to its icon and surfaces its
                label as a hover tooltip; the previous hand-rolled active
                styling now maps onto the button's isActive prop (#28). */}

            {/* Rules Registry — reusable, versioned, governed rule
                definitions (Phase 2). */}
            <SidebarMenuItem>
              <SidebarMenuButton
                asChild
                isActive={location.pathname.startsWith("/registry-rules")}
                tooltip={t("sidebar.rulesRegistry")}
              >
                <Link to="/registry-rules">
                  <Library />
                  <span>{t("sidebar.rulesRegistry")}</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>

            {/* Monitored Tables — apply registry rules to real tables
                (slot->column mapping), profile them, and publish to
                materialize into dq_quality_rules (Phase 3D). */}
            <SidebarMenuItem>
              <SidebarMenuButton
                asChild
                isActive={location.pathname.startsWith("/monitored-tables")}
                tooltip={t("sidebar.monitoredTables")}
              >
                <Link to="/monitored-tables">
                  <Table2 />
                  <span>{t("sidebar.monitoredTables")}</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>

            {/* Table Spaces — group monitored tables into governed,
                versioned, schedulable bundles (Phase 11; renamed from
                "Data Products" in P21 item 28). ``/runs`` and the old
                ``/data-products`` path redirect here so old bookmarks don't
                404, so both are folded into the active-state check. */}
            <SidebarMenuItem>
              <SidebarMenuButton
                asChild
                isActive={
                  location.pathname.startsWith("/table-spaces") ||
                  location.pathname.startsWith("/data-products") ||
                  (location.pathname.startsWith("/runs") &&
                    !location.pathname.startsWith("/runs-history"))
                }
                tooltip={t("sidebar.dataProducts")}
              >
                <Link to="/table-spaces">
                  <Boxes />
                  <span>{t("sidebar.dataProducts")}</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>

            {/* Divider before the review group — separates the build/apply
                surfaces (Rules Registry, Monitored Tables, Table Spaces)
                from the approval queue below (#11). */}
            <hr className="my-2 border-sidebar-border" />

            {/* Review & Approve — approvals for registry rules AND
                per-table applications. Renamed from "Drafts & Review" (#11).
                Import Rules used to sit just above this item; it now lives in
                the username dropdown since it's a bulk-registry operation,
                not a daily nav destination (see HeaderUserMenu).
                Hidden — along with the divider that follows it — when
                approvals are disabled app-wide (no review queue, B2-142). */}
            {approvalsEnabled && (
              <>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === "/rules/drafts"}
                    tooltip={t("sidebar.reviewAndApprove")}
                  >
                    <Link to="/rules/drafts">
                      <ClipboardCheck />
                      <span>{t("sidebar.reviewAndApprove")}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>

                {/* Divider before the observability group (Runs History,
                    Results). */}
                <hr className="my-2 border-sidebar-border" />
              </>
            )}

            {/* Runs History — visible to all */}
            <SidebarMenuItem>
              <SidebarMenuButton
                asChild
                isActive={location.pathname.startsWith("/runs-history")}
                tooltip={t("sidebar.runsHistory")}
              >
                <Link to="/runs-history">
                  <History />
                  <span>{t("sidebar.runsHistory")}</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>

            {/* Results — org-wide DQ results composition over all monitored
                tables (dq-results endpoints). Visible to all; the backend
                filters to the viewer's accessible catalogs. Admin-gated and
                hidden by default (B2-20) — an admin opts in on the
                Configuration page. */}
            {globalResultsEnabled && (
              <SidebarMenuItem>
                <SidebarMenuButton
                  asChild
                  isActive={location.pathname.startsWith("/results")}
                  tooltip={t("sidebar.results")}
                >
                  <Link to="/results">
                    <LineChart />
                    <span>{t("sidebar.results")}</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
            )}
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
              <SidebarMenuButton asChild tooltip={t("sidebar.documentation")}>
                <a
                  href="https://databrickslabs.github.io/dqx/docs/guide/dqx_studio/#accessing-dqx-studio"
                  target="_blank"
                  rel="noopener noreferrer"
                  title={t("sidebar.documentationTitle")}
                >
                  <BookOpen />
                  <span className="flex-1">{t("sidebar.documentation")}</span>
                  <ExternalLink
                    className="text-muted-foreground"
                    aria-hidden
                  />
                </a>
              </SidebarMenuButton>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarGroupContent>
      </SidebarGroup>
    </SidebarLayout>
  );
}
