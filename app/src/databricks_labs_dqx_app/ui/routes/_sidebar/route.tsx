import SidebarLayout from "@/components/layout/SidebarLayout";
import { createFileRoute, Link, useLocation } from "@tanstack/react-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { cn } from "@/lib/utils";
import { usePermissions } from "@/hooks/use-permissions";
import {
  Sparkles,
  Database,
  Upload,
  BarChart3,
  PlayCircle,
  ShieldCheck,
  ClipboardCheck,
  ChevronDown,
  PenLine,
  History,
  Bell,
} from "lucide-react";
import {
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubItem,
  SidebarMenuSubButton,
} from "@/components/ui/sidebar";

export const Route = createFileRoute("/_sidebar")({
  component: () => <Layout />,
});

function Layout() {
  const location = useLocation();
  const { canCreateRules, canRunRules } = usePermissions();
  const { t } = useTranslation();

  const isCreateActive =
    location.pathname.startsWith("/rules/create") ||
    location.pathname.startsWith("/rules/single-table") ||
    location.pathname.startsWith("/rules/import") ||
    location.pathname.startsWith("/profiler");

  const [createOpen, setCreateOpen] = useState(isCreateActive);

  const createChildren = [
    {
      to: "/rules/single-table",
      label: t("sidebar.singleTableRules"),
      icon: <Sparkles size={14} />,
      match: (path: string) =>
        path.startsWith("/rules/single-table") || path.startsWith("/rules/create"),
    },
    {
      to: "/rules/create-sql",
      label: t("sidebar.crossTableRules"),
      icon: <Database size={14} />,
      match: (path: string) => path.startsWith("/rules/create-sql"),
    },
    {
      to: "/profiler",
      label: t("sidebar.profileAndGenerate"),
      icon: <BarChart3 size={14} />,
      match: (path: string) => path.startsWith("/profiler"),
    },
    {
      to: "/rules/import",
      label: t("sidebar.importRules"),
      icon: <Upload size={14} />,
      match: (path: string) => path.startsWith("/rules/import"),
    },
  ];

  return (
    <SidebarLayout>
      <SidebarGroup className="pt-2">
        <SidebarGroupContent>
          <SidebarMenu>
            {/* Create Rules — expandable (hidden for viewers) */}
            {canCreateRules && (
            <SidebarMenuItem>
              <button
                type="button"
                onClick={() => setCreateOpen((prev) => !prev)}
                className={cn(
                  "flex w-full items-center gap-2 p-2 rounded-lg text-sm font-medium transition-colors",
                  isCreateActive
                    ? "text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <PenLine size={16} />
                <span className="flex-1 text-left">{t("sidebar.createRules")}</span>
                <ChevronDown
                  size={14}
                  className={cn(
                    "text-muted-foreground transition-transform duration-200",
                    createOpen && "rotate-180",
                  )}
                />
              </button>
              {createOpen && (
                <SidebarMenuSub>
                  {createChildren.map((child) => (
                    <SidebarMenuSubItem key={child.to}>
                      <SidebarMenuSubButton
                        asChild
                        isActive={child.match(location.pathname)}
                      >
                        <Link to={child.to} className="flex items-center gap-2">
                          {child.icon}
                          <span>{child.label}</span>
                        </Link>
                      </SidebarMenuSubButton>
                    </SidebarMenuSubItem>
                  ))}
                </SidebarMenuSub>
              )}
            </SidebarMenuItem>
            )}

            {/* Drafts & Review */}
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

            {/* Active Rules */}
            <SidebarMenuItem>
              <Link
                to="/rules/active"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg",
                  location.pathname === "/rules/active" ||
                    location.pathname === "/rules"
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <ShieldCheck size={16} />
                <span>{t("sidebar.activeRules")}</span>
              </Link>
            </SidebarMenuItem>

            <hr className="my-2 border-sidebar-border" />

            {/* Run Rules — only visible to users with the RUNNER role
                (admins are implicit runners). Authors/approvers without
                an explicit RUNNER mapping cannot see this entry. */}
            {canRunRules && (
            <SidebarMenuItem>
              <Link
                to="/runs"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg",
                  location.pathname.startsWith("/runs")
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <PlayCircle size={16} />
                <span>{t("sidebar.runRules")}</span>
              </Link>
            </SidebarMenuItem>
            )}

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

            <hr className="my-2 border-sidebar-border" />

            {/* Configure Alerts — admin only */}
            <SidebarMenuItem>
              <Link
                to="/alerts"
                className={cn(
                  "flex items-center gap-2 p-2 rounded-lg",
                  location.pathname.startsWith("/alerts")
                    ? "bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                )}
              >
                <Bell size={16} />
                <span>{t("sidebar.configureAlerts")}</span>
              </Link>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarGroupContent>
      </SidebarGroup>
    </SidebarLayout>
  );
}
