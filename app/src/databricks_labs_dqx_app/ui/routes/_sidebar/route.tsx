import SidebarLayout from "@/components/apx/SidebarLayout";
import { createFileRoute, Link, useLocation } from "@tanstack/react-router";
import { useState } from "react";
import { cn } from "@/lib/utils";
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

  const isCreateActive =
    location.pathname.startsWith("/rules/create") ||
    location.pathname.startsWith("/rules/generate") ||
    location.pathname.startsWith("/rules/import") ||
    location.pathname.startsWith("/profiler");

  const [createOpen, setCreateOpen] = useState(isCreateActive);

  const createChildren = [
    {
      to: "/rules/generate",
      label: "Single table rules",
      icon: <Sparkles size={14} />,
      match: (path: string) =>
        path.startsWith("/rules/generate") || path.startsWith("/rules/create"),
    },
    {
      to: "/rules/create-sql",
      label: "Cross-table rules",
      icon: <Database size={14} />,
      match: (path: string) => path.startsWith("/rules/create-sql"),
    },
    {
      to: "/profiler",
      label: "Profile & generate",
      icon: <BarChart3 size={14} />,
      match: (path: string) => path.startsWith("/profiler"),
    },
    {
      to: "/rules/import",
      label: "Import rules",
      icon: <Upload size={14} />,
      match: (path: string) => path.startsWith("/rules/import"),
    },
  ];

  return (
    <SidebarLayout>
      <SidebarGroup className="pt-2">
        <SidebarGroupContent>
          <SidebarMenu>
            {/* Create Rules — expandable */}
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
                <span className="flex-1 text-left">Create Rules</span>
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
                <span>Drafts & Review</span>
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
                <span>Active Rules</span>
              </Link>
            </SidebarMenuItem>

            {/* Run Rules */}
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
                <span>Run Rules</span>
              </Link>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarGroupContent>
      </SidebarGroup>
    </SidebarLayout>
  );
}
