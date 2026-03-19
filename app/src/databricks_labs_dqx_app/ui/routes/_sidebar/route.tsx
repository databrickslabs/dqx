import * as React from "react";
import SidebarLayout from "@/components/apx/SidebarLayout";
import { createFileRoute, Link, useLocation } from "@tanstack/react-router";
import { cn } from "@/lib/utils";
import { FileCode, Settings, Search, ClipboardCheck } from "lucide-react";
import {
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuItem,
} from "@/components/ui/sidebar";

export const Route = createFileRoute("/_sidebar")({
  component: () => <Layout />,
});

function Layout() {
  const location = useLocation();

  const configNavItems = [
    {
      to: "/config",
      label: "Configuration",
      icon: <Settings size={16} />,
      match: (path: string) => path === "/config",
    },
  ];

  const profilerNavItems = [
    {
      to: "/profiler",
      label: "Profiler",
      icon: <Search size={16} />,
      match: (path: string) => path.startsWith("/profiler"),
    },
    {
      to: "/rules",
      label: "Quality Rules",
      icon: <ClipboardCheck size={16} />,
      match: (path: string) => path.startsWith("/rules"),
    },
  ];

  const legacyNavItems = [
    {
      to: "/runs",
      label: "Runs (Legacy)",
      icon: <FileCode size={16} />,
      match: (path: string) => path.startsWith("/runs"),
    },
  ];

  const renderNavItems = (items: Array<{ to: string; label: string; icon: React.ReactNode; match: (path: string) => boolean }>) => (
    <SidebarMenu>
      {items.map((item) => (
        <SidebarMenuItem key={item.to}>
          <Link
            to={item.to}
            className={cn(
              "flex items-center gap-2 p-2 rounded-lg",
              item.match(location.pathname)
                ? "bg-sidebar-accent text-sidebar-accent-foreground"
                : "text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
            )}
          >
            {item.icon}
            <span>{item.label}</span>
          </Link>
        </SidebarMenuItem>
      ))}
    </SidebarMenu>
  );

  return (
    <SidebarLayout>
      <SidebarGroup>
        <SidebarGroupLabel className="text-xs text-muted-foreground">Setup</SidebarGroupLabel>
        <SidebarGroupContent>
          {renderNavItems(configNavItems)}
        </SidebarGroupContent>
      </SidebarGroup>

      <SidebarGroup>
        <SidebarGroupLabel className="text-xs text-muted-foreground">Data Quality</SidebarGroupLabel>
        <SidebarGroupContent>
          {renderNavItems(profilerNavItems)}
        </SidebarGroupContent>
      </SidebarGroup>

      <SidebarGroup>
        <SidebarGroupLabel className="text-xs text-muted-foreground">Legacy</SidebarGroupLabel>
        <SidebarGroupContent>
          {renderNavItems(legacyNavItems)}
        </SidebarGroupContent>
      </SidebarGroup>
    </SidebarLayout>
  );
}
