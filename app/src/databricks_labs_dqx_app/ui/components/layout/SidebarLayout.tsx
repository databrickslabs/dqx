import { Outlet, useLocation } from "@tanstack/react-router";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  Sidebar,
  SidebarContent,
  SidebarInset,
  SidebarProvider,
  SidebarRail,
  SidebarTrigger,
} from "@/components/ui/sidebar";
import { ModeToggle } from "@/components/layout/mode-toggle";
import Logo from "@/components/layout/Logo";
import HeaderUserMenu from "@/components/layout/HeaderUserMenu";
import { InsightsDashboardHost } from "@/components/insights/InsightsDashboard";
import { cn } from "@/lib/utils";
import { useGetVersion } from "@/lib/api";

interface SidebarLayoutProps {
  children?: ReactNode;
}

function SidebarLayout({ children }: SidebarLayoutProps) {
  const { t } = useTranslation();
  const { data: resp } = useGetVersion();
  const appVersion = resp?.data?.version;
  // The Insights dashboard iframe is hosted persistently (see
  // ``InsightsDashboardHost``) so it doesn't reload on every navigation.
  // While Insights is active we hide the empty route ``<Outlet/>`` wrapper
  // and show the persistent host instead.
  const location = useLocation();
  const onInsights = location.pathname.startsWith("/insights");

  return (
    <div className="flex flex-col h-screen overflow-x-hidden">
      {/* Fixed top banner — independent of sidebar, never shifts */}
      <header className="sticky top-0 z-50 bg-background border-b shrink-0">
        <div className="flex h-12 items-center justify-between px-4">
          <div className="flex items-center gap-3">
            <Logo />
          </div>
          <div className="flex items-center gap-1">
            <ModeToggle />
            <HeaderUserMenu />
          </div>
        </div>
      </header>

      {/* Sidebar + main content below the banner. `min-h-0` overrides the
          shadcn default `min-h-svh` on SidebarProvider — that default
          assumes SidebarProvider is the full-viewport root, but here it
          sits below our fixed header, so `min-h-svh` was forcing the total
          layout past 100vh and causing a second (body-level) scrollbar in
          addition to the intended inner one. */}
      <SidebarProvider className="flex-1 min-h-0">
        <Sidebar className="top-12 h-[calc(100vh-3rem)]">
          <SidebarContent className="flex flex-col justify-between">
            {children}
          </SidebarContent>
          <SidebarRail />
        </Sidebar>
        <SidebarInset className="flex flex-col min-h-0 min-w-0">
          <div className="flex items-center h-10 px-4 shrink-0">
            <SidebarTrigger className="-ml-1 cursor-pointer" />
          </div>
          {/* `scrollbar-gutter: stable` keeps the `max-w-7xl mx-auto` page
              content in the SAME horizontal position whether or not the
              vertical scrollbar is present. Without it, with classic
              (non-overlay) scrollbars a tall tab (e.g. a Monitored Table's
              About tab listing every column) summons the scrollbar, shrinks
              this scroller, and `mx-auto` re-centers the content a few px
              LEFT of its position on shorter sibling tabs (P23 item 15). */}
          <div className="flex-1 min-h-0 min-w-0 overflow-y-auto overflow-x-hidden [scrollbar-gutter:stable]">
            <div className={cn("flex flex-col gap-4 p-6 pt-0 max-w-7xl mx-auto min-w-0", onInsights && "hidden")}>
              <Outlet />
            </div>
            <InsightsDashboardHost />
          </div>
          <footer className="shrink-0 border-t bg-background py-2 px-6">
            <div className="max-w-7xl mx-auto flex items-center justify-between text-[11px] text-muted-foreground">
              <span>
                {t("footer.tagline")}
                {appVersion ? ` · v${appVersion}` : ""}
              </span>
              <a
                href="https://github.com/databrickslabs/dqx"
                target="_blank"
                rel="noopener noreferrer"
                className="hover:text-foreground transition-colors"
              >
                github.com/databrickslabs/dqx
              </a>
            </div>
          </footer>
        </SidebarInset>
      </SidebarProvider>
    </div>
  );
}
export default SidebarLayout;
