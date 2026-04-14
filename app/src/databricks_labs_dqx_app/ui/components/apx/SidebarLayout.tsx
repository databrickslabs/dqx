import { Outlet } from "@tanstack/react-router";
import type { ReactNode } from "react";
import {
  Sidebar,
  SidebarContent,
  SidebarInset,
  SidebarProvider,
  SidebarRail,
  SidebarTrigger,
} from "@/components/ui/sidebar";
import { ModeToggle } from "@/components/apx/mode-toggle";
import { AIAssistantTrigger } from "@/components/AIAssistantProvider";
import Logo from "@/components/apx/Logo";
import HeaderUserMenu from "@/components/apx/HeaderUserMenu";
import { useVersion } from "@/lib/api";

interface SidebarLayoutProps {
  children?: ReactNode;
}

function SidebarLayout({ children }: SidebarLayoutProps) {
  const { data: resp } = useVersion();
  const appVersion = resp?.data?.version;

  return (
    <div className="flex flex-col h-screen">
      {/* Fixed top banner — independent of sidebar, never shifts */}
      <header className="sticky top-0 z-50 bg-background border-b shrink-0">
        <div className="flex h-12 items-center justify-between px-4">
          <div className="flex items-center gap-3">
            <Logo />
          </div>
          <div className="flex items-center gap-1">
            <AIAssistantTrigger />
            <ModeToggle />
            <HeaderUserMenu />
          </div>
        </div>
      </header>

      {/* Sidebar + main content below the banner */}
      <SidebarProvider>
        <Sidebar className="top-12 h-[calc(100vh-3rem)]">
          <SidebarContent className="flex flex-col justify-between">
            {children}
          </SidebarContent>
          <SidebarRail />
        </Sidebar>
        <SidebarInset className="flex flex-col min-h-0">
          <div className="flex items-center h-10 px-4 shrink-0">
            <SidebarTrigger className="-ml-1 cursor-pointer" />
          </div>
          <div className="flex-1 overflow-auto">
            <div className="flex flex-col gap-4 p-6 pt-0 max-w-7xl mx-auto">
              <Outlet />
            </div>
          </div>
          <footer className="shrink-0 border-t bg-background py-2 px-6">
            <div className="max-w-7xl mx-auto flex items-center justify-between text-[11px] text-muted-foreground">
              <span>
                DQX — Data Quality Explorer
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
