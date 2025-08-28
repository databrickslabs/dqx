import { Link, Outlet, useLocation } from "@tanstack/react-router";
import { cn } from "@/lib/utils";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuItem,
  SidebarProvider,
  SidebarRail,
  SidebarTrigger,
} from "@/components/ui/sidebar";
import Logo from "@/assets/Logo";
import Profile from "@/components/Profile";
import { ModeToggle } from "@/components/theme/mode-toggle";

interface NavItem {
  to: string;
  label: string;
  icon: React.ReactNode;
  match: (path: string) => boolean;
}

function Layout() {
  const location = useLocation();

  const navItems: NavItem[] = [];

  return (
    <SidebarProvider>
      <Sidebar>
        <SidebarHeader>
          <Link to="/" className="flex items-center gap-2 px-4 py-2">
            <Logo className="h-8 w-8" />
            <h2 className="text-lg font-semibold">
              <code>dqx</code>
            </h2>
          </Link>
        </SidebarHeader>
        <SidebarContent>
          <SidebarGroup>
            <SidebarGroupContent>
              <SidebarMenu>
                {navItems.map((item) => (
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
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
        <SidebarFooter>
          <Profile />
        </SidebarFooter>
        <SidebarRail />
      </Sidebar>
      <SidebarInset className="flex flex-col">
        <header className="flex h-16 shrink-0 items-center gap-2 border-b px-4">
          <SidebarTrigger className="-ml-1 cursor-pointer" />
          <div className="flex-1" />
          <ModeToggle />
        </header>
        <div className="flex flex-1 justify-center">
          <div className="flex flex-1 flex-col gap-4 p-6 max-w-7xl">
            <Outlet />
          </div>
        </div>
      </SidebarInset>
    </SidebarProvider>
  );
}
export default Layout;
