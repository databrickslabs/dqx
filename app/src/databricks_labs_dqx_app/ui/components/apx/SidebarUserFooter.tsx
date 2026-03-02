import { Suspense, useMemo } from "react";
import { Link, useLocation } from "@tanstack/react-router";
import { SidebarMenuButton } from "@/components/ui/sidebar";
import { useCurrentUserSuspense } from "@/lib/api";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import selector from "@/lib/selector";

function SidebarUserFooterSkeleton() {
  return (
    <SidebarMenuButton size="lg">
      <Skeleton className="h-8 w-8 rounded-lg" />
      <div className="grid flex-1 text-left text-sm leading-tight gap-1">
        <Skeleton className="h-4 w-24 rounded" />
        <Skeleton className="h-3 w-46 rounded" />
      </div>
    </SidebarMenuButton>
  );
}

function SidebarUserFooterContent() {
  const { data: user } = useCurrentUserSuspense(selector());
  const location = useLocation();

  const firstLetters = useMemo(() => {
    const userName = user.user_name ?? "";
    const [first = "", ...rest] = userName.split(" ");
    const last = rest.at(-1) ?? "";
    return `${first[0] ?? ""}${last[0] ?? ""}`.toUpperCase();
  }, [user.user_name]);

  const isProfileActive = location.pathname === "/profile";

  return (
    <SidebarMenuButton
      asChild
      size="lg"
      isActive={isProfileActive}
      className={cn(
        "data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground",
        isProfileActive && "bg-sidebar-accent text-sidebar-accent-foreground",
      )}
    >
      <Link to="/profile">
        <Avatar className="h-8 w-8 rounded-lg grayscale">
          <AvatarFallback className="rounded-lg">{firstLetters}</AvatarFallback>
        </Avatar>
        <div className="grid flex-1 text-left text-sm leading-tight">
          <span className="truncate font-medium">{user.display_name}</span>
          <span className="text-muted-foreground truncate text-xs">
            {user.user_name}
          </span>
        </div>
      </Link>
    </SidebarMenuButton>
  );
}

export default function SidebarUserFooter() {
  return (
    <Suspense fallback={<SidebarUserFooterSkeleton />}>
      <SidebarUserFooterContent />
    </Suspense>
  );
}
