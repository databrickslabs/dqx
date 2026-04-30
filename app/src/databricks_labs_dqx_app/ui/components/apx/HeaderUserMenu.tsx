import { Suspense, useMemo, useState } from "react";
import { Link, useLocation } from "@tanstack/react-router";
import {
  useCurrentUserSuspense,
  useCurrentUserRoleSuspense,
} from "@/hooks/use-suspense-queries";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import selector from "@/lib/selector";
import { type User, type UserRoleOut } from "@/lib/api";
import { Settings, User as UserIcon, ChevronDown } from "lucide-react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Separator } from "@/components/ui/separator";

function HeaderUserMenuSkeleton() {
  return (
    <div className="flex items-center gap-2">
      <Skeleton className="h-7 w-7 rounded-full" />
      <Skeleton className="h-4 w-20 rounded" />
    </div>
  );
}

function HeaderUserMenuContent() {
  const { data: user } = useCurrentUserSuspense(selector<User>());
  const { data: roleResp } = useCurrentUserRoleSuspense(selector<UserRoleOut>());
  const location = useLocation();
  const [open, setOpen] = useState(false);

  const role = roleResp?.role;
  const isAdmin = role === "admin";

  const initials = useMemo(() => {
    const name = user.display_name ?? user.user_name ?? "";
    const parts = name.split(/[\s.@]+/);
    return (parts[0]?.[0] ?? "").toUpperCase() + (parts[1]?.[0] ?? "").toUpperCase();
  }, [user.display_name, user.user_name]);

  const isConfigActive = location.pathname === "/config";
  const isProfileActive = location.pathname === "/profile";

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          type="button"
          className="flex items-center gap-2 rounded-md px-2 py-1.5 text-sm hover:bg-accent transition-colors outline-none"
        >
          <Avatar className="h-7 w-7 rounded-full">
            <AvatarFallback className="rounded-full text-[11px] font-medium">
              {initials}
            </AvatarFallback>
          </Avatar>
          <span className="hidden sm:inline truncate max-w-[120px] text-sm font-medium">
            {user.display_name ?? user.user_name}
          </span>
          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
        </button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-56 p-0">
        <div className="px-3 py-2.5">
          <p className="text-sm font-medium truncate">{user.display_name}</p>
          <p className="text-xs text-muted-foreground truncate">{user.user_name}</p>
        </div>
        <Separator />
        <div className="py-1">
          <Link
            to="/profile"
            onClick={() => setOpen(false)}
            className={cn(
              "flex items-center gap-2 px-3 py-2 text-sm hover:bg-accent transition-colors",
              isProfileActive && "bg-accent",
            )}
          >
            <UserIcon className="h-4 w-4" />
            Profile
          </Link>
          {isAdmin && (
            <Link
              to="/config"
              onClick={() => setOpen(false)}
              className={cn(
                "flex items-center gap-2 px-3 py-2 text-sm hover:bg-accent transition-colors",
                isConfigActive && "bg-accent",
              )}
            >
              <Settings className="h-4 w-4" />
              Configuration
            </Link>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}

export default function HeaderUserMenu() {
  return (
    <Suspense fallback={<HeaderUserMenuSkeleton />}>
      <HeaderUserMenuContent />
    </Suspense>
  );
}
