import { Suspense, useMemo } from "react";
import { SidebarMenuButton } from "@/components/ui/sidebar";
import { useProfileSuspense } from "@/lib/api";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Skeleton } from "@/components/ui/skeleton";
import FadeIn from "@/components/FadeIn";
import selector from "@/lib/selector";

function ProfileSkeleton() {
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

function ProfileContent() {
  const { data: profile } = useProfileSuspense(selector());

  const firstLetters = useMemo(() => {
    const userName = profile.user.displayName ?? "";
    const [first = "", ...rest] = userName.split(" ");
    const last = rest.at(-1) ?? "";
    return `${first[0] ?? ""}${last[0] ?? ""}`.toUpperCase();
  }, [profile.user.displayName]);

  return (
    <FadeIn>
      <SidebarMenuButton
        size="lg"
        className="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
      >
        <Avatar className="h-8 w-8 rounded-lg grayscale">
          <AvatarFallback className="rounded-lg">{firstLetters}</AvatarFallback>
        </Avatar>
        <div className="grid flex-1 text-left text-sm leading-tight">
          <span className="truncate font-medium">
            {profile.user.displayName}
          </span>
          <span className="text-muted-foreground truncate text-xs">
            {profile.user.userName}
          </span>
        </div>
      </SidebarMenuButton>
    </FadeIn>
  );
}

export default function Profile() {
  return (
    <Suspense fallback={<ProfileSkeleton />}>
      <ProfileContent />
    </Suspense>
  );
}
