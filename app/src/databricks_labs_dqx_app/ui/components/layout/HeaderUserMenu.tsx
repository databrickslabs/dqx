import { Suspense, useMemo } from "react";
import { Link } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import {
  useCurrentUserSuspense,
  useCurrentUserRoleSuspense,
} from "@/hooks/use-suspense-queries";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Skeleton } from "@/components/ui/skeleton";
import selector from "@/lib/selector";
import { type User, type UserRoleOut, useGetVersion } from "@/lib/api";
import {
  Settings,
  User as UserIcon,
  ChevronDown,
  Languages,
  ExternalLink,
} from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuPortal,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  SUPPORTED_LANGUAGES,
  ensureLocaleLoaded,
  toSupportedCode,
} from "@/lib/i18n";

const GITHUB_URL = "https://github.com/databrickslabs/dqx";

/** Maps backend role identifiers to their i18n label keys. */
const ROLE_LABEL_KEYS: Record<string, string> = {
  admin: "roleManagement.roleAdmin",
  rule_approver: "roleManagement.roleApprover",
  rule_author: "roleManagement.roleAuthor",
  viewer: "roleManagement.roleViewer",
};

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
  // B2-22: the app version is fixed for the life of the deployment — pin to
  // staleTime: Infinity so it's fetched once and served from cache instead of
  // refetching every time the header menu remounts.
  const { data: verResp } = useGetVersion({ query: { staleTime: Infinity } });
  const { t, i18n } = useTranslation();

  const appVersion = verResp?.data?.version;
  const role = roleResp?.role;
  const isAdmin = role === "admin";
  const currentLanguage = toSupportedCode(i18n.resolvedLanguage ?? "en");

  // B2-65: present every registered locale in alphabetical order by the label
  // shown to the user (its native name), so the picker reads Deutsch / English /
  // Español / … regardless of registration order in SUPPORTED_LANGUAGES.
  const sortedLanguages = useMemo(
    () =>
      [...SUPPORTED_LANGUAGES].sort((a, b) => a.nativeLabel.localeCompare(b.nativeLabel)),
    [],
  );

  const initials = useMemo(() => {
    const name = user.display_name ?? user.user_name ?? "";
    const parts = name.split(/[\s.@]+/);
    return (parts[0]?.[0] ?? "").toUpperCase() + (parts[1]?.[0] ?? "").toUpperCase();
  }, [user.display_name, user.user_name]);

  const handleLanguageChange = (value: string) => {
    ensureLocaleLoaded(value)
      .then(() => i18n.changeLanguage(value))
      .catch((err) => {
        console.error("Failed to switch language", err);
        toast.error(t("language.switchFailed"));
      });
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          className="flex items-center gap-2 rounded-md px-2 py-1.5 text-sm hover:bg-accent transition-colors outline-none data-[state=open]:bg-accent"
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
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-60">
        <DropdownMenuLabel className="font-normal">
          <p className="text-sm font-medium truncate">{user.display_name}</p>
          <p className="text-xs text-muted-foreground truncate">
            {user.user_name}
          </p>
          {role && (
            <p className="text-xs text-muted-foreground/70 mt-0.5 truncate">
              {t(ROLE_LABEL_KEYS[role] ?? "roleManagement.roleViewer")}
            </p>
          )}
        </DropdownMenuLabel>
        <DropdownMenuSeparator />
        <DropdownMenuGroup>
          <DropdownMenuItem asChild>
            <Link to="/profile">
              <UserIcon />
              {t("userMenu.profile")}
            </Link>
          </DropdownMenuItem>
          <DropdownMenuSub>
            <DropdownMenuSubTrigger>
              <Languages className="h-4 w-4" />
              <span className="ml-2">{t("language.label")}</span>
            </DropdownMenuSubTrigger>
            <DropdownMenuPortal>
              <DropdownMenuSubContent>
                <DropdownMenuRadioGroup
                  value={currentLanguage}
                  onValueChange={handleLanguageChange}
                >
                  {sortedLanguages.map((lang) => (
                    <DropdownMenuRadioItem key={lang.code} value={lang.code}>
                      <span className="font-medium">{lang.nativeLabel}</span>
                      {lang.nativeLabel !== lang.label && (
                        <span className="text-muted-foreground ml-2 text-xs">
                          ({lang.label})
                        </span>
                      )}
                    </DropdownMenuRadioItem>
                  ))}
                </DropdownMenuRadioGroup>
              </DropdownMenuSubContent>
            </DropdownMenuPortal>
          </DropdownMenuSub>
        </DropdownMenuGroup>
        {isAdmin && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuItem asChild>
              <Link to="/settings">
                <Settings />
                {t("userMenu.adminSettings")}
              </Link>
            </DropdownMenuItem>
          </>
        )}
        <DropdownMenuSeparator />
        <div className="px-2 py-1.5 text-[11px] text-muted-foreground">
          <a
            href={GITHUB_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 hover:text-foreground transition-colors"
          >
            github.com/databrickslabs/dqx
            <ExternalLink className="h-3 w-3" aria-hidden />
          </a>
          {appVersion && <p className="font-mono font-light">v{appVersion}</p>}
        </div>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

export default function HeaderUserMenu() {
  return (
    <Suspense fallback={<HeaderUserMenuSkeleton />}>
      <HeaderUserMenuContent />
    </Suspense>
  );
}
