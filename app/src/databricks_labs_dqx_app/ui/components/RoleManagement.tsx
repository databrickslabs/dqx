import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Users,
  Plus,
  Trash2,
  Shield,
  AlertCircle,
  ChevronsUpDown,
  Check,
  Search,
  Loader2,
  Info,
} from "lucide-react";
import { isAxiosError } from "axios";
import { toast } from "sonner";
import {
  listRoleMappings,
  createRoleMapping,
  deleteRoleMapping,
  listWorkspaceGroups,
  listAvailableRoles,
  getListRoleMappingsQueryKey,
} from "@/lib/api";
import { cn } from "@/lib/utils";

/**
 * Pull the server-supplied ``detail`` off a FastAPI error if we can,
 * otherwise fall back to the raw error message. The role-mapping create
 * endpoint returns 400 on ``ValueError`` (bad role name) and 500 on
 * SQL/permission failures, both with a ``detail`` string the user
 * actually needs to see — the previous "Failed to create mapping.
 * Please try again." banner was hiding all of it.
 */
function extractRoleMappingError(err: unknown, fallback: string): string {
  if (isAxiosError(err)) {
    const detail = (err.response?.data as { detail?: unknown } | undefined)?.detail;
    if (typeof detail === "string" && detail.trim()) return detail;
    if (err.message) return err.message;
  }
  if (err instanceof Error && err.message) return err.message;
  return fallback;
}

const GROUP_SEARCH_DEBOUNCE_MS = 250;
// Server-side cap matches the FastAPI route's ``limit`` default. Going
// higher does little for UX (nobody scrolls 200+ items in a popover) and
// keeps SCIM responses snappy on huge workspaces.
const GROUP_SEARCH_LIMIT = 200;

/**
 * Searchable group picker backed by the server-side
 * ``GET /api/v1/roles/groups?search=&limit=`` endpoint.
 *
 * The previous implementation eagerly fetched every workspace group and
 * rendered them in a Radix Select. On workspaces with thousands of groups
 * (each carrying its full member roster in the SCIM payload) this would
 * stall at "Loading…" for many seconds — sometimes indefinitely. We now
 * push the matching to SCIM via ``filter=displayName co "..."`` and only
 * render the top ``GROUP_SEARCH_LIMIT`` matches, refetched as the user
 * types (debounced).
 */
function GroupCombobox({
  value,
  onChange,
}: {
  value: string;
  onChange: (groupName: string) => void;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [searchInput, setSearchInput] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const inputRef = useRef<HTMLInputElement | null>(null);

  // Debounce so we don't fire one SCIM call per keystroke.
  useEffect(() => {
    const t = setTimeout(
      () => setDebouncedSearch(searchInput.trim()),
      GROUP_SEARCH_DEBOUNCE_MS,
    );
    return () => clearTimeout(t);
  }, [searchInput]);

  const {
    data: groupsData,
    isLoading,
    isFetching,
    error,
  } = useQuery({
    queryKey: ["workspaceGroups", debouncedSearch],
    queryFn: () =>
      listWorkspaceGroups({
        search: debouncedSearch || undefined,
        limit: GROUP_SEARCH_LIMIT,
      }),
    // Same group list rarely changes mid-session; cache it for a minute
    // to avoid refetching when the user reopens the popover.
    staleTime: 60_000,
  });

  const groups = groupsData?.data || [];
  const reachedLimit = groups.length >= GROUP_SEARCH_LIMIT;

  const handleSelect = (groupName: string) => {
    onChange(groupName);
    setOpen(false);
    setSearchInput("");
  };

  return (
    <Popover
      open={open}
      onOpenChange={(next) => {
        setOpen(next);
        if (next) {
          // Defer focus so Radix's portal mount completes first.
          requestAnimationFrame(() => inputRef.current?.focus());
        } else {
          setSearchInput("");
        }
      }}
    >
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between font-normal"
        >
          <span className={cn(!value && "text-muted-foreground")}>
            {value || t("roleManagement.selectGroup")}
          </span>
          <ChevronsUpDown className="h-4 w-4 opacity-50 shrink-0" />
        </Button>
      </PopoverTrigger>
      <PopoverContent
        className="p-0 w-[--radix-popover-trigger-width] min-w-[280px]"
        align="start"
      >
        <div className="flex items-center gap-2 border-b px-3 py-2">
          <Search className="h-4 w-4 text-muted-foreground shrink-0" />
          <Input
            ref={inputRef}
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder={t("roleManagement.groupSearchPlaceholder")}
            className="border-0 shadow-none focus-visible:ring-0 px-0 h-8"
          />
          {isFetching && !isLoading ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground shrink-0" />
          ) : null}
        </div>
        <div className="max-h-64 overflow-y-auto py-1">
          {isLoading ? (
            <div className="px-3 py-6 text-center text-sm text-muted-foreground">
              {t("roleManagement.loading")}
            </div>
          ) : error ? (
            <div className="px-3 py-6 text-center text-sm text-destructive">
              {t("roleManagement.failedLoadGroups")}
            </div>
          ) : groups.length === 0 ? (
            <div className="px-3 py-6 text-center text-sm text-muted-foreground">
              {debouncedSearch
                ? t("roleManagement.noGroupsMatch")
                : t("roleManagement.noGroupsFound")}
            </div>
          ) : (
            <>
              {groups.map((group) => {
                const name = group.display_name;
                const selected = name === value;
                return (
                  <button
                    key={`${group.id ?? name}`}
                    type="button"
                    onClick={() => handleSelect(name)}
                    className={cn(
                      "w-full flex items-center gap-2 px-3 py-1.5 text-sm text-left",
                      "hover:bg-accent hover:text-accent-foreground",
                      "focus:bg-accent focus:text-accent-foreground focus:outline-none",
                    )}
                  >
                    <Check
                      className={cn(
                        "h-4 w-4 shrink-0",
                        selected ? "opacity-100" : "opacity-0",
                      )}
                    />
                    <span className="truncate">{name}</span>
                  </button>
                );
              })}
              {reachedLimit ? (
                <div className="px-3 py-2 text-xs text-muted-foreground border-t mt-1">
                  {t("roleManagement.showingFirst", { count: GROUP_SEARCH_LIMIT })}
                </div>
              ) : null}
            </>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}

function getRoleLabel(role: string, t: (key: string) => string): string {
  switch (role) {
    case "admin": return t("roleManagement.roleAdmin");
    case "rule_approver": return t("roleManagement.roleApprover");
    case "rule_author": return t("roleManagement.roleAuthor");
    case "viewer": return t("roleManagement.roleViewer");
    case "runner": return t("roleManagement.roleRunner");
    default: return role;
  }
}

function getRoleDescription(role: string, t: (key: string) => string): string {
  switch (role) {
    case "admin": return t("roleManagement.roleAdminDescription");
    case "rule_approver": return t("roleManagement.roleApproverDescription");
    case "rule_author": return t("roleManagement.roleAuthorDescription");
    case "viewer": return t("roleManagement.roleViewerDescription");
    case "runner": return t("roleManagement.roleRunnerDescription");
    default: return "";
  }
}

function RoleMappingRow({
  mapping,
  onDelete,
  isDeleting,
}: {
  mapping: { role: string; group_name: string };
  onDelete: () => void;
  isDeleting: boolean;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex items-center justify-between py-2 px-3 bg-muted/30 rounded-md">
      <div className="flex items-center gap-3">
        <Badge variant="outline" className="font-mono">
          {getRoleLabel(mapping.role, t)}
        </Badge>
        <span className="text-sm text-muted-foreground">→</span>
        <span className="font-medium">{mapping.group_name}</span>
      </div>
      <Button
        variant="ghost"
        size="sm"
        onClick={onDelete}
        disabled={isDeleting}
        className="text-destructive hover:text-destructive hover:bg-destructive/10"
      >
        <Trash2 className="h-4 w-4" />
      </Button>
    </div>
  );
}

/**
 * Form state is owned by the parent so that:
 *
 *   1. The values aren't blown away on click (the mutation is fired
 *      synchronously but resolves async — clearing on click means a
 *      slow request "vanishes" the user's selections, leaving them
 *      with no idea whether anything happened).
 *   2. On error we leave the role/group selected so the user can fix
 *      whatever the server complained about and retry without
 *      re-picking from scratch.
 *   3. The parent decides when to clear (only on a *confirmed* server
 *      success) via the ``resetSignal`` prop, which the form watches
 *      with ``useEffect``.
 */
function AddRoleMappingForm({
  selectedRole,
  setSelectedRole,
  selectedGroup,
  setSelectedGroup,
  onAdd,
  isAdding,
}: {
  selectedRole: string;
  setSelectedRole: (role: string) => void;
  selectedGroup: string;
  setSelectedGroup: (group: string) => void;
  onAdd: (role: string, groupName: string) => void;
  isAdding: boolean;
}) {
  const { t } = useTranslation();
  const { data: rolesData } = useQuery({
    queryKey: ["availableRoles"],
    queryFn: () => listAvailableRoles(),
  });

  const roles = rolesData?.data || [];

  const handleAdd = () => {
    if (selectedRole && selectedGroup) {
      onAdd(selectedRole, selectedGroup);
    }
  };

  return (
    <div className="flex items-end gap-3 pt-4 border-t">
      <div className="flex-1 space-y-1">
        <label className="text-sm font-medium">{t("roleManagement.role")}</label>
        <Select value={selectedRole} onValueChange={setSelectedRole} disabled={isAdding}>
          <SelectTrigger>
            <SelectValue placeholder={t("roleManagement.selectRole")} />
          </SelectTrigger>
          <SelectContent>
            {roles.map((role) => (
              <SelectItem key={role} value={role}>
                <div className="flex flex-col">
                  <span>{getRoleLabel(role, t)}</span>
                  <span className="text-xs text-muted-foreground">
                    {getRoleDescription(role, t)}
                  </span>
                </div>
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="flex-1 space-y-1">
        <label className="text-sm font-medium">{t("roleManagement.databricksGroup")}</label>
        <GroupCombobox value={selectedGroup} onChange={setSelectedGroup} />
      </div>

      <Button
        onClick={handleAdd}
        disabled={!selectedRole || !selectedGroup || isAdding}
        className="shrink-0"
      >
        {isAdding ? (
          <Loader2 className="h-4 w-4 mr-1 animate-spin" />
        ) : (
          <Plus className="h-4 w-4 mr-1" />
        )}
        {isAdding ? t("roleManagement.adding") : t("roleManagement.add")}
      </Button>
    </div>
  );
}

export function RoleManagement() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [deletingKey, setDeletingKey] = useState<string | null>(null);
  // Form values live up here so we can keep them across a slow/failed
  // mutation. They're cleared in the mutation's ``onSuccess`` handler.
  const [selectedRole, setSelectedRole] = useState<string>("");
  const [selectedGroup, setSelectedGroup] = useState<string>("");

  const {
    data: mappingsData,
    isLoading,
    error,
  } = useQuery({
    queryKey: getListRoleMappingsQueryKey(),
    queryFn: () => listRoleMappings(),
  });

  const createMutation = useMutation({
    mutationFn: ({ role, groupName }: { role: string; groupName: string }) =>
      createRoleMapping({ role, group_name: groupName }),
    onSuccess: async (_data, variables) => {
      // Refetch (not just invalidate) so the new row is visible the
      // moment the success toast fires. Without ``await``, the toast
      // can race ahead of the network roundtrip and the user briefly
      // sees the old list.
      await queryClient.refetchQueries({ queryKey: getListRoleMappingsQueryKey() });
      setSelectedRole("");
      setSelectedGroup("");
      const roleLabel = getRoleLabel(variables.role, t);
      toast.success(t("roleManagement.mappingAdded", { role: roleLabel, group: variables.groupName }), {
        description: t("roleManagement.mappingAddedDescription"),
        duration: 6000,
      });
    },
    onError: (err) => {
      toast.error(t("roleManagement.failedCreate"), {
        description: extractRoleMappingError(err, t("roleManagement.fallbackError")),
        duration: 8000,
      });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: ({ role, groupName }: { role: string; groupName: string }) =>
      deleteRoleMapping(role, groupName),
    onSuccess: async (_data, variables) => {
      await queryClient.refetchQueries({ queryKey: getListRoleMappingsQueryKey() });
      setDeletingKey(null);
      toast.success(t("roleManagement.mappingRemoved", { role: variables.role, group: variables.groupName }), {
        description: t("roleManagement.mappingRemovedDescription"),
        duration: 6000,
      });
    },
    onError: (err) => {
      setDeletingKey(null);
      toast.error(t("roleManagement.failedDelete"), {
        description: extractRoleMappingError(err, t("roleManagement.fallbackError")),
        duration: 8000,
      });
    },
  });

  const mappings = mappingsData?.data || [];

  const handleAdd = (role: string, groupName: string) => {
    createMutation.mutate({ role, groupName });
  };

  const handleDelete = (role: string, groupName: string) => {
    const key = `${role}:${groupName}`;
    setDeletingKey(key);
    deleteMutation.mutate({ role, groupName });
  };

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Shield className="h-5 w-5" />
            {t("roleManagement.title")}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Shield className="h-5 w-5" />
            {t("roleManagement.title")}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2 text-destructive">
            <AlertCircle className="h-4 w-4" />
            <span>{t("roleManagement.failedLoad")}</span>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Shield className="h-5 w-5" />
          Role Management
        </CardTitle>
        <CardDescription>
          {t("roleManagement.description")}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {/*
          Propagation-delay disclosure. The frontend caches each user's
          resolved role in React Query with ``staleTime: 60_000`` (see
          ``ui/lib/route-guards.ts``), so a user whose group was just
          mapped — or unmapped — keeps their old role until the cache
          revalidates: at most ~1 minute, or sooner if they navigate.
          Surfacing this here so admins don't second-guess a successful
          assignment and re-toggle the mapping.
        */}
        <div
          className="flex items-start gap-2 rounded-md border border-border bg-muted/40 p-3 text-sm text-muted-foreground"
          role="note"
        >
          <Info className="h-4 w-4 mt-0.5 shrink-0 text-foreground/70" />
          <div>
            <p className="text-foreground/90">
              {t("roleManagement.delayPrefix")}<span className="font-medium">{t("roleManagement.delayBoldDuration")}</span>{t("roleManagement.delaySuffix")}
            </p>
            <p className="text-xs mt-0.5">
              {t("roleManagement.delayBody")}
            </p>
          </div>
        </div>

        {mappings.length === 0 ? (
          <div className="text-center py-6 text-muted-foreground">
            <Users className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>{t("roleManagement.noMappings")}</p>
            <p className="text-sm">
              {t("roleManagement.addMappingHint")}
            </p>
          </div>
        ) : (
          <div className="space-y-2">
            {mappings.map((mapping) => {
              const key = `${mapping.role}:${mapping.group_name}`;
              return (
                <RoleMappingRow
                  key={key}
                  mapping={mapping}
                  onDelete={() => handleDelete(mapping.role, mapping.group_name)}
                  isDeleting={deletingKey === key}
                />
              );
            })}
          </div>
        )}

        <AddRoleMappingForm
          selectedRole={selectedRole}
          setSelectedRole={setSelectedRole}
          selectedGroup={selectedGroup}
          setSelectedGroup={setSelectedGroup}
          onAdd={handleAdd}
          isAdding={createMutation.isPending}
        />
      </CardContent>
    </Card>
  );
}
