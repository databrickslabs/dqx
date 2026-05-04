import { useEffect, useRef, useState } from "react";
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
} from "lucide-react";
import {
  listRoleMappings,
  createRoleMapping,
  deleteRoleMapping,
  listWorkspaceGroups,
  listAvailableRoles,
  getListRoleMappingsQueryKey,
} from "@/lib/api";
import { cn } from "@/lib/utils";

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
            {value || "Select group..."}
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
            placeholder="Search groups..."
            className="border-0 shadow-none focus-visible:ring-0 px-0 h-8"
          />
          {isFetching && !isLoading ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground shrink-0" />
          ) : null}
        </div>
        <div className="max-h-64 overflow-y-auto py-1">
          {isLoading ? (
            <div className="px-3 py-6 text-center text-sm text-muted-foreground">
              Loading...
            </div>
          ) : error ? (
            <div className="px-3 py-6 text-center text-sm text-destructive">
              Failed to load groups
            </div>
          ) : groups.length === 0 ? (
            <div className="px-3 py-6 text-center text-sm text-muted-foreground">
              {debouncedSearch
                ? "No groups match that search"
                : "No groups found"}
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
                  Showing first {GROUP_SEARCH_LIMIT} matches — refine your
                  search to narrow results.
                </div>
              ) : null}
            </>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}

const ROLE_LABELS: Record<string, string> = {
  admin: "Admin",
  rule_approver: "Approver",
  rule_author: "Author",
  viewer: "Viewer",
  runner: "Runner",
};

const ROLE_DESCRIPTIONS: Record<string, string> = {
  admin: "Full access including role management (admins are implicit runners)",
  rule_approver: "Can approve/reject rules and all author permissions",
  rule_author: "Can create, edit, and submit rules",
  viewer: "Read-only access to rules",
  // Runner is intentionally additive — assigning it does NOT grant author
  // or approver privileges. It only unlocks the Run Rules page.
  runner: "Can run approved rules from the Run Rules page (additive — independent of other roles)",
};

function RoleMappingRow({
  mapping,
  onDelete,
  isDeleting,
}: {
  mapping: { role: string; group_name: string };
  onDelete: () => void;
  isDeleting: boolean;
}) {
  return (
    <div className="flex items-center justify-between py-2 px-3 bg-muted/30 rounded-md">
      <div className="flex items-center gap-3">
        <Badge variant="outline" className="font-mono">
          {ROLE_LABELS[mapping.role] || mapping.role}
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

function AddRoleMappingForm({
  onAdd,
  isAdding,
}: {
  onAdd: (role: string, groupName: string) => void;
  isAdding: boolean;
}) {
  const [selectedRole, setSelectedRole] = useState<string>("");
  const [selectedGroup, setSelectedGroup] = useState<string>("");

  const { data: rolesData } = useQuery({
    queryKey: ["availableRoles"],
    queryFn: () => listAvailableRoles(),
  });

  const roles = rolesData?.data || [];

  const handleAdd = () => {
    if (selectedRole && selectedGroup) {
      onAdd(selectedRole, selectedGroup);
      setSelectedRole("");
      setSelectedGroup("");
    }
  };

  return (
    <div className="flex items-end gap-3 pt-4 border-t">
      <div className="flex-1 space-y-1">
        <label className="text-sm font-medium">Role</label>
        <Select value={selectedRole} onValueChange={setSelectedRole}>
          <SelectTrigger>
            <SelectValue placeholder="Select role..." />
          </SelectTrigger>
          <SelectContent>
            {roles.map((role) => (
              <SelectItem key={role} value={role}>
                <div className="flex flex-col">
                  <span>{ROLE_LABELS[role] || role}</span>
                  <span className="text-xs text-muted-foreground">
                    {ROLE_DESCRIPTIONS[role]}
                  </span>
                </div>
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="flex-1 space-y-1">
        <label className="text-sm font-medium">Databricks Group</label>
        <GroupCombobox value={selectedGroup} onChange={setSelectedGroup} />
      </div>

      <Button
        onClick={handleAdd}
        disabled={!selectedRole || !selectedGroup || isAdding}
        className="shrink-0"
      >
        <Plus className="h-4 w-4 mr-1" />
        Add
      </Button>
    </div>
  );
}

export function RoleManagement() {
  const queryClient = useQueryClient();
  const [deletingKey, setDeletingKey] = useState<string | null>(null);

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
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: getListRoleMappingsQueryKey() });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: ({ role, groupName }: { role: string; groupName: string }) =>
      deleteRoleMapping(role, groupName),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: getListRoleMappingsQueryKey() });
      setDeletingKey(null);
    },
    onError: () => {
      setDeletingKey(null);
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
            Role Management
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
            Role Management
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2 text-destructive">
            <AlertCircle className="h-4 w-4" />
            <span>Failed to load role mappings</span>
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
          Map Databricks workspace groups to application roles. Users inherit
          the highest-priority role from their group memberships.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {mappings.length === 0 ? (
          <div className="text-center py-6 text-muted-foreground">
            <Users className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No role mappings configured.</p>
            <p className="text-sm">
              Add a mapping below to assign roles to Databricks groups.
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
          onAdd={handleAdd}
          isAdding={createMutation.isPending}
        />

        {createMutation.isError && (
          <div className="flex items-center gap-2 text-destructive text-sm">
            <AlertCircle className="h-4 w-4" />
            <span>Failed to create mapping. Please try again.</span>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
