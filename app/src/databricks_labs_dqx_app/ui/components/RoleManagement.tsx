import { useCallback, useEffect, useRef, useState } from "react";
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import {
  Users,
  Plus,
  Trash2,
  Shield,
  AlertCircle,
  Loader2,
  Info,
} from "lucide-react";
import { isAxiosError } from "axios";
import { toast } from "sonner";
import {
  listRoleMappings,
  createRoleMapping,
  deleteRoleMapping,
  listAvailableRoles,
  getListRoleMappingsQueryKey,
  useListPrivilegedPrincipals,
  type PrivilegedPrincipalOut,
  PrivilegedPrincipalOutKind,
  type PrincipalSearchOut,
} from "@/lib/api";
/** Best-effort OS detection for the hard-refresh keyboard hint. Falls back to
 *  the Ctrl form off-Mac (and when userAgentData/platform is unavailable). */
function hardRefreshShortcut(): string {
  const nav =
    typeof navigator !== "undefined"
      ? (navigator as Navigator & { userAgentData?: { platform?: string } })
      : undefined;
  const platform = nav?.userAgentData?.platform || nav?.platform || nav?.userAgent || "";
  return /mac|iphone|ipad|ipod/i.test(platform) ? "⌘+R" : "Ctrl+R";
}
import {
  PrincipalPicker,
  type PickedPrincipal,
} from "@/components/permissions/PrincipalPicker";

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
    <div className="flex items-center justify-between rounded-md border bg-muted/30 p-3">
      <div className="flex items-center gap-3">
        <span className="font-medium">{mapping.group_name}</span>
        <span className="text-sm text-muted-foreground">→</span>
        <Badge variant="outline" className="font-mono">
          {getRoleLabel(mapping.role, t)}
        </Badge>
      </div>
      <Button
        variant="ghost"
        size="sm"
        onClick={onDelete}
        disabled={isDeleting}
        aria-label={t("roleManagement.deleteMappingAria", { role: getRoleLabel(mapping.role, t), group: mapping.group_name })}
        className="text-destructive hover:text-destructive hover:bg-destructive/10"
      >
        <Trash2 className="h-4 w-4" />
      </Button>
    </div>
  );
}

/** A disabled row representing a workspace admin or app owner — no delete affordance. */
function PrivilegedPrincipalRow({ principal }: { principal: PrivilegedPrincipalOut }) {
  const { t } = useTranslation();
  const suffix =
    principal.kind === PrivilegedPrincipalOutKind.workspace_admin
      ? t("roleManagement.suffixWorkspaceAdmin")
      : t("roleManagement.suffixAppOwner");
  return (
    <div className="flex items-center justify-between rounded-md border bg-muted/20 p-3 opacity-60">
      <div className="flex items-center gap-3">
        <span className="font-medium">{principal.principal}</span>
        <span className="text-sm text-muted-foreground">{suffix}</span>
      </div>
      {/* No delete button — implicit elevated access */}
    </div>
  );
}

/**
 * Inline form for adding a new entitlement.
 *
 * Auto-submits when both a User/Group and an Entitlement are selected
 * (no explicit "Add" button needed for the in-progress selection). The
 * `resetSignal` prop is incremented by the parent on a confirmed server
 * success, clearing the local selections so the form is ready for the
 * next mapping.
 */
function AddEntitlementForm({
  onAdd,
  isAdding,
  existingMappings,
  resetSignal,
}: {
  onAdd: (role: string, groupName: string) => void;
  isAdding: boolean;
  existingMappings: { role: string; group_name: string }[];
  resetSignal: number;
}) {
  const { t } = useTranslation();
  const [pickedPrincipal, setPickedPrincipal] = useState<PickedPrincipal | null>(null);
  const [selectedRole, setSelectedRole] = useState<string>("");
  // Guard against re-firing the same principal+role pair (strict-mode double
  // invocations, rapid state changes, or duplicate selections).
  const lastFiredRef = useRef<string>("");

  const { data: rolesData } = useQuery({
    queryKey: ["availableRoles"],
    queryFn: () => listAvailableRoles(),
  });

  const roles = rolesData?.data || [];

  // Reset local selections whenever the parent signals a successful add.
  useEffect(() => {
    setPickedPrincipal(null);
    setSelectedRole("");
    lastFiredRef.current = "";
  }, [resetSignal]);

  // Auto-add as soon as both fields are filled in.
  useEffect(() => {
    if (!pickedPrincipal || !selectedRole || isAdding) return;
    const pairKey = `${selectedRole}:${pickedPrincipal.principal_name}`;
    if (lastFiredRef.current === pairKey) return;
    const isDuplicate = existingMappings.some(
      (m) => m.role === selectedRole && m.group_name === pickedPrincipal.principal_name,
    );
    if (isDuplicate) {
      toast.info(t("roleManagement.mappingAlreadyExists"));
      return;
    }
    lastFiredRef.current = pairKey;
    onAdd(selectedRole, pickedPrincipal.principal_name);
  }, [pickedPrincipal, selectedRole, isAdding, existingMappings, onAdd]);

  const handleSelect = (p: PrincipalSearchOut) => {
    setPickedPrincipal({
      principal_id: p.workspace_principal_id,
      principal_type: p.kind,
      principal_name: p.display_name,
    });
  };

  const handleClear = () => {
    setPickedPrincipal(null);
    lastFiredRef.current = "";
  };

  return (
    <div className="flex flex-wrap items-start gap-3">
      {/* User/Group */}
      <div className="space-y-1.5">
        <label className="block text-sm font-medium">{t("roleManagement.databricksGroup")}</label>
        <PrincipalPicker
          value={pickedPrincipal}
          onSelect={handleSelect}
          onClear={handleClear}
          disabled={isAdding}
          className="w-56"
        />
      </div>

      {/* Entitlement — trigger shows the entitlement name only, left-aligned */}
      <div className="space-y-1.5">
        <label className="block text-sm font-medium">{t("roleManagement.role")}</label>
        <Select value={selectedRole || undefined} onValueChange={setSelectedRole} disabled={isAdding}>
          <SelectTrigger className="w-64 justify-between">
            {/* Explicit children override Radix's default (which would echo the
                item's two-line content) so the trigger shows the name only. */}
            <SelectValue placeholder={t("roleManagement.selectRole")}>
              {selectedRole ? getRoleLabel(selectedRole, t) : undefined}
            </SelectValue>
          </SelectTrigger>
          <SelectContent>
            {roles.map((role) => (
              <SelectItem key={role} value={role} textValue={getRoleLabel(role, t)}>
                <div className="flex flex-col text-left">
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

      {isAdding && (
        <Loader2 className="h-4 w-4 animate-spin text-muted-foreground self-center mt-6" />
      )}
    </div>
  );
}

export function RoleManagement() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [deletingKey, setDeletingKey] = useState<string | null>(null);
  const [pendingDelete, setPendingDelete] = useState<{ role: string; group: string } | null>(null);
  // Whether to show the inline "add entitlement" form.
  const [showAddForm, setShowAddForm] = useState(false);
  // Incremented on successful add so AddEntitlementForm resets its local state.
  const [resetSignal, setResetSignal] = useState(0);

  const {
    data: mappingsData,
    isLoading,
    error,
  } = useQuery({
    queryKey: getListRoleMappingsQueryKey(),
    queryFn: () => listRoleMappings(),
  });

  const { data: privilegedData } = useListPrivilegedPrincipals({
    query: { select: (d) => d.data },
  });
  const privilegedPrincipals: PrivilegedPrincipalOut[] = privilegedData ?? [];

  const createMutation = useMutation({
    mutationFn: ({ role, groupName }: { role: string; groupName: string }) =>
      createRoleMapping({ role, group_name: groupName }),
    onSuccess: async (_data, variables) => {
      await queryClient.refetchQueries({ queryKey: getListRoleMappingsQueryKey() });
      // Signal the form to reset and hide it.
      setResetSignal((n) => n + 1);
      setShowAddForm(false);
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

  const handleAdd = useCallback((role: string, groupName: string) => {
    createMutation.mutate({ role, groupName });
  }, [createMutation]);

  const handleDeleteRequest = (role: string, groupName: string) => {
    setPendingDelete({ role, group: groupName });
  };

  const confirmDelete = () => {
    if (!pendingDelete) return;
    const { role, group } = pendingDelete;
    const key = `${role}:${group}`;
    setDeletingKey(key);
    setPendingDelete(null);
    deleteMutation.mutate({ role, groupName: group });
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

  const hasRows = mappings.length > 0 || privilegedPrincipals.length > 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Shield className="h-5 w-5" />
          {t("roleManagement.title")}
        </CardTitle>
        <CardDescription>
          {t("roleManagement.description")}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Propagation-delay disclosure */}
        <div
          className="flex items-start gap-2 rounded-md border border-border bg-muted/40 p-3 text-sm text-muted-foreground"
          role="note"
        >
          <Info className="h-4 w-4 mt-0.5 shrink-0 text-foreground/70" />
          <div>
            <p className="text-foreground/90">
              {t("roleManagement.delayTitle")}
            </p>
            <p className="text-xs mt-0.5">
              {t("roleManagement.delayBody", { shortcut: hardRefreshShortcut() })}
            </p>
          </div>
        </div>

        {/* Entitlement rows — real mappings + privileged principals */}
        {!hasRows ? (
          <div className="text-center py-6 text-muted-foreground">
            <Users className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>{t("roleManagement.noMappings")}</p>
            <p className="text-sm">
              {t("roleManagement.addMappingHint")}
            </p>
          </div>
        ) : (
          <div className="space-y-2">
            {privilegedPrincipals.map((p) => (
              <PrivilegedPrincipalRow key={`${p.kind}:${p.principal}`} principal={p} />
            ))}
            {mappings.map((mapping) => {
              const key = `${mapping.role}:${mapping.group_name}`;
              return (
                <RoleMappingRow
                  key={key}
                  mapping={mapping}
                  onDelete={() => handleDeleteRequest(mapping.role, mapping.group_name)}
                  isDeleting={deletingKey === key}
                />
              );
            })}
          </div>
        )}

        {/* Inline add form — revealed on button click */}
        {showAddForm && (
          <AddEntitlementForm
            onAdd={handleAdd}
            isAdding={createMutation.isPending}
            existingMappings={mappings}
            resetSignal={resetSignal}
          />
        )}

        {/* "Add new entitlement" sits below the rows */}
        {!showAddForm && (
          <Button
            variant="outline"
            size="sm"
            onClick={() => setShowAddForm(true)}
            className="gap-1.5"
          >
            <Plus className="h-4 w-4" />
            {t("roleManagement.add")}
          </Button>
        )}
      </CardContent>

      <AlertDialog
        open={pendingDelete !== null}
        onOpenChange={(open) => {
          if (!open) setPendingDelete(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("roleManagement.deleteMappingTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {pendingDelete
                ? t("roleManagement.deleteMappingBody", {
                    role: getRoleLabel(pendingDelete.role, t),
                    group: pendingDelete.group,
                  })
                : null}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmDelete}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              {t("common.delete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </Card>
  );
}
