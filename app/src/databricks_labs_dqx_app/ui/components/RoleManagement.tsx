import { useState } from "react";
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
import { Users, Plus, Trash2, Shield, AlertCircle } from "lucide-react";
import {
  listRoleMappings,
  createRoleMapping,
  deleteRoleMapping,
  listWorkspaceGroups,
  listAvailableRoles,
  getListRoleMappingsQueryKey,
} from "@/lib/api";

const ROLE_LABELS: Record<string, string> = {
  admin: "Admin",
  rule_approver: "Approver",
  rule_author: "Author",
  viewer: "Viewer",
};

const ROLE_DESCRIPTIONS: Record<string, string> = {
  admin: "Full access including role management",
  rule_approver: "Can approve/reject rules and all author permissions",
  rule_author: "Can create, edit, and submit rules",
  viewer: "Read-only access to rules",
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

  const { data: groupsData, isLoading: groupsLoading } = useQuery({
    queryKey: ["workspaceGroups"],
    queryFn: () => listWorkspaceGroups(),
  });

  const roles = rolesData?.data || [];
  const groups = groupsData?.data || [];

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
        <Select
          value={selectedGroup}
          onValueChange={setSelectedGroup}
          disabled={groupsLoading}
        >
          <SelectTrigger>
            <SelectValue
              placeholder={groupsLoading ? "Loading..." : "Select group..."}
            />
          </SelectTrigger>
          <SelectContent>
            {groups.map((group) => (
              <SelectItem key={group.display_name} value={group.display_name}>
                {group.display_name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
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
