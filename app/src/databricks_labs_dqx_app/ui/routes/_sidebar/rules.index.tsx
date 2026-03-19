import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useState } from "react";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  BookCheck,
  Plus,
  Trash2,
  SendHorizonal,
  CheckCircle2,
  XCircle,
  Clock,
  FileEdit,
} from "lucide-react";
import { FadeIn } from "@/components/anim/FadeIn";
import { ShinyText } from "@/components/anim/ShinyText";
import { toast } from "sonner";
import {
  useListRules,
  useDeleteRules,
  useSubmitRulesForApproval,
  useApproveRules,
  useRejectRules,
  type RuleCatalogEntryOut,
} from "@/lib/api";

export const Route = createFileRoute("/_sidebar/rules/")({
  component: RulesIndexPage,
});

const STATUS_OPTIONS = [
  { value: "all", label: "All Statuses" },
  { value: "draft", label: "Draft" },
  { value: "pending_approval", label: "Pending Approval" },
  { value: "approved", label: "Approved" },
  { value: "rejected", label: "Rejected" },
];

function statusBadge(status: string) {
  switch (status) {
    case "draft":
      return (
        <Badge variant="secondary" className="gap-1">
          <FileEdit className="h-3 w-3" />
          Draft
        </Badge>
      );
    case "pending_approval":
      return (
        <Badge variant="outline" className="gap-1 border-amber-500 text-amber-600">
          <Clock className="h-3 w-3" />
          Pending
        </Badge>
      );
    case "approved":
      return (
        <Badge variant="outline" className="gap-1 border-green-500 text-green-600">
          <CheckCircle2 className="h-3 w-3" />
          Approved
        </Badge>
      );
    case "rejected":
      return (
        <Badge variant="outline" className="gap-1 border-red-500 text-red-600">
          <XCircle className="h-3 w-3" />
          Rejected
        </Badge>
      );
    default:
      return <Badge variant="secondary">{status}</Badge>;
  }
}

function RulesIndexPage() {
  const navigate = useNavigate();
  const [statusFilter, setStatusFilter] = useState("all");
  const [pendingAction, setPendingAction] = useState<string | null>(null);

  const {
    data: rulesResp,
    isLoading,
    error,
    refetch,
  } = useListRules(
    statusFilter === "all" ? {} : { status: statusFilter },
  );
  const rules: RuleCatalogEntryOut[] = Array.isArray(rulesResp?.data) ? rulesResp.data : [];

  const deleteRulesMutation = useDeleteRules();
  const submitMutation = useSubmitRulesForApproval();
  const approveMutation = useApproveRules();
  const rejectMutation = useRejectRules();

  const isBusy = pendingAction !== null;

  const handleDelete = async (tableFqn: string) => {
    if (isBusy) return;
    if (!confirm(`Delete rules for ${tableFqn}?`)) return;
    setPendingAction(tableFqn);
    try {
      await deleteRulesMutation.mutateAsync({ tableFqn });
      toast.success(`Rules deleted for ${tableFqn}`);
      refetch();
    } catch {
      toast.error("Failed to delete rules");
    } finally {
      setPendingAction(null);
    }
  };

  const handleSubmit = async (tableFqn: string, version: number) => {
    if (isBusy) return;
    setPendingAction(tableFqn);
    try {
      await submitMutation.mutateAsync({
        tableFqn,
        data: { status: "pending_approval", expected_version: version },
      });
      toast.success("Submitted for approval");
      refetch();
    } catch {
      toast.error("Failed to submit for approval");
    } finally {
      setPendingAction(null);
    }
  };

  const handleApprove = async (tableFqn: string, version: number) => {
    if (isBusy) return;
    setPendingAction(tableFqn);
    try {
      await approveMutation.mutateAsync({
        tableFqn,
        data: { status: "approved", expected_version: version },
      });
      toast.success("Rules approved");
      refetch();
    } catch {
      toast.error("Failed to approve rules");
    } finally {
      setPendingAction(null);
    }
  };

  const handleReject = async (tableFqn: string, version: number) => {
    if (isBusy) return;
    setPendingAction(tableFqn);
    try {
      await rejectMutation.mutateAsync({
        tableFqn,
        data: { status: "rejected", expected_version: version },
      });
      toast.success("Rules rejected");
      refetch();
    } catch {
      toast.error("Failed to reject rules");
    } finally {
      setPendingAction(null);
    }
  };

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb page="Rules" />
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight">
              <ShinyText text="Rules Catalog" speed={6} className="font-bold" />
            </h1>
            <p className="text-muted-foreground">
              Manage data quality rule sets for your tables.
            </p>
          </div>
          <Button onClick={() => navigate({ to: "/rules/generate" })} className="gap-2">
            <Plus className="h-4 w-4" />
            New Rules
          </Button>
        </div>
      </div>

      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <BookCheck className="h-5 w-5" />
                Rule Sets
              </CardTitle>
              <CardDescription>
                {isLoading
                  ? "Loading..."
                  : `${rules.length} rule set${rules.length !== 1 ? "s" : ""}`}
              </CardDescription>
            </div>
            <Select value={statusFilter} onValueChange={setStatusFilter}>
              <SelectTrigger className="w-[180px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {STATUS_OPTIONS.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardHeader>
        <CardContent>
          {isLoading && (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-14 w-full" />
              ))}
            </div>
          )}

          {error && (
            <p className="text-destructive text-sm">
              Failed to load rules: {(error as Error).message}
            </p>
          )}

          {!isLoading && !error && rules.length > 0 && (
            <FadeIn duration={0.3}>
              <div className="border rounded-lg overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b bg-muted/50">
                      <th className="text-left p-3 font-medium">Table</th>
                      <th className="text-left p-3 font-medium">Status</th>
                      <th className="text-left p-3 font-medium">Version</th>
                      <th className="text-left p-3 font-medium">Rules</th>
                      <th className="text-left p-3 font-medium">Updated</th>
                      <th className="text-right p-3 font-medium">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rules.map((rule) => (
                      <tr
                        key={rule.table_fqn}
                        className="border-b last:border-b-0 hover:bg-muted/30 transition-colors cursor-pointer"
                        onClick={() =>
                          navigate({
                            to: "/rules/generate",
                            search: { table: rule.table_fqn },
                          })
                        }
                      >
                        <td className="p-3 font-mono text-xs">
                          {rule.table_fqn}
                        </td>
                        <td className="p-3">{statusBadge(rule.status)}</td>
                        <td className="p-3 tabular-nums">v{rule.version}</td>
                        <td className="p-3 tabular-nums">
                          {rule.checks.length}
                        </td>
                        <td className="p-3 text-muted-foreground text-xs">
                          {rule.updated_at
                            ? new Date(rule.updated_at).toLocaleDateString()
                            : "-"}
                        </td>
                        <td className="p-3 text-right">
                          <div
                            className="flex items-center justify-end gap-1"
                            onClick={(e) => e.stopPropagation()}
                          >
                            {rule.status === "draft" && (
                              <Button
                                size="sm"
                                variant="outline"
                                disabled={isBusy}
                                onClick={() => handleSubmit(rule.table_fqn, rule.version)}
                                className="gap-1 h-7 text-xs"
                              >
                                <SendHorizonal className="h-3 w-3" />
                                {pendingAction === rule.table_fqn ? "Submitting..." : "Submit"}
                              </Button>
                            )}
                            {rule.status === "pending_approval" && (
                              <>
                                <Button
                                  size="sm"
                                  variant="outline"
                                  disabled={isBusy}
                                  onClick={() => handleApprove(rule.table_fqn, rule.version)}
                                  className="gap-1 h-7 text-xs text-green-600"
                                >
                                  <CheckCircle2 className="h-3 w-3" />
                                  {pendingAction === rule.table_fqn ? "Approving..." : "Approve"}
                                </Button>
                                <Button
                                  size="sm"
                                  variant="outline"
                                  disabled={isBusy}
                                  onClick={() => handleReject(rule.table_fqn, rule.version)}
                                  className="gap-1 h-7 text-xs text-red-600"
                                >
                                  <XCircle className="h-3 w-3" />
                                  {pendingAction === rule.table_fqn ? "Rejecting..." : "Reject"}
                                </Button>
                              </>
                            )}
                            <Button
                              size="sm"
                              variant="ghost"
                              disabled={isBusy}
                              onClick={() => handleDelete(rule.table_fqn)}
                              className="h-7 text-xs text-destructive"
                            >
                              <Trash2 className="h-3 w-3" />
                            </Button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </FadeIn>
          )}

          {!isLoading && !error && rules.length === 0 && (
            <div className="flex flex-col items-center justify-center py-16 text-center">
              <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
                <BookCheck className="h-8 w-8 text-muted-foreground" />
              </div>
              <h3 className="text-lg font-medium text-muted-foreground">
                No Rules Yet
              </h3>
              <p className="text-muted-foreground/70 text-sm mt-1 max-w-md">
                Create your first rule set by selecting a table and generating
                rules with AI.
              </p>
              <Button
                onClick={() => navigate({ to: "/rules/generate" })}
                className="mt-4 gap-2"
              >
                <Plus className="h-4 w-4" />
                New Rules
              </Button>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
