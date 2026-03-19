import { createFileRoute } from "@tanstack/react-router";
import { Suspense, useState } from "react";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import {
  useListRules,
  useCreateRule,
  useDeleteRule,
  useSubmitRuleForApproval,
  useApproveRule,
  useRejectRule,
  useDryRunRule,
  useExportRule,
  useGetAppSettings,
  useGetRuleFilterOptions,
  QualityRule,
} from "@/lib/api";
import selector from "@/lib/selector";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  CardFooter,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Separator } from "@/components/ui/separator";
import { Input } from "@/components/ui/input";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  AlertCircle,
  ClipboardCheck,
  Plus,
  Trash2,
  Send,
  CheckCircle2,
  XCircle,
  Clock,
  User,
  Play,
  Loader2,
  FileText,
  Filter,
  MoreHorizontal,
  Eye,
  Edit,
  Download,
} from "lucide-react";
import { toast } from "sonner";
import { FadeIn } from "@/components/anim/FadeIn";
import { ShinyText } from "@/components/anim/ShinyText";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Textarea } from "@/components/ui/textarea";

export const Route = createFileRoute("/_sidebar/rules")({
  component: () => <RulesPage />,
});

const STATUS_COLORS: Record<string, string> = {
  draft: "bg-gray-100 text-gray-800 border-gray-300",
  pending_approval: "bg-yellow-100 text-yellow-800 border-yellow-300",
  approved: "bg-green-100 text-green-800 border-green-300",
  exported: "bg-blue-100 text-blue-800 border-blue-300",
  rejected: "bg-red-100 text-red-800 border-red-300",
};

const STATUS_ICONS: Record<string, React.ReactNode> = {
  draft: <Edit className="h-3 w-3" />,
  pending_approval: <Clock className="h-3 w-3" />,
  approved: <CheckCircle2 className="h-3 w-3" />,
  exported: <Download className="h-3 w-3" />,
  rejected: <XCircle className="h-3 w-3" />,
};

function CreateRuleDialog({
  appCatalog,
  onCreated,
}: {
  appCatalog: string;
  onCreated: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");
  const [rulesJson, setRulesJson] = useState("[]");
  const [isCreating, setIsCreating] = useState(false);

  const { mutate: createRule } = useCreateRule();

  const handleCreate = () => {
    try {
      const rules = JSON.parse(rulesJson);
      if (!Array.isArray(rules)) {
        toast.error("Rules must be a JSON array");
        return;
      }

      setIsCreating(true);
      createRule(
        {
          params: { app_catalog: appCatalog },
          data: {
            source_catalog: catalog,
            source_schema: schema,
            source_table: table,
            rules,
          },
        },
        {
          onSuccess: () => {
            setIsCreating(false);
            setOpen(false);
            toast.success("Rule created successfully");
            onCreated();
            // Reset form
            setCatalog("");
            setSchema("");
            setTable("");
            setRulesJson("[]");
          },
          onError: (error) => {
            setIsCreating(false);
            const msg = (error as any).response?.data?.detail || error.message;
            toast.error("Failed to create rule: " + msg);
          },
        }
      );
    } catch (e) {
      toast.error("Invalid JSON: " + (e as Error).message);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="mr-2 h-4 w-4" />
          New Rule
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Create Quality Rule</DialogTitle>
          <DialogDescription>
            Define a new data quality rule for a table
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4">
          <div className="grid gap-4 md:grid-cols-3">
            <div className="space-y-2">
              <Label htmlFor="new-catalog">Catalog</Label>
              <Input
                id="new-catalog"
                value={catalog}
                onChange={(e) => setCatalog(e.target.value)}
                placeholder="e.g., main"
                className="font-mono"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="new-schema">Schema</Label>
              <Input
                id="new-schema"
                value={schema}
                onChange={(e) => setSchema(e.target.value)}
                placeholder="e.g., default"
                className="font-mono"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="new-table">Table</Label>
              <Input
                id="new-table"
                value={table}
                onChange={(e) => setTable(e.target.value)}
                placeholder="e.g., customers"
                className="font-mono"
              />
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="rules-json">Rules (JSON)</Label>
            <Textarea
              id="rules-json"
              value={rulesJson}
              onChange={(e) => setRulesJson(e.target.value)}
              placeholder='[{"name": "not_null_check", "column": "id", "check": {"function": "is_not_null"}}]'
              className="font-mono h-48"
            />
            <p className="text-xs text-muted-foreground">
              Define rules as a JSON array following the DQX check format
            </p>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button
            onClick={handleCreate}
            disabled={isCreating || !catalog || !schema || !table}
          >
            {isCreating ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Creating...
              </>
            ) : (
              "Create Rule"
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function RuleChecksDisplay({ rulesJson }: { rulesJson: string }) {
  try {
    const rules = JSON.parse(rulesJson || "[]");
    if (!Array.isArray(rules) || rules.length === 0) {
      return <p className="text-xs text-muted-foreground italic">No checks defined</p>;
    }

    return (
      <div className="space-y-1.5 mt-2">
        {rules.map((rule: any, index: number) => {
          const checkType = rule.check?.function || rule.check_type || "unknown";
          const column = rule.column || "-";
          const criticality = rule.criticality || "error";
          const args = rule.check?.arguments || rule.params || {};
          
          return (
            <div
              key={index}
              className="flex items-center gap-2 flex-wrap text-xs bg-muted/50 rounded px-2 py-1.5"
            >
              <Badge variant="secondary" className="text-xs font-mono">
                {checkType}
              </Badge>
              <span className="text-muted-foreground">on</span>
              <Badge variant="outline" className="text-xs font-mono">
                {column}
              </Badge>
              <Badge
                variant={criticality === "error" ? "destructive" : "secondary"}
                className="text-xs"
              >
                {criticality}
              </Badge>
              {Object.keys(args).length > 0 && (
                <span className="text-muted-foreground font-mono text-xs">
                  {Object.entries(args)
                    .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
                    .join(", ")}
                </span>
              )}
            </div>
          );
        })}
      </div>
    );
  } catch (e) {
    return <p className="text-xs text-destructive">Invalid rules JSON</p>;
  }
}

function RuleCard({
  rule,
  appCatalog,
  onUpdate,
}: {
  rule: QualityRule;
  appCatalog: string;
  onUpdate: () => void;
}) {
  const [isDryRunning, setIsDryRunning] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isApproving, setIsApproving] = useState(false);
  const [isRejecting, setIsRejecting] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [isExporting, setIsExporting] = useState(false);
  const [showDetails, setShowDetails] = useState(false);
  const [dryRunResult, setDryRunResult] = useState<{
    pass_rate: number;
    passed_rows: number;
    failed_rows: number;
    total_rows: number;
    ran_at: Date;
  } | null>(null);

  const { mutate: submitForApproval } = useSubmitRuleForApproval();
  const { mutate: approveRule } = useApproveRule();
  const { mutate: rejectRule } = useRejectRule();
  const { mutate: deleteRule } = useDeleteRule();
  const { mutate: dryRunRule } = useDryRunRule();
  const { mutate: exportRule } = useExportRule();

  const handleSubmit = () => {
    setIsSubmitting(true);
    submitForApproval(
      { ruleId: rule.rule_id, params: { app_catalog: appCatalog } },
      {
        onSuccess: () => {
          setIsSubmitting(false);
          toast.success("Rule submitted for approval");
          onUpdate();
        },
        onError: (error) => {
          setIsSubmitting(false);
          const msg = (error as any).response?.data?.detail || error.message;
          toast.error("Failed to submit: " + msg);
        },
      }
    );
  };

  const handleApprove = () => {
    setIsApproving(true);
    approveRule(
      { ruleId: rule.rule_id, params: { app_catalog: appCatalog } },
      {
        onSuccess: () => {
          setIsApproving(false);
          toast.success("Rule approved");
          onUpdate();
        },
        onError: (error) => {
          setIsApproving(false);
          const msg = (error as any).response?.data?.detail || error.message;
          toast.error("Failed to approve: " + msg);
        },
      }
    );
  };

  const handleReject = () => {
    setIsRejecting(true);
    rejectRule(
      {
        ruleId: rule.rule_id,
        params: { app_catalog: appCatalog },
        data: { action: "reject" },
      },
      {
        onSuccess: () => {
          setIsRejecting(false);
          toast.success("Rule rejected");
          onUpdate();
        },
        onError: (error) => {
          setIsRejecting(false);
          const msg = (error as any).response?.data?.detail || error.message;
          toast.error("Failed to reject: " + msg);
        },
      }
    );
  };

  const handleDelete = () => {
    if (!confirm("Are you sure you want to delete this rule?")) return;
    setIsDeleting(true);
    deleteRule(
      { ruleId: rule.rule_id, params: { app_catalog: appCatalog } },
      {
        onSuccess: () => {
          setIsDeleting(false);
          toast.success("Rule deleted");
          onUpdate();
        },
        onError: (error) => {
          setIsDeleting(false);
          const msg = (error as any).response?.data?.detail || error.message;
          toast.error("Failed to delete: " + msg);
        },
      }
    );
  };

  const handleDryRun = () => {
    setIsDryRunning(true);
    setDryRunResult(null);
    dryRunRule(
      {
        ruleId: rule.rule_id,
        params: { app_catalog: appCatalog },
        data: { sample_size: 10000 },
      },
      {
        onSuccess: (response) => {
          setIsDryRunning(false);
          const data = response.data;
          setDryRunResult({
            pass_rate: data.pass_rate,
            passed_rows: data.passed_rows,
            failed_rows: data.failed_rows,
            total_rows: data.total_rows,
            ran_at: new Date(),
          });
          toast.success(`Dry run complete: ${data.pass_rate.toFixed(1)}% pass rate`);
        },
        onError: (error) => {
          setIsDryRunning(false);
          const msg = (error as any).response?.data?.detail || error.message;
          toast.error("Dry run failed: " + msg);
        },
      }
    );
  };

  const handleExport = () => {
    setIsExporting(true);
    exportRule(
      {
        ruleId: rule.rule_id,
        params: { app_catalog: appCatalog },
        data: { checks_location: `${rule.source_table_fqn}_checks` },
      },
      {
        onSuccess: () => {
          setIsExporting(false);
          toast.success("Rule exported to checks successfully");
          onUpdate();
        },
        onError: (error) => {
          setIsExporting(false);
          const msg = (error as any).response?.data?.detail || error.message;
          toast.error("Failed to export: " + msg);
        },
      }
    );
  };

  const statusClass = STATUS_COLORS[rule.status] || STATUS_COLORS.draft;

  return (
    <>
    <Card className="hover:border-primary/30 transition-colors">
      <CardContent className="py-4 px-4">
        <div className="flex items-start justify-between gap-4">
          <div className="flex-1 min-w-0 space-y-2">
            <div className="flex items-center gap-2 flex-wrap">
              <Badge variant="outline" className={statusClass}>
                {STATUS_ICONS[rule.status]}
                <span className="ml-1 capitalize">
                  {rule.status.replace("_", " ")}
                </span>
              </Badge>
              <span className="text-xs font-mono text-muted-foreground">
                {rule.rule_id.substring(0, 8)}...
              </span>
              <Badge variant="secondary" className="text-xs">
                {rule.rules_count} rule{rule.rules_count !== 1 ? "s" : ""}
              </Badge>
            </div>

            <p className="font-mono text-sm truncate" title={rule.source_table_fqn}>
              {rule.source_table_fqn}
            </p>

            {/* Display actual rule checks */}
            <RuleChecksDisplay rulesJson={rule.rules_json} />

            {/* Dry Run Results */}
            {dryRunResult && (
              <div className="mt-3 p-3 rounded-md bg-muted/50 border">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs font-medium flex items-center gap-1">
                    <Play className="h-3 w-3" />
                    Dry Run Results
                  </span>
                  <span className="text-xs text-muted-foreground">
                    {dryRunResult.ran_at.toLocaleTimeString()}
                  </span>
                </div>
                <div className="flex items-center gap-4">
                  <div className="flex-1">
                    <div className="flex items-center justify-between text-xs mb-1">
                      <span>Pass Rate</span>
                      <span className={`font-medium ${dryRunResult.pass_rate >= 90 ? "text-green-600" : dryRunResult.pass_rate >= 70 ? "text-yellow-600" : "text-red-600"}`}>
                        {dryRunResult.pass_rate.toFixed(1)}%
                      </span>
                    </div>
                    <div className="h-2 bg-muted rounded-full overflow-hidden">
                      <div 
                        className={`h-full transition-all ${dryRunResult.pass_rate >= 90 ? "bg-green-500" : dryRunResult.pass_rate >= 70 ? "bg-yellow-500" : "bg-red-500"}`}
                        style={{ width: `${dryRunResult.pass_rate}%` }}
                      />
                    </div>
                  </div>
                  <div className="text-xs text-right">
                    <div className="text-green-600">
                      <CheckCircle2 className="h-3 w-3 inline mr-1" />
                      {dryRunResult.passed_rows.toLocaleString()} passed
                    </div>
                    <div className="text-red-600">
                      <XCircle className="h-3 w-3 inline mr-1" />
                      {dryRunResult.failed_rows.toLocaleString()} failed
                    </div>
                    <div className="text-muted-foreground">
                      of {dryRunResult.total_rows.toLocaleString()} sampled
                    </div>
                  </div>
                </div>
              </div>
            )}

            <div className="flex items-center gap-4 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <User className="h-3 w-3" />
                {rule.created_by}
              </span>
              <span className="flex items-center gap-1">
                <Clock className="h-3 w-3" />
                {new Date(rule.created_at).toLocaleDateString()}
              </span>
              {rule.approved_by && (
                <span className="flex items-center gap-1">
                  <CheckCircle2 className="h-3 w-3 text-green-600" />
                  Approved by {rule.approved_by}
                </span>
              )}
            </div>
          </div>

          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={handleDryRun}
              disabled={isDryRunning}
            >
              {isDryRunning ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <>
                  <Play className="h-4 w-4 mr-1" />
                  Dry Run
                </>
              )}
            </Button>

            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" size="icon">
                  <MoreHorizontal className="h-4 w-4" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={() => setShowDetails(true)}>
                  <Eye className="mr-2 h-4 w-4" />
                  View Details
                </DropdownMenuItem>

                {rule.status === "draft" && (
                  <>
                    <DropdownMenuItem disabled>
                      <Edit className="mr-2 h-4 w-4" />
                      Edit Rule
                    </DropdownMenuItem>
                    <DropdownMenuItem
                      onClick={handleSubmit}
                      disabled={isSubmitting}
                    >
                      <Send className="mr-2 h-4 w-4" />
                      Submit for Approval
                    </DropdownMenuItem>
                  </>
                )}

                {rule.status === "pending_approval" && (
                  <>
                    <DropdownMenuItem
                      onClick={handleApprove}
                      disabled={isApproving}
                      className="text-green-600"
                    >
                      <CheckCircle2 className="mr-2 h-4 w-4" />
                      Approve
                    </DropdownMenuItem>
                    <DropdownMenuItem
                      onClick={handleReject}
                      disabled={isRejecting}
                      className="text-red-600"
                    >
                      <XCircle className="mr-2 h-4 w-4" />
                      Reject
                    </DropdownMenuItem>
                  </>
                )}

                {rule.status === "approved" && (
                  <DropdownMenuItem
                    onClick={handleExport}
                    disabled={isExporting}
                  >
                    {isExporting ? (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    ) : (
                      <Download className="mr-2 h-4 w-4" />
                    )}
                    Export to Checks
                  </DropdownMenuItem>
                )}

                <DropdownMenuSeparator />

                <DropdownMenuItem
                  onClick={handleDelete}
                  disabled={isDeleting}
                  className="text-destructive"
                >
                  <Trash2 className="mr-2 h-4 w-4" />
                  Delete
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>
      </CardContent>
    </Card>

    {/* View Details Dialog */}
    <Dialog open={showDetails} onOpenChange={setShowDetails}>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <FileText className="h-5 w-5" />
            Rule Details
          </DialogTitle>
          <DialogDescription>
            Full details for rule {rule.rule_id.substring(0, 8)}...
          </DialogDescription>
        </DialogHeader>
        
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label className="text-xs text-muted-foreground">Status</Label>
              <Badge variant="outline" className={STATUS_COLORS[rule.status] || STATUS_COLORS.draft}>
                {STATUS_ICONS[rule.status]}
                <span className="ml-1 capitalize">{rule.status.replace("_", " ")}</span>
              </Badge>
            </div>
            <div>
              <Label className="text-xs text-muted-foreground">Rule ID</Label>
              <p className="font-mono text-sm">{rule.rule_id}</p>
            </div>
          </div>

          <div>
            <Label className="text-xs text-muted-foreground">Source Table</Label>
            <p className="font-mono text-sm bg-muted px-2 py-1 rounded">{rule.source_table_fqn}</p>
          </div>

          <Separator />

          <div>
            <Label className="text-xs text-muted-foreground mb-2 block">
              Quality Checks ({rule.rules_count})
            </Label>
            <RuleChecksDisplay rulesJson={rule.rules_json} />
          </div>

          <Separator />

          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <Label className="text-xs text-muted-foreground">Created By</Label>
              <p>{rule.created_by}</p>
            </div>
            <div>
              <Label className="text-xs text-muted-foreground">Created At</Label>
              <p>{new Date(rule.created_at).toLocaleString()}</p>
            </div>
            {rule.approved_by && (
              <>
                <div>
                  <Label className="text-xs text-muted-foreground">Approved By</Label>
                  <p>{rule.approved_by}</p>
                </div>
                <div>
                  <Label className="text-xs text-muted-foreground">Approved At</Label>
                  <p>{rule.approved_at ? new Date(rule.approved_at).toLocaleString() : "-"}</p>
                </div>
              </>
            )}
            {rule.exported_to && (
              <>
                <div>
                  <Label className="text-xs text-muted-foreground">Exported To</Label>
                  <p className="font-mono text-xs">{rule.exported_to}</p>
                </div>
                <div>
                  <Label className="text-xs text-muted-foreground">Exported At</Label>
                  <p>{rule.exported_at ? new Date(rule.exported_at).toLocaleString() : "-"}</p>
                </div>
              </>
            )}
          </div>

          <Separator />

          <div>
            <Label className="text-xs text-muted-foreground mb-2 block">Raw Rules JSON</Label>
            <pre className="bg-muted p-3 rounded text-xs overflow-x-auto max-h-48">
              {JSON.stringify(JSON.parse(rule.rules_json || "[]"), null, 2)}
            </pre>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => setShowDetails(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
    </>
  );
}

function RulesList({ appCatalog }: { appCatalog: string }) {
  const [statusFilter, setStatusFilter] = useState<string>("all");
  const [catalogFilter, setCatalogFilter] = useState<string>("all");
  const [schemaFilter, setSchemaFilter] = useState<string>("all");
  const [tableFilter, setTableFilter] = useState<string>("all");
  const queryClient = useQueryClient();

  // Fetch filter options
  const { data: filterOptions } = useGetRuleFilterOptions(
    { app_catalog: appCatalog },
    { query: { enabled: !!appCatalog } }
  );

  const catalogs = filterOptions?.data?.catalogs || [];
  const schemas = filterOptions?.data?.schemas || [];
  const tables = filterOptions?.data?.tables || [];

  const { data, isLoading, error, refetch } = useListRules(
    {
      app_catalog: appCatalog,
      status: statusFilter !== "all" ? statusFilter : undefined,
      source_catalog: catalogFilter !== "all" ? catalogFilter : undefined,
      source_schema: schemaFilter !== "all" ? schemaFilter : undefined,
      table_name: tableFilter !== "all" ? tableFilter : undefined,
      limit: 100,
    },
    { query: { enabled: !!appCatalog } }
  );

  const handleUpdate = () => {
    refetch();
    queryClient.invalidateQueries({ queryKey: ["/api/rules/filter-options"] });
  };

  const clearFilters = () => {
    setStatusFilter("all");
    setCatalogFilter("all");
    setSchemaFilter("all");
    setTableFilter("all");
  };

  const hasActiveFilters = statusFilter !== "all" || catalogFilter !== "all" || 
    schemaFilter !== "all" || tableFilter !== "all";

  if (isLoading) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-24 w-full" />
        <Skeleton className="h-24 w-full" />
        <Skeleton className="h-24 w-full" />
      </div>
    );
  }

  if (error) {
    return (
      <Card className="border-destructive/50">
        <CardContent className="pt-4">
          <div className="flex items-center gap-2 text-sm text-destructive">
            <AlertCircle className="h-4 w-4" />
            Failed to load rules. Make sure the app is initialized.
          </div>
        </CardContent>
      </Card>
    );
  }

  const rules = data?.data?.rules || [];

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Filter className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm font-medium">Filters</span>
            {hasActiveFilters && (
              <Button variant="ghost" size="sm" onClick={clearFilters} className="h-6 px-2 text-xs">
                Clear all
              </Button>
            )}
          </div>
          <CreateRuleDialog appCatalog={appCatalog} onCreated={handleUpdate} />
        </div>
        
        <div className="flex flex-wrap items-center gap-2">
          <Select value={catalogFilter} onValueChange={setCatalogFilter}>
            <SelectTrigger className="w-40">
              <SelectValue placeholder="Catalog" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Catalogs</SelectItem>
              {catalogs.map((catalog) => (
                <SelectItem key={catalog} value={catalog}>{catalog}</SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Select value={schemaFilter} onValueChange={setSchemaFilter}>
            <SelectTrigger className="w-40">
              <SelectValue placeholder="Schema" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Schemas</SelectItem>
              {schemas.map((schema) => (
                <SelectItem key={schema} value={schema}>{schema}</SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Select value={tableFilter} onValueChange={setTableFilter}>
            <SelectTrigger className="w-40">
              <SelectValue placeholder="Table" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Tables</SelectItem>
              {tables.map((table) => (
                <SelectItem key={table} value={table}>{table}</SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Select value={statusFilter} onValueChange={setStatusFilter}>
            <SelectTrigger className="w-44">
              <SelectValue placeholder="Status" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Statuses</SelectItem>
              <SelectItem value="draft">Draft</SelectItem>
              <SelectItem value="pending_approval">Pending Approval</SelectItem>
              <SelectItem value="approved">Approved</SelectItem>
              <SelectItem value="exported">Exported</SelectItem>
              <SelectItem value="rejected">Rejected</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      {rules.length === 0 ? (
        <Card>
          <CardContent className="py-8 text-center">
            <FileText className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
            <h3 className="text-lg font-medium mb-1">No rules found</h3>
            <p className="text-sm text-muted-foreground">
              {hasActiveFilters
                ? "No rules match the current filters. Try adjusting your filters."
                : "Create your first quality rule to get started"}
            </p>
            {hasActiveFilters && (
              <Button variant="outline" size="sm" onClick={clearFilters} className="mt-3">
                Clear Filters
              </Button>
            )}
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-3">
          {rules.map((rule) => (
            <RuleCard
              key={rule.rule_id}
              rule={rule}
              appCatalog={appCatalog}
              onUpdate={handleUpdate}
            />
          ))}
        </div>
      )}

      {data?.data?.total !== undefined && (
        <p className="text-xs text-muted-foreground text-center">
          Showing {rules.length} of {data.data.total} rules
        </p>
      )}
    </div>
  );
}

const STORAGE_KEY = "dqx_app_catalog";

function RulesPage() {
  // Get the configured catalog from localStorage
  const storedCatalog = typeof window !== "undefined" 
    ? localStorage.getItem(STORAGE_KEY) || "main" 
    : "main";

  // Get app settings to know the app catalog
  const { data: appSettings, isLoading: isLoadingSettings } = useGetAppSettings(
    { catalog: storedCatalog },
    { query: { retry: false, enabled: !!storedCatalog } }
  );

  const appCatalog = appSettings?.data?.target_catalog || storedCatalog;
  const isInitialized = appSettings?.data?.initialized === true;

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb page="Quality Rules" />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            <ShinyText text="Quality Rules" speed={6} className="font-bold" />
          </h1>
          <p className="text-muted-foreground">
            Manage data quality rules and their approval workflow
          </p>
        </div>
      </div>

      {!isInitialized && !isLoadingSettings && (
        <FadeIn>
          <Card className="border-yellow-500/50 bg-yellow-50/10">
            <CardContent className="pt-4">
              <div className="flex items-center gap-2 text-sm">
                <AlertCircle className="h-4 w-4 text-yellow-600" />
                <span>
                  App not initialized. Please go to{" "}
                  <a href="/config" className="text-primary underline">
                    Configuration
                  </a>{" "}
                  to set up the app first.
                </span>
              </div>
            </CardContent>
          </Card>
        </FadeIn>
      )}

      <FadeIn delay={0.1}>
        <QueryErrorResetBoundary>
          {({ reset }) => (
            <ErrorBoundary
              onReset={reset}
              fallbackRender={({ resetErrorBoundary }) => (
                <Card className="border-destructive/50">
                  <CardContent className="pt-4">
                    <div className="flex items-center gap-2">
                      <AlertCircle className="h-4 w-4 text-destructive" />
                      <span className="text-sm">Failed to load rules</span>
                      <Button variant="outline" size="sm" onClick={resetErrorBoundary}>
                        Retry
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              )}
            >
              <RulesList appCatalog={appCatalog} />
            </ErrorBoundary>
          )}
        </QueryErrorResetBoundary>
      </FadeIn>
    </div>
  );
}
