import { createFileRoute } from "@tanstack/react-router";
import { Suspense, useState, useEffect } from "react";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import {
  useProfileTable,
  useGetExistingProfiles,
  useGetAppSettings,
  useGetAvailableCatalogs,
  useGetAvailableSchemas,
  useGetAvailableTables,
  useCreateRule,
  GeneratedRule,
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import { Label } from "@/components/ui/label";
import {
  AlertCircle,
  Database,
  Search,
  Play,
  Loader2,
  Clock,
  User,
  CheckCircle2,
  XCircle,
  Table2,
  BarChart3,
  ChevronRight,
  Save,
  ListChecks,
} from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";
import { toast } from "sonner";
import { FadeIn } from "@/components/anim/FadeIn";
import { ShinyText } from "@/components/anim/ShinyText";

export const Route = createFileRoute("/_sidebar/profiler")({
  component: () => <ProfilerPage />,
});

function TableSelector({
  onTableSelect,
}: {
  onTableSelect: (catalog: string, schema: string, table: string) => void;
}) {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const [table, setTable] = useState("");

  // Fetch catalogs
  const { data: catalogsData, isLoading: isLoadingCatalogs } = useGetAvailableCatalogs();
  const catalogs = catalogsData?.data?.catalogs || [];

  // Fetch schemas when catalog is selected
  const { data: schemasData, isLoading: isLoadingSchemas } = useGetAvailableSchemas(
    { catalog },
    { query: { enabled: !!catalog } }
  );
  const schemas = schemasData?.data?.schemas || [];

  // Fetch tables when schema is selected
  const { data: tablesData, isLoading: isLoadingTables } = useGetAvailableTables(
    { catalog, schema },
    { query: { enabled: !!catalog && !!schema } }
  );
  const tables = tablesData?.data?.tables || [];

  // Reset dependent selections when parent changes
  useEffect(() => {
    setSchema("");
    setTable("");
  }, [catalog]);

  useEffect(() => {
    setTable("");
  }, [schema]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (catalog && schema && table) {
      onTableSelect(catalog, schema, table);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Table2 className="h-5 w-5" />
          Select Table to Profile
        </CardTitle>
        <CardDescription>
          Choose a table from your Unity Catalog to analyze
        </CardDescription>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="grid gap-4 md:grid-cols-3">
            {/* Catalog Selector */}
            <div className="space-y-2">
              <Label>Catalog</Label>
              {isLoadingCatalogs ? (
                <div className="flex items-center gap-2 h-10 px-3 border rounded-md">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span className="text-sm text-muted-foreground">Loading...</span>
                </div>
              ) : catalogs.length > 0 ? (
                <Select value={catalog} onValueChange={setCatalog}>
                  <SelectTrigger className="font-mono">
                    <SelectValue placeholder="Select catalog" />
                  </SelectTrigger>
                  <SelectContent>
                    {catalogs.map((c: { name: string }) => (
                      <SelectItem key={c.name} value={c.name} className="font-mono">
                        {c.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              ) : (
                <Input
                  value={catalog}
                  onChange={(e) => setCatalog(e.target.value)}
                  placeholder="Enter catalog name"
                  className="font-mono"
                />
              )}
            </div>

            {/* Schema Selector */}
            <div className="space-y-2">
              <Label>Schema</Label>
              {!catalog ? (
                <div className="flex items-center gap-2 h-10 px-3 border rounded-md bg-muted/50">
                  <span className="text-sm text-muted-foreground">Select catalog first</span>
                </div>
              ) : isLoadingSchemas ? (
                <div className="flex items-center gap-2 h-10 px-3 border rounded-md">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span className="text-sm text-muted-foreground">Loading...</span>
                </div>
              ) : schemas.length > 0 ? (
                <Select value={schema} onValueChange={setSchema}>
                  <SelectTrigger className="font-mono">
                    <SelectValue placeholder="Select schema" />
                  </SelectTrigger>
                  <SelectContent>
                    {schemas.map((s: { name: string }) => (
                      <SelectItem key={s.name} value={s.name} className="font-mono">
                        {s.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              ) : (
                <Input
                  value={schema}
                  onChange={(e) => setSchema(e.target.value)}
                  placeholder="Enter schema name"
                  className="font-mono"
                />
              )}
            </div>

            {/* Table Selector */}
            <div className="space-y-2">
              <Label>Table</Label>
              {!schema ? (
                <div className="flex items-center gap-2 h-10 px-3 border rounded-md bg-muted/50">
                  <span className="text-sm text-muted-foreground">Select schema first</span>
                </div>
              ) : isLoadingTables ? (
                <div className="flex items-center gap-2 h-10 px-3 border rounded-md">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  <span className="text-sm text-muted-foreground">Loading...</span>
                </div>
              ) : tables.length > 0 ? (
                <Select value={table} onValueChange={setTable}>
                  <SelectTrigger className="font-mono">
                    <SelectValue placeholder="Select table" />
                  </SelectTrigger>
                  <SelectContent>
                    {tables.map((t: { name: string }) => (
                      <SelectItem key={t.name} value={t.name} className="font-mono">
                        {t.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              ) : (
                <Input
                  value={table}
                  onChange={(e) => setTable(e.target.value)}
                  placeholder="Enter table name"
                  className="font-mono"
                />
              )}
            </div>
          </div>

          {/* Selected table preview */}
          {catalog && schema && table && (
            <div className="flex items-center gap-2 text-sm bg-muted/50 p-2 rounded-md">
              <Database className="h-4 w-4 text-muted-foreground" />
              <code className="font-mono">
                {catalog}
                <ChevronRight className="h-3 w-3 inline mx-1" />
                {schema}
                <ChevronRight className="h-3 w-3 inline mx-1" />
                {table}
              </code>
            </div>
          )}

          <Button type="submit" disabled={!catalog || !schema || !table}>
            <Search className="mr-2 h-4 w-4" />
            Load Table
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}

function ExistingProfilesList({
  catalog,
  schema,
  table,
  appCatalog,
}: {
  catalog: string;
  schema: string;
  table: string;
  appCatalog: string;
}) {
  const { data, isLoading, error } = useGetExistingProfiles(
    catalog,
    schema,
    table,
    { app_catalog: appCatalog },
    { query: { enabled: !!appCatalog } }
  );

  if (isLoading) {
    return (
      <div className="space-y-2">
        <Skeleton className="h-16 w-full" />
        <Skeleton className="h-16 w-full" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center gap-2 text-sm text-destructive">
        <AlertCircle className="h-4 w-4" />
        Failed to load existing profiles
      </div>
    );
  }

  const profiles = data?.data?.existing_profiles || [];
  
  if (profiles.length === 0) {
    return (
      <p className="text-sm text-muted-foreground py-4 text-center">
        No existing profiles for this table. Start a new profile below.
      </p>
    );
  }

  return (
    <div className="space-y-2">
      {profiles.map((profile: any) => (
        <Card key={profile.profile_id} className="bg-muted/50">
          <CardContent className="py-3 px-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <Badge
                  variant={profile.status === "completed" ? "default" : "secondary"}
                  className="flex items-center gap-1"
                >
                  {profile.status === "completed" ? (
                    <CheckCircle2 className="h-3 w-3" />
                  ) : profile.status === "running" ? (
                    <Loader2 className="h-3 w-3 animate-spin" />
                  ) : (
                    <XCircle className="h-3 w-3" />
                  )}
                  {profile.status}
                </Badge>
                <span className="text-xs font-mono text-muted-foreground">
                  {profile.profile_id?.substring(0, 8)}...
                </span>
              </div>
              <div className="flex items-center gap-4 text-xs text-muted-foreground">
                <span className="flex items-center gap-1">
                  <User className="h-3 w-3" />
                  {profile.created_by}
                </span>
                <span className="flex items-center gap-1">
                  <Clock className="h-3 w-3" />
                  {profile.created_at
                    ? new Date(profile.created_at).toLocaleDateString()
                    : "Unknown"}
                </span>
                {profile.rows_profiled && (
                  <span className="flex items-center gap-1">
                    <BarChart3 className="h-3 w-3" />
                    {profile.rows_profiled.toLocaleString()} rows
                  </span>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function ProfileTableSection({
  catalog,
  schema,
  table,
  appCatalog,
}: {
  catalog: string;
  schema: string;
  table: string;
  appCatalog: string;
}) {
  const [sampleLimit, setSampleLimit] = useState(50000);
  const [isProfiling, setIsProfiling] = useState(false);
  const [profileResult, setProfileResult] = useState<{
    status: string;
    profile_id?: string;
    message?: string;
    rows_profiled?: number;
    columns_profiled?: number;
    duration_seconds?: number;
  } | null>(null);
  const [generatedRules, setGeneratedRules] = useState<GeneratedRule[]>([]);
  const [selectedRules, setSelectedRules] = useState<Set<number>>(new Set());
  const [isSavingRules, setIsSavingRules] = useState(false);
  const queryClient = useQueryClient();

  const { mutate: profileTable } = useProfileTable();
  const { mutate: createRule } = useCreateRule();

  const handleStartProfile = () => {
    setIsProfiling(true);
    setGeneratedRules([]);
    setSelectedRules(new Set());
    setProfileResult({ status: "running", message: "Starting profiler..." });
    
    profileTable(
      {
        params: { app_catalog: appCatalog },
        data: {
          catalog,
          schema_name: schema,
          table,
          sample_limit: sampleLimit,
        },
      },
      {
        onSuccess: (response) => {
          setIsProfiling(false);
          const status = response.data.status;
          const errorMsg = response.data.error;
          const rules = response.data.generated_rules || [];
          
          let message: string;
          if (status === "completed") {
            message = rules.length > 0 
              ? `Profile completed! Generated ${rules.length} rule${rules.length !== 1 ? 's' : ''}.`
              : "Profile completed successfully!";
          } else if (status === "failed") {
            message = errorMsg || "Profile failed. Check the results for details.";
          } else {
            message = "Profile submitted";
          }
          
          setProfileResult({
            status,
            profile_id: response.data.profile_id,
            message,
            rows_profiled: response.data.rows_profiled ?? undefined,
            columns_profiled: response.data.columns_profiled ?? undefined,
            duration_seconds: response.data.duration_seconds ?? undefined,
          });
          
          if (rules.length > 0) {
            setGeneratedRules(rules);
            // Select all rules by default
            setSelectedRules(new Set(rules.map((_, i) => i)));
          }
          
          if (status === "completed") {
            toast.success(message);
          } else if (status === "failed") {
            toast.error(errorMsg || "Profile failed. Check the results for details.");
          }
          
          queryClient.invalidateQueries({
            queryKey: [`/api/tables/${catalog}/${schema}/${table}/profiles`],
          });
        },
        onError: (error) => {
          setIsProfiling(false);
          const msg = (error as any).response?.data?.detail || error.message;
          setProfileResult({
            status: "error",
            message: "Failed to start profile: " + msg,
          });
          toast.error("Failed to start profile: " + msg);
        },
      }
    );
  };

  const handleRuleToggle = (index: number, checked: boolean) => {
    const newSelected = new Set(selectedRules);
    if (checked) {
      newSelected.add(index);
    } else {
      newSelected.delete(index);
    }
    setSelectedRules(newSelected);
  };

  const handleSelectAll = () => {
    if (selectedRules.size === generatedRules.length) {
      setSelectedRules(new Set());
    } else {
      setSelectedRules(new Set(generatedRules.map((_, i) => i)));
    }
  };

  const handleSaveSelectedRules = async () => {
    const rulesToSave = generatedRules.filter((_, i) => selectedRules.has(i));
    if (rulesToSave.length === 0) {
      toast.error("Please select at least one rule to save");
      return;
    }

    setIsSavingRules(true);
    const tableFqn = `${catalog}.${schema}.${table}`;
    
    let savedCount = 0;
    let errorCount = 0;

    for (const rule of rulesToSave) {
      try {
        await new Promise<void>((resolve, reject) => {
          createRule(
            {
              params: { app_catalog: appCatalog },
              data: {
                source_catalog: catalog,
                source_schema: schema,
                source_table: table,
                rules: [{
                  name: rule.name,
                  check: {
                    function: rule.check_type,
                    arguments: rule.params || {},
                  },
                  column: rule.column,
                  criticality: rule.criticality,
                }],
              },
            },
            {
              onSuccess: () => {
                savedCount++;
                resolve();
              },
              onError: (err) => {
                errorCount++;
                resolve(); // Continue with other rules
              },
            }
          );
        });
      } catch (e) {
        errorCount++;
      }
    }

    setIsSavingRules(false);
    
    if (savedCount > 0) {
      toast.success(`Saved ${savedCount} rule${savedCount !== 1 ? 's' : ''} successfully!`);
      setGeneratedRules([]);
      setSelectedRules(new Set());
      // Refresh rules list
      queryClient.invalidateQueries({ queryKey: ["/api/rules"] });
    }
    if (errorCount > 0) {
      toast.error(`Failed to save ${errorCount} rule${errorCount !== 1 ? 's' : ''}`);
    }
  };

  const tableFqn = `${catalog}.${schema}.${table}`;

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="flex items-center gap-2">
              <Database className="h-5 w-5" />
              {tableFqn}
            </CardTitle>
            <CardDescription>
              View existing profiles or start a new analysis
            </CardDescription>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div>
          <h4 className="text-sm font-medium mb-2">Existing Profiles</h4>
          <ExistingProfilesList
            catalog={catalog}
            schema={schema}
            table={table}
            appCatalog={appCatalog}
          />
        </div>

        <Separator />

        <div className="space-y-4">
          <h4 className="text-sm font-medium">Start New Profile</h4>
          <div className="flex items-end gap-4">
            <div className="space-y-2">
              <Label htmlFor="sample-limit">Sample Limit (rows)</Label>
              <Input
                id="sample-limit"
                type="number"
                value={sampleLimit}
                onChange={(e) => setSampleLimit(parseInt(e.target.value) || 50000)}
                className="w-40"
              />
            </div>
            <Button
              onClick={handleStartProfile}
              disabled={isProfiling}
            >
              {isProfiling ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Profiling...
                </>
              ) : (
                <>
                  <Play className="mr-2 h-4 w-4" />
                  Start Profile
                </>
              )}
            </Button>
          </div>
          
          {/* Profile Status Display */}
          {profileResult && (
            <div className={`flex items-center gap-2 p-3 rounded-md ${
              profileResult.status === "running" 
                ? "bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800"
                : profileResult.status === "completed"
                ? "bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800"
                : profileResult.status === "failed" || profileResult.status === "error"
                ? "bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800"
                : "bg-muted"
            }`}>
              {profileResult.status === "running" && (
                <Loader2 className="h-4 w-4 animate-spin text-blue-600" />
              )}
              {profileResult.status === "completed" && (
                <CheckCircle2 className="h-4 w-4 text-green-600" />
              )}
              {(profileResult.status === "failed" || profileResult.status === "error") && (
                <XCircle className="h-4 w-4 text-red-600" />
              )}
              <div className="flex-1">
                <p className={`text-sm font-medium ${
                  profileResult.status === "running" ? "text-blue-700 dark:text-blue-300"
                  : profileResult.status === "completed" ? "text-green-700 dark:text-green-300"
                  : "text-red-700 dark:text-red-300"
                }`}>
                  {profileResult.message}
                </p>
                <div className="flex gap-4 text-xs text-muted-foreground mt-0.5">
                  {profileResult.profile_id && (
                    <span>ID: <code className="font-mono">{profileResult.profile_id.substring(0, 8)}...</code></span>
                  )}
                  {profileResult.rows_profiled !== undefined && (
                    <span>{profileResult.rows_profiled.toLocaleString()} rows</span>
                  )}
                  {profileResult.columns_profiled !== undefined && (
                    <span>{profileResult.columns_profiled} columns</span>
                  )}
                  {profileResult.duration_seconds !== undefined && (
                    <span>{profileResult.duration_seconds.toFixed(1)}s</span>
                  )}
                </div>
              </div>
            </div>
          )}
          
          <p className="text-xs text-muted-foreground">
            Creates a temporary view with your permissions and profiles the sampled data
          </p>
        </div>

        {/* Generated Rules Section */}
        {generatedRules.length > 0 && (
          <>
            <Separator />
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <h4 className="text-sm font-medium flex items-center gap-2">
                  <ListChecks className="h-4 w-4" />
                  Generated Rules ({generatedRules.length})
                </h4>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleSelectAll}
                  >
                    {selectedRules.size === generatedRules.length ? "Deselect All" : "Select All"}
                  </Button>
                  <Button
                    size="sm"
                    onClick={handleSaveSelectedRules}
                    disabled={selectedRules.size === 0 || isSavingRules}
                  >
                    {isSavingRules ? (
                      <>
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        Saving...
                      </>
                    ) : (
                      <>
                        <Save className="mr-2 h-4 w-4" />
                        Save Selected ({selectedRules.size})
                      </>
                    )}
                  </Button>
                </div>
              </div>
              
              <div className="space-y-2 max-h-96 overflow-y-auto">
                {generatedRules.map((rule, index) => (
                  <div
                    key={index}
                    className={`flex items-start gap-3 p-3 rounded-md border transition-colors ${
                      selectedRules.has(index)
                        ? "bg-primary/5 border-primary/30"
                        : "bg-muted/30 border-border"
                    }`}
                  >
                    <Checkbox
                      checked={selectedRules.has(index)}
                      onCheckedChange={(checked) => handleRuleToggle(index, checked as boolean)}
                      className="mt-0.5"
                    />
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="font-medium text-sm">{rule.name}</span>
                        <Badge variant="secondary" className="text-xs">
                          {rule.check_type}
                        </Badge>
                        <Badge variant="outline" className="text-xs font-mono">
                          {rule.column}
                        </Badge>
                        <Badge 
                          variant={rule.criticality === "error" ? "destructive" : "secondary"}
                          className="text-xs"
                        >
                          {rule.criticality}
                        </Badge>
                      </div>
                      {rule.params && Object.keys(rule.params).length > 0 && (
                        <div className="mt-1 text-xs text-muted-foreground font-mono">
                          {Object.entries(rule.params).map(([key, value]) => (
                            <span key={key} className="mr-2">
                              {key}: {JSON.stringify(value)}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
              
              <p className="text-xs text-muted-foreground">
                Select the rules you want to save. Saved rules will appear in the Rules page for approval and export.
              </p>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}

const STORAGE_KEY = "dqx_app_catalog";

function ProfilerPage() {
  const [selectedTable, setSelectedTable] = useState<{
    catalog: string;
    schema: string;
    table: string;
  } | null>(null);

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
        <PageBreadcrumb page="Profiler" />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            <ShinyText text="Table Profiler" speed={6} className="font-bold" />
          </h1>
          <p className="text-muted-foreground">
            Analyze table structure and generate quality rule suggestions
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
        <TableSelector
          onTableSelect={(catalog, schema, table) =>
            setSelectedTable({ catalog, schema, table })
          }
        />
      </FadeIn>

      {selectedTable && (
        <FadeIn delay={0.2}>
          <QueryErrorResetBoundary>
            {({ reset }) => (
              <ErrorBoundary
                onReset={reset}
                fallbackRender={({ resetErrorBoundary }) => (
                  <Card className="border-destructive/50">
                    <CardContent className="pt-4">
                      <div className="flex items-center gap-2">
                        <AlertCircle className="h-4 w-4 text-destructive" />
                        <span className="text-sm">Failed to load table information</span>
                        <Button variant="outline" size="sm" onClick={resetErrorBoundary}>
                          Retry
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                )}
              >
                <ProfileTableSection
                  {...selectedTable}
                  appCatalog={appCatalog}
                />
              </ErrorBoundary>
            )}
          </QueryErrorResetBoundary>
        </FadeIn>
      )}
    </div>
  );
}
