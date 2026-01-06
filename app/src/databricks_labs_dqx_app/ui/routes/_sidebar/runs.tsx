import {
  createFileRoute,
  Link,
  useNavigate,
  useParams,
} from "@tanstack/react-router";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  useConfigSuspense,
  useConfig,
  useSaveRunConfig,
  useDeleteRunConfig,
  RunConfig,
} from "@/lib/api";
import selector from "@/lib/selector";
import { Button } from "@/components/ui/button";
import {
  Plus,
  Trash2,
  Save,
  FileCode,
  AlertCircle,
  RotateCcw,
  Loader2,
} from "lucide-react";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";
import { toast } from "sonner";
import { useState, useEffect, Suspense } from "react";
import yaml from "js-yaml";
import { Skeleton } from "@/components/ui/skeleton";
import { useQueryClient } from "@tanstack/react-query";

export const Route = createFileRoute("/_sidebar/runs")({
  component: RunsPage,
});

function RunsPage() {
  const navigate = useNavigate();
  const params = useParams({ strict: false }) as { runName?: string };
  const currentRunName = params.runName;
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [isDeletingRun, setIsDeletingRun] = useState(false);
  const queryClient = useQueryClient();

  // We use the non-suspense hook here just to get data for the "Add Run" logic
  // so the header doesn't suspend.
  const { data: configData } = useConfig(undefined, selector());
  const { mutateAsync: saveRun } = useSaveRunConfig();

  // Derived from configData if available, or empty list
  const existingRunNames =
    configData?.config?.run_configs?.map((r) => r.name || "") || [];

  const handleCreateRun = async (name: string) => {
    const newRun: RunConfig = {
      name: name,
      // Input and Output config are omitted to avoid unnecessary defaults
    };

    try {
      await saveRun({ data: { config: newRun } });
      // Refetch all config queries to ensure consistency
      await queryClient.refetchQueries({ queryKey: ["/api/config"] });
      toast.success(`Run "${name}" created`);
      navigate({ to: "/runs/$runName", params: { runName: name } });
      setIsCreateOpen(false);
    } catch (error) {
      toast.error("Failed to create new run");
      console.error(error);
      throw error;
    }
  };

  return (
    <div className="flex flex-col h-full">
      <PageBreadcrumb
        items={currentRunName ? [{ label: "Runs", to: "/runs" }] : []}
        page={currentRunName || "Runs"}
      />

      <div className="flex flex-1 gap-6 mt-4 overflow-hidden">
        {/* Sidebar */}
        <aside className="w-72 shrink-0 flex flex-col border-r border-border/50 pr-4 overflow-hidden">
          <div className="flex items-center justify-between mb-4 shrink-0">
            <h2 className="font-semibold text-lg text-foreground">
              Run Configurations
            </h2>
            <Button
              variant="outline"
              size="icon"
              onClick={() => setIsCreateOpen(true)}
              disabled={!configData}
              title="Add New Run"
              className="h-9 w-9"
            >
              <Plus className="h-4 w-4" />
            </Button>
          </div>

          <div className="flex-1 overflow-y-auto space-y-1">
            <Suspense fallback={<RunsListSkeleton />}>
              <RunsSidebarList
                currentRunName={currentRunName}
                isDeleting={isDeletingRun}
              />
            </Suspense>
          </div>
        </aside>

        {/* Content Area */}
        <main className="flex-1 overflow-hidden flex flex-col">
          <Suspense fallback={<RunEditorSkeleton />}>
            <RunEditorContainer
              currentRunName={currentRunName}
              onAddRun={() => setIsCreateOpen(true)}
              onDeletingChange={setIsDeletingRun}
            />
          </Suspense>
        </main>
      </div>

      <CreateRunDialog
        open={isCreateOpen}
        onOpenChange={setIsCreateOpen}
        existingNames={existingRunNames}
        onCreate={handleCreateRun}
      />
    </div>
  );
}

function CreateRunDialog({
  open,
  onOpenChange,
  existingNames,
  onCreate,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  existingNames: string[];
  onCreate: (name: string) => Promise<void>;
}) {
  const [name, setName] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Reset state when opening
  useEffect(() => {
    if (open) {
      setName("");
      setIsSubmitting(false);
    }
  }, [open]);

  const isConflict = existingNames.includes(name);
  const isValid = name.trim().length > 0 && !isConflict;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!isValid) return;

    setIsSubmitting(true);
    try {
      await onCreate(name);
    } catch (e) {
      setIsSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle>Create New Run</DialogTitle>
          <DialogDescription>
            Enter a unique name for the new run configuration.
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={handleSubmit}>
          <div className="grid gap-4 py-4">
            <div className="grid gap-2">
              <Label htmlFor="name">Name</Label>
              <Input
                id="name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                className={cn(
                  isConflict &&
                    "border-destructive focus-visible:ring-destructive",
                )}
                placeholder="e.g. daily_sales_check"
                autoFocus
                autoComplete="off"
              />
              {isConflict && (
                <p className="text-xs text-destructive">
                  This run name already exists.
                </p>
              )}
            </div>
          </div>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={!isValid || isSubmitting}>
              {isSubmitting ? (
                <Loader2 className="h-4 w-4 animate-spin mr-2" />
              ) : null}
              Create
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

function RunsListSkeleton() {
  return (
    <div className="space-y-2">
      {[1, 2, 3, 4].map((i) => (
        <Skeleton key={i} className="h-10 w-full" />
      ))}
    </div>
  );
}

function RunEditorSkeleton() {
  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between pb-4 border-b border-border/50">
        <div className="space-y-2">
          <Skeleton className="h-8 w-48" />
          <Skeleton className="h-4 w-64" />
        </div>
        <div className="flex gap-2">
          <Skeleton className="h-9 w-20" />
          <Skeleton className="h-9 w-20" />
          <Skeleton className="h-9 w-9" />
        </div>
      </div>
      <div className="flex-1 mt-4">
        <Skeleton className="h-full w-full rounded-lg" />
      </div>
    </div>
  );
}

function RunsSidebarList({
  currentRunName,
  isDeleting,
}: {
  currentRunName?: string;
  isDeleting?: boolean;
}) {
  const { data: configData } = useConfigSuspense(undefined, selector());

  const runConfigs = configData.config.run_configs || [];
  const hasRuns = runConfigs.length > 0;

  if (isDeleting) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!hasRuns) {
    return (
      <div className="flex flex-col items-center justify-center py-8 text-center">
        <FileCode className="h-12 w-12 text-muted-foreground/30 mb-3" />
        <p className="text-muted-foreground text-sm mb-1">No runs configured</p>
        <p className="text-muted-foreground/70 text-xs">
          Click the + button to create your first run
        </p>
      </div>
    );
  }

  return (
    <>
      {runConfigs.map((run) => (
        <div
          key={run.name}
          className={cn(
            "group flex items-center gap-2 rounded-lg transition-all duration-200",
            currentRunName === run.name
              ? "bg-primary/10 ring-1 ring-primary/20"
              : "hover:bg-muted/50",
          )}
        >
          <Link
            to="/runs/$runName"
            params={{ runName: run.name || "" }}
            className={cn(
              "flex-1 flex items-center gap-3 px-3 py-2.5 text-sm font-medium",
              currentRunName === run.name
                ? "text-primary"
                : "text-muted-foreground hover:text-foreground",
            )}
          >
            <FileCode className="h-4 w-4 shrink-0" />
            <span className="truncate">{run.name}</span>
          </Link>
        </div>
      ))}
    </>
  );
}

function RunEditorContainer({
  currentRunName,
  onAddRun,
  onDeletingChange,
}: {
  currentRunName?: string;
  onAddRun: () => void;
  onDeletingChange: (isDeleting: boolean) => void;
}) {
  const { data: configData, refetch } = useConfigSuspense(
    undefined,
    selector(),
  );
  const { mutateAsync: saveRun, isPending: isSaving } = useSaveRunConfig();
  const { mutateAsync: deleteRun, isPending: isDeleting } =
    useDeleteRunConfig();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  // New state for controlling the Delete Dialog
  const [isDeleteOpen, setIsDeleteOpen] = useState(false);

  // Notify parent when isDeleting changes
  useEffect(() => {
    onDeletingChange(isDeleting);
  }, [isDeleting, onDeletingChange]);

  const runConfigs = configData.config.run_configs || [];
  const selectedRun = currentRunName
    ? runConfigs.find((r) => r.name === currentRunName)
    : undefined;

  const hasRuns = runConfigs.length > 0;
  const runNotFound = currentRunName && !selectedRun;

  // Remove auto-select logic - let users explicitly choose a run
  // This prevents issues when deleting runs or navigating

  const [yamlContent, setYamlContent] = useState("");
  const [isDirty, setIsDirty] = useState(false);

  useEffect(() => {
    if (selectedRun) {
      try {
        const dump = yaml.dump(selectedRun);
        setYamlContent(dump);
        setIsDirty(false);
      } catch (e) {
        console.error("Error converting config to YAML", e);
        toast.error("Error parsing run configuration");
      }
    } else {
      setYamlContent("");
      setIsDirty(false);
    }
  }, [selectedRun, currentRunName]);

  const handleSave = async () => {
    if (!currentRunName) return;

    try {
      const parsedConfig = yaml.load(yamlContent) as RunConfig;

      if (typeof parsedConfig !== "object" || !parsedConfig) {
        throw new Error("Invalid YAML content");
      }

      if (!parsedConfig.name) {
        throw new Error("Run name is required");
      }

      await saveRun({ data: { config: parsedConfig } });

      toast.success("Run configuration saved successfully");
      setIsDirty(false);

      // If name changed, navigate to new URL
      if (parsedConfig.name !== currentRunName) {
        navigate({
          to: "/runs/$runName",
          params: { runName: parsedConfig.name },
        });
      }

      await refetch();
    } catch (error) {
      console.error("Save error", error);
      const message =
        error instanceof Error
          ? error.message
          : "Check YAML syntax or validation errors.";
      toast.error(`Failed to save: ${message}`);
    }
  };

  const handleDelete = async () => {
    if (!currentRunName) return;

    // Close the dialog immediately
    setIsDeleteOpen(false);

    // Store the run name before navigation
    const deletedRunName = currentRunName;

    // Optimistically update the cache to remove the run immediately
    queryClient.setQueryData(["/api/config"], (oldData: any) => {
      if (!oldData?.data?.config?.run_configs) return oldData;

      return {
        ...oldData,
        data: {
          ...oldData.data,
          config: {
            ...oldData.data.config,
            run_configs: oldData.data.config.run_configs.filter(
              (run: RunConfig) => run.name !== deletedRunName,
            ),
          },
        },
      };
    });

    // Navigate immediately after optimistic update
    navigate({ to: "/runs" });

    try {
      // Perform the actual deletion in the background
      await deleteRun({ name: deletedRunName });

      // Refetch to ensure we're in sync with the server
      await queryClient.refetchQueries({ queryKey: ["/api/config"] });

      toast.success(`Run "${deletedRunName}" deleted`);
    } catch (error) {
      console.error("Delete error", error);
      toast.error("Failed to delete run");

      // Refetch to restore correct state on error
      await queryClient.refetchQueries({ queryKey: ["/api/config"] });
    }
  };

  const handleReset = () => {
    if (selectedRun) {
      try {
        const dump = yaml.dump(selectedRun);
        setYamlContent(dump);
        setIsDirty(false);
        toast.info("Changes discarded");
      } catch (e) {
        console.error("Error converting config to YAML", e);
      }
    }
  };

  if (isDeleting) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!hasRuns) {
    return <EmptyState onAddRun={onAddRun} />;
  }

  if (runNotFound) {
    return <RunNotFoundState runName={currentRunName!} onAddRun={onAddRun} />;
  }

  if (!selectedRun) {
    return <SelectRunState />;
  }

  return (
    <RunEditor
      runName={currentRunName!}
      yamlContent={yamlContent}
      setYamlContent={setYamlContent}
      isDirty={isDirty}
      setIsDirty={setIsDirty}
      onSave={handleSave}
      onReset={handleReset}
      onDelete={handleDelete}
      isSaving={isSaving}
      isDeleting={isDeleting}
      isDeleteOpen={isDeleteOpen}
      setIsDeleteOpen={setIsDeleteOpen}
    />
  );
}

// Empty state when no runs exist
function EmptyState({ onAddRun }: { onAddRun: () => void }) {
  return (
    <div className="flex-1 flex flex-col items-center justify-center text-center p-8">
      <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mb-6">
        <FileCode className="h-8 w-8 text-primary" />
      </div>
      <h3 className="text-xl font-semibold mb-2">No Run Configurations</h3>
      <p className="text-muted-foreground mb-6 max-w-md">
        Run configurations define how DQX processes your data quality checks.
        Create your first run to get started.
      </p>
      <Button onClick={onAddRun} className="gap-2">
        <Plus className="h-4 w-4" />
        Create Your First Run
      </Button>
    </div>
  );
}

// State when URL has a run name that doesn't exist
function RunNotFoundState({
  runName,
  onAddRun,
}: {
  runName: string;
  onAddRun: () => void;
}) {
  return (
    <div className="flex-1 flex flex-col items-center justify-center text-center p-8">
      <div className="w-16 h-16 rounded-full bg-destructive/10 flex items-center justify-center mb-6">
        <AlertCircle className="h-8 w-8 text-destructive" />
      </div>
      <h3 className="text-xl font-semibold mb-2">Run Not Found</h3>
      <p className="text-muted-foreground mb-6 max-w-md">
        The run configuration{" "}
        <code className="px-1.5 py-0.5 bg-muted rounded text-sm font-mono">
          {runName}
        </code>{" "}
        does not exist. It may have been deleted or renamed.
      </p>
      <div className="flex gap-3">
        <Button variant="outline" asChild>
          <Link to="/runs">View All Runs</Link>
        </Button>
        <Button onClick={onAddRun} className="gap-2">
          <Plus className="h-4 w-4" />
          Create New Run
        </Button>
      </div>
    </div>
  );
}

// State when at /runs with runs existing but none selected
function SelectRunState() {
  return (
    <div className="flex-1 flex flex-col items-center justify-center text-center p-8">
      <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
        <FileCode className="h-8 w-8 text-muted-foreground" />
      </div>
      <h3 className="text-lg font-medium text-muted-foreground">
        Select a Run
      </h3>
      <p className="text-muted-foreground/70 text-sm mt-1">
        Choose a run configuration from the list to view and edit
      </p>
    </div>
  );
}

// YAML Editor component
interface RunEditorProps {
  runName: string;
  yamlContent: string;
  setYamlContent: (content: string) => void;
  isDirty: boolean;
  setIsDirty: (dirty: boolean) => void;
  onSave: () => void;
  onReset: () => void;
  onDelete: () => void;
  isSaving: boolean;
  isDeleting: boolean;
  isDeleteOpen: boolean;
  setIsDeleteOpen: (open: boolean) => void;
}

function RunEditor({
  runName,
  yamlContent,
  setYamlContent,
  isDirty,
  setIsDirty,
  onSave,
  onReset,
  onDelete,
  isSaving,
  isDeleting,
  isDeleteOpen,
  setIsDeleteOpen,
}: RunEditorProps) {
  const isLocked = isSaving || isDeleting;

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between pb-4 border-b border-border/50">
        <div>
          <h2 className="text-2xl font-bold tracking-tight">{runName}</h2>
          <p className="text-muted-foreground text-sm mt-0.5">
            Edit configuration in YAML format
            {isDirty && (
              <span className="text-amber-500 ml-2">• Unsaved changes</span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="icon"
            onClick={onReset}
            disabled={!isDirty || isLocked}
            title="Reset changes"
          >
            <RotateCcw className="h-4 w-4" />
          </Button>
          <Button
            onClick={onSave}
            variant="default"
            size="icon"
            disabled={!isDirty || isLocked}
            title="Save changes"
          >
            {isSaving ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Save className="h-4 w-4" />
            )}
          </Button>

          <AlertDialog open={isDeleteOpen} onOpenChange={setIsDeleteOpen}>
            <AlertDialogTrigger asChild>
              <Button
                variant="destructive"
                size="icon"
                disabled={isDeleting || isLocked}
                title="Delete Run"
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Delete Run Configuration</AlertDialogTitle>
                <AlertDialogDescription>
                  Are you sure you want to delete the run configuration{" "}
                  <span className="font-mono font-medium">{runName}</span>? This
                  action cannot be undone.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel disabled={isDeleting}>
                  Cancel
                </AlertDialogCancel>
                <AlertDialogAction
                  onClick={(e) => {
                    e.preventDefault();
                    if (isDeleting) return;
                    onDelete();
                  }}
                  disabled={isDeleting}
                  className="bg-destructive text-foreground hover:bg-destructive/90"
                >
                  {isDeleting ? (
                    <Loader2 className="h-4 w-4 animate-spin mr-2" />
                  ) : null}
                  Delete
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </div>
      </div>

      {/* YAML Editor */}
      <div className="flex-1 min-h-0 mt-4 relative">
        <div className="absolute inset-0 rounded-lg border border-border/50 bg-muted/30 overflow-hidden">
          <textarea
            value={yamlContent}
            onChange={(e) => {
              setYamlContent(e.target.value);
              setIsDirty(true);
            }}
            disabled={isLocked}
            className={cn(
              "w-full h-full resize-none p-4",
              "font-mono text-sm leading-relaxed",
              "bg-transparent focus:outline-none",
              "placeholder:text-muted-foreground/50",
              isLocked && "opacity-50 cursor-not-allowed",
            )}
            spellCheck={false}
            placeholder="# Run configuration YAML..."
          />
        </div>
      </div>
    </div>
  );
}
