import { createFileRoute } from "@tanstack/react-router";
import { Suspense } from "react";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useConfigSuspense } from "@/hooks/use-suspense-queries";
import selector from "@/lib/selector";
import type { ConfigOut } from "@/lib/api";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Separator } from "@/components/ui/separator";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  AlertCircle,
  Settings,
  Database,
  Server,
  Activity,
  Package,
  UploadCloud,
} from "lucide-react";
import { FadeIn } from "@/components/anim/FadeIn";
import { ShinyText } from "@/components/anim/ShinyText";

export const Route = createFileRoute("/_sidebar/config")({
  component: () => <ConfigPage />,
});

function GeneralSettingsData() {
  const { data } = useConfigSuspense(selector<ConfigOut>());
  const { config } = data as ConfigOut;

  return (
    <div className="grid gap-4 md:grid-cols-3">
      <div>
        <p className="text-sm font-medium text-muted-foreground">Log Level</p>
        <p className="text-lg font-mono">{config.log_level || "INFO"}</p>
      </div>
      <div>
        <p className="text-sm font-medium text-muted-foreground">
          Serverless Clusters
        </p>
        <div className="flex items-center gap-2">
          <Badge variant={config.serverless_clusters ? "default" : "secondary"}>
            {config.serverless_clusters ? "Enabled" : "Disabled"}
          </Badge>
        </div>
      </div>
      <div>
        <p className="text-sm font-medium text-muted-foreground">
          Upload Dependencies
        </p>
        <div className="flex items-center gap-2">
          <Badge variant={config.upload_dependencies ? "default" : "secondary"}>
            {config.upload_dependencies ? "Enabled" : "Disabled"}
          </Badge>
          {config.upload_dependencies && (
            <UploadCloud className="h-4 w-4 text-blue-500" />
          )}
        </div>
      </div>
      <div>
        <p className="text-sm font-medium text-muted-foreground">
          Profiler Parallelism
        </p>
        <p className="text-lg">
          {config.profiler_max_parallelism || "Default"}
        </p>
      </div>
      <div>
        <p className="text-sm font-medium text-muted-foreground">
          Quality Checker Parallelism
        </p>
        <p className="text-lg">
          {config.quality_checker_max_parallelism || "Default"}
        </p>
      </div>
      {config.custom_metrics && config.custom_metrics.length > 0 && (
        <div className="col-span-full">
          <p className="text-sm font-medium text-muted-foreground mb-1">
            Custom Metrics
          </p>
          <div className="flex flex-wrap gap-1">
            {config.custom_metrics.map((metric) => (
              <Badge
                key={metric}
                variant="outline"
                className="font-mono text-xs"
              >
                {metric}
              </Badge>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function RunConfigsData() {
  const { data } = useConfigSuspense(selector<ConfigOut>());
  const { config } = data as ConfigOut;

  if (!config.run_configs || config.run_configs.length === 0) {
    return (
      <p className="text-muted-foreground text-sm">
        No run configurations defined.
      </p>
    );
  }

  return (
    <div className="space-y-4">
      {config.run_configs.map((runConfig, index) => (
        <Card
          key={index}
          className="bg-card/50 backdrop-blur-sm border-border/50 transition-all hover:border-primary/20 hover:shadow-sm"
        >
          <CardContent className="grid gap-2 p-4">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold text-lg">
                {runConfig.name || `Config ${index + 1}`}
              </h3>
              <div className="flex gap-2">
                {runConfig.warehouse_id && (
                  <Badge
                    variant="outline"
                    className="font-mono text-xs flex items-center gap-1 bg-background/50"
                  >
                    <Server className="h-3 w-3" />
                    {runConfig.warehouse_id}
                  </Badge>
                )}
                {runConfig.lakebase_instance_name && (
                  <Badge
                    variant="secondary"
                    className="font-mono text-xs flex items-center gap-1"
                  >
                    <Database className="h-3 w-3" />
                    {runConfig.lakebase_instance_name}
                  </Badge>
                )}
              </div>
            </div>

            <Separator className="my-2 opacity-50" />

            <div className="grid md:grid-cols-2 gap-4 text-sm">
              <div className="space-y-1">
                <span className="font-medium text-muted-foreground flex items-center gap-1">
                  <Package className="h-3 w-3" /> Input
                </span>
                <div className="pl-4 border-l-2 border-primary/20">
                  <p>
                    Location:{" "}
                    <span className="font-mono text-xs">
                      {runConfig.input_config?.location}
                    </span>
                  </p>
                  {runConfig.input_config?.format && (
                    <p>Format: {runConfig.input_config.format}</p>
                  )}
                </div>
              </div>

              <div className="space-y-1">
                <span className="font-medium text-muted-foreground flex items-center gap-1">
                  <Database className="h-3 w-3" /> Output
                </span>
                <div className="pl-4 border-l-2 border-primary/20">
                  <p>
                    Location:{" "}
                    <span className="font-mono text-xs">
                      {runConfig.output_config?.location}
                    </span>
                  </p>
                  {runConfig.output_config?.mode && (
                    <p>Mode: {runConfig.output_config.mode}</p>
                  )}
                </div>
              </div>
            </div>

            {runConfig.checks_location && (
              <div className="text-sm mt-2 pt-2 border-t border-border/30">
                <span className="font-medium text-muted-foreground">
                  Checks Location:{" "}
                </span>
                <span className="font-mono text-xs bg-muted/50 px-1 py-0.5 rounded">
                  {runConfig.checks_location}
                </span>
              </div>
            )}
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function ClusterOverridesCard() {
  const { data } = useConfigSuspense(selector<ConfigOut>());
  const { config } = data as ConfigOut;

  if (
    !config.profiler_override_clusters &&
    !config.quality_checker_override_clusters &&
    !config.e2e_override_clusters
  ) {
    return null;
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Server className="h-5 w-5" />
          Cluster Overrides
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {config.profiler_override_clusters && (
          <div>
            <h4 className="font-medium mb-2">Profiler</h4>
            <pre className="bg-muted p-2 rounded-md text-xs overflow-x-auto">
              {JSON.stringify(config.profiler_override_clusters, null, 2)}
            </pre>
          </div>
        )}
        {config.quality_checker_override_clusters && (
          <div>
            <h4 className="font-medium mb-2">Quality Checker</h4>
            <pre className="bg-muted p-2 rounded-md text-xs overflow-x-auto">
              {JSON.stringify(
                config.quality_checker_override_clusters,
                null,
                2,
              )}
            </pre>
          </div>
        )}
        {config.e2e_override_clusters && (
          <div>
            <h4 className="font-medium mb-2">E2E Tests</h4>
            <pre className="bg-muted p-2 rounded-md text-xs overflow-x-auto">
              {JSON.stringify(config.e2e_override_clusters, null, 2)}
            </pre>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function RunConfigCount() {
  const { data } = useConfigSuspense(selector<ConfigOut>());
  const { config } = data as ConfigOut;
  return <>({config.run_configs?.length || 0})</>;
}

function SectionError({
  resetErrorBoundary,
}: {
  resetErrorBoundary: () => void;
}) {
  return (
    <div className="flex flex-col gap-2 items-start">
      <p className="text-sm text-destructive flex items-center gap-1">
        <AlertCircle className="h-4 w-4" /> Failed to load section
      </p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary}>
        Retry
      </Button>
    </div>
  );
}

function ConfigPage() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb page="Configuration" />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            <ShinyText text="Configuration" speed={6} className="font-bold" />
          </h1>
          <p className="text-muted-foreground">
            View your current workspace configuration.
          </p>
        </div>
      </div>

      <QueryErrorResetBoundary>
        {({ reset }) => (
          <div className="space-y-6 pb-8">
            <FadeIn delay={0.1}>
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Settings className="h-5 w-5" />
                    General Settings
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <ErrorBoundary
                    onReset={reset}
                    fallbackRender={SectionError}
                  >
                    <Suspense
                      fallback={
                        <div className="grid gap-4 md:grid-cols-3">
                          <Skeleton className="h-16" />
                          <Skeleton className="h-16" />
                          <Skeleton className="h-16" />
                          <Skeleton className="h-16" />
                          <Skeleton className="h-16" />
                          <Skeleton className="h-16" />
                        </div>
                      }
                    >
                      <GeneralSettingsData />
                    </Suspense>
                  </ErrorBoundary>
                </CardContent>
              </Card>
            </FadeIn>

            <FadeIn delay={0.2}>
              <Card className="border-0 shadow-none bg-transparent">
                <CardHeader className="px-0">
                  <CardTitle className="flex items-center gap-2">
                    <Activity className="h-5 w-5" />
                    Run Configurations
                    <ErrorBoundary onReset={reset} fallback={null}>
                      <Suspense
                        fallback={
                          <Skeleton className="h-4 w-8 inline-block ml-2" />
                        }
                      >
                        <span className="ml-1 text-sm font-normal text-muted-foreground">
                          <RunConfigCount />
                        </span>
                      </Suspense>
                    </ErrorBoundary>
                  </CardTitle>
                  <CardDescription>
                    Defined run configurations
                  </CardDescription>
                </CardHeader>
                <CardContent className="px-0">
                  <ErrorBoundary
                    onReset={reset}
                    fallbackRender={SectionError}
                  >
                    <Suspense
                      fallback={
                        <div className="space-y-4">
                          <Skeleton className="h-32 w-full" />
                          <Skeleton className="h-32 w-full" />
                        </div>
                      }
                    >
                      <RunConfigsData />
                    </Suspense>
                  </ErrorBoundary>
                </CardContent>
              </Card>
            </FadeIn>

            <ErrorBoundary onReset={reset} fallback={null}>
              <Suspense fallback={null}>
                <FadeIn delay={0.3}>
                  <ClusterOverridesCard />
                </FadeIn>
              </Suspense>
            </ErrorBoundary>
          </div>
        )}
      </QueryErrorResetBoundary>
    </div>
  );
}
