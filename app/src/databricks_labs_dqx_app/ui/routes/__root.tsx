import { ThemeProvider } from "@/components/layout/theme-provider";
import { AIAssistantProvider } from "@/components/AIAssistantProvider";
import { QueryClient } from "@tanstack/react-query";
import { createRootRouteWithContext, Outlet } from "@tanstack/react-router";
import { TanStackRouterDevtools } from "@tanstack/react-router-devtools";
import { Toaster } from "sonner";
import { useEffect } from "react";
import { useTimezone } from "@/lib/api-custom";
import { setDisplayTimezone } from "@/lib/format-utils";

function TimezoneSync() {
  const { data: tz } = useTimezone();
  const timezone = (tz as { timezone: string } | undefined)?.timezone;
  useEffect(() => {
    if (timezone) setDisplayTimezone(timezone);
  }, [timezone]);
  return null;
}

/**
 * App-level SVG defs. Reusable gradients that any inline SVG icon can fill
 * or stroke with `AI_GRADIENT_URL` from `@/lib/ai-style`.
 */
function AppSvgDefs() {
  return (
    <svg
      width="0"
      height="0"
      style={{ position: "absolute", pointerEvents: "none" }}
      aria-hidden="true"
    >
      <defs>
        <linearGradient id="dqx-ai-gradient" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="#8b5cf6" />
          <stop offset="50%" stopColor="#d946ef" />
          <stop offset="100%" stopColor="#ec4899" />
        </linearGradient>
      </defs>
    </svg>
  );
}

export const Route = createRootRouteWithContext<{
  queryClient: QueryClient;
}>()({
  component: () => (
    <ThemeProvider defaultTheme="dark" storageKey="cdh-ui-theme">
      <AppSvgDefs />
      <TimezoneSync />
      <AIAssistantProvider>
        <Outlet />
      </AIAssistantProvider>
      <Toaster richColors />
      {import.meta.env.DEV && (
        <TanStackRouterDevtools position="bottom-right" />
      )}
    </ThemeProvider>
  ),
});
