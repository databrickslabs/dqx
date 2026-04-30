import { ThemeProvider } from "@/components/apx/theme-provider";
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

export const Route = createRootRouteWithContext<{
  queryClient: QueryClient;
}>()({
  component: () => (
    <ThemeProvider defaultTheme="dark" storageKey="cdh-ui-theme">
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
