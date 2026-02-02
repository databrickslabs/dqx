import { createFileRoute } from "@tanstack/react-router";

// This route exists for URL matching at /runs
// The parent runs.tsx handles all rendering
export const Route = createFileRoute("/_sidebar/runs/")({
  component: () => null,
});
