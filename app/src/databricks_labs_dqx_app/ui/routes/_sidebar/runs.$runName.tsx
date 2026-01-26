import { createFileRoute } from "@tanstack/react-router";

// This route exists for URL matching at /runs/$runName
// The parent runs.tsx handles all rendering
export const Route = createFileRoute("/_sidebar/runs/$runName")({
  component: () => null,
});
