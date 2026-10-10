import { useSyncExternalStore } from "react";
import type { BrandingTheme } from "@/lib/branding";

/**
 * Unsaved edits shared between the Branding and Styling cards, so each card's preview shows the
 * other's changes as they are made. *undefined* means "nothing being edited; use the saved value".
 */
type PreviewState = { theme: BrandingTheme | undefined; companyName: string | undefined };

let state: PreviewState = { theme: undefined, companyName: undefined };
const listeners = new Set<() => void>();

export function publishPreview(patch: Partial<PreviewState>): void {
  state = { ...state, ...patch };
  for (const l of listeners) l();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function usePreviewState(): PreviewState {
  return useSyncExternalStore(subscribe, () => state);
}
