/**
 * Session handoff for the Describe-a-rule → Create Rule flow.
 *
 * The AI proposal is too large/fragile for a URL search param, so it travels
 * via sessionStorage. The create page reads + clears it on mount and passes
 * it into RegistryRuleFormDialog as `initialAiProposal`.
 */

import type { AiGenerateRuleOut } from "@/lib/api";

export const DESCRIBE_RULE_PROPOSAL_KEY = "dqx:describe-rule:proposal";

export function storeDescribeRuleProposal(proposal: AiGenerateRuleOut): void {
  try {
    sessionStorage.setItem(DESCRIBE_RULE_PROPOSAL_KEY, JSON.stringify(proposal));
  } catch {
    // Quota / private mode — create page will just open empty.
  }
}

export function takeDescribeRuleProposal(): AiGenerateRuleOut | null {
  try {
    const raw = sessionStorage.getItem(DESCRIBE_RULE_PROPOSAL_KEY);
    if (!raw) return null;
    sessionStorage.removeItem(DESCRIBE_RULE_PROPOSAL_KEY);
    const parsed = JSON.parse(raw) as AiGenerateRuleOut;
    if (!parsed || typeof parsed !== "object" || typeof parsed.name !== "string") {
      return null;
    }
    return parsed;
  } catch {
    try {
      sessionStorage.removeItem(DESCRIBE_RULE_PROPOSAL_KEY);
    } catch {
      /* ignore */
    }
    return null;
  }
}

/** Allowlist returnTo paths that come from the table Apply Rules tab. */
export function isAllowedReturnTo(returnTo: string | undefined | null): returnTo is string {
  if (!returnTo || typeof returnTo !== "string") return false;
  // Must be a same-origin relative path under monitored-tables.
  if (!returnTo.startsWith("/monitored-tables/")) return false;
  if (returnTo.includes("://") || returnTo.includes("//")) return false;
  return true;
}

/** Parse an allowlisted returnTo into a TanStack navigate target. */
export function parseMonitoredTableReturnTo(
  returnTo: string,
): { bindingId: string; search: Record<string, string> } | null {
  if (!isAllowedReturnTo(returnTo)) return null;
  const match = returnTo.match(/^\/monitored-tables\/([^/?]+)(?:\?(.*))?$/);
  if (!match) return null;
  const bindingId = decodeURIComponent(match[1]);
  if (!bindingId) return null;
  const search = Object.fromEntries(new URLSearchParams(match[2] || ""));
  return { bindingId, search };
}
