/**
 * Pure data-state logic for the rule-level Results view (registry rule
 * detail page) — extracted so it is unit-testable under `bun test`
 * without mounting components (this repo has no component-test
 * infrastructure); consumers (RuleResultsTab, RegistryRuleFormDialog)
 * stay thin JSX shells over this helper.
 */
import type { TableScoreOut } from "@/lib/api";

/**
 * Data states of the rule-level Results view (registry rule detail page).
 *
 * `applied_to_count` is the TOTAL number of current applications across all
 * monitored tables (viewer-independent), while `per_table` only contains the
 * applied tables the current viewer can access — so the two disagree
 * legitimately:
 *
 * - "not-applied": zero current applications anywhere. The Results tab is
 *   disabled with an explanatory tooltip in this state.
 * - "no-access":   applied somewhere, but every applied table is outside the
 *   viewer's access — show an explanatory empty state, NOT "not applied".
 * - "has-data":    the viewer can see at least one applied table's score.
 */
export type RuleResultsState = "not-applied" | "no-access" | "has-data";

export function ruleResultsState(score: {
  applied_to_count?: number;
  per_table?: TableScoreOut[];
}): RuleResultsState {
  if ((score.applied_to_count ?? 0) === 0) return "not-applied";
  if ((score.per_table?.length ?? 0) === 0) return "no-access";
  return "has-data";
}
