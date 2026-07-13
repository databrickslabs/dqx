/**
 * Fire-and-forget row-entitlement pre-verification (P4.3).
 *
 * Genie answers failing-rows questions through the entitlement-gated
 * `v_dq_failing_rows` dynamic view (P4.1): rows appear only for source
 * tables the CALLING user self-verified SELECT on within the TTL window.
 * To solve the cold start — a user opening the chat and asking about a
 * table they have never verified — the results surfaces pre-verify the
 * tables already on screen by fire-and-forgetting
 * `POST /api/v1/genie/verify-entitlements`:
 *
 * - `GenieChatProvider` mount (`components/results/AskGenieButton.tsx`):
 *   the context table FQN (table surface) or the product's member FQNs
 *   (product surface — the provider already receives them for the sent
 *   preamble).
 * - The global Results page (`routes/_sidebar/results.tsx`): the first
 *   {@link BY_TABLE_PREVERIFY_LIMIT} FQNs of its base by-table rows.
 *
 * The call is invisible plumbing: silent failure, no UI blocking, no
 * retries. A module-level "already attempted this session" set (the same
 * module-state idiom as `genieConversationStore` / `results-invalidation`)
 * dedupes so each FQN is sent at most once per browser session — the
 * backend's own 24h-TTL freshness skip makes a re-send after a reload
 * cheap anyway.
 */
import { verifyGenieEntitlements } from "@/lib/api";

/** Server-side request cap (`GenieVerifyEntitlementsIn.table_fqns` /
 *  `entitlement_service.VERIFY_ENTITLEMENTS_MAX_FQNS`). A larger batch
 *  would be rejected with a 422, so the client never sends more. */
export const VERIFY_REQUEST_MAX_FQNS = 50;

/** How many by-table rows the global Results page pre-verifies — the first
 *  screenful, per the Phase 4 plan ("global page: cap the first 25"). */
export const BY_TABLE_PREVERIFY_LIMIT = 25;

/** Synthetic FQN prefix of cross-table SQL checks — not a real table, so
 *  never worth an entitlement probe. */
const SQL_CHECK_PREFIX = "__sql_check__/";

/**
 * True for a plain unquoted three-part `catalog.schema.table` name — the
 * ONLY format worth verifying. The runner writes
 * `dq_quarantine_records.source_table_fqn` verbatim from the binding's
 * plain FQN, and the gated view matches entitlement rows against it with
 * a string equality, so anything else (synthetic `__sql_check__/` keys,
 * backtick-quoted parts that would be stored quoted and never match)
 * could not open any rows even if the backend accepted it.
 */
export function isPlainTableFqn(fqn: string): boolean {
  if (fqn.startsWith(SQL_CHECK_PREFIX)) return false;
  if (fqn.includes("`")) return false;
  const parts = fqn.split(".");
  return parts.length === 3 && parts.every((p) => p.trim().length > 0);
}

/**
 * Build the FQN batch for one verify call, in candidate order: drop
 * null/blank entries and non-plain FQNs (see {@link isPlainTableFqn}),
 * keep the first occurrence of each FQN, skip everything already
 * attempted this session, and cap at *limit* (never above the server's
 * request cap). Pure — the session set is passed in, not read.
 */
export function selectFqnsToVerify(
  candidates: readonly (string | null | undefined)[],
  alreadyAttempted: ReadonlySet<string>,
  limit: number = VERIFY_REQUEST_MAX_FQNS,
): string[] {
  const cap = Math.min(limit, VERIFY_REQUEST_MAX_FQNS);
  const picked: string[] = [];
  const seen = new Set<string>();
  for (const candidate of candidates) {
    if (picked.length >= cap) break;
    if (!candidate) continue;
    if (seen.has(candidate) || alreadyAttempted.has(candidate)) continue;
    seen.add(candidate);
    if (!isPlainTableFqn(candidate)) continue;
    picked.push(candidate);
  }
  return picked;
}

/** FQNs already sent to verify-entitlements this session. Attempted — not
 *  "verified": outcomes are ignored (fire-and-forget, no retries), so one
 *  attempt per FQN per session is the contract. */
const attemptedThisSession = new Set<string>();

/**
 * Fire-and-forget pre-verification of the caller's row-level access for
 * the given tables. Skips FQNs already attempted this session, skips
 * entirely when nothing new remains, never blocks and never surfaces a
 * failure — a missed verification only means Genie's failing-rows view
 * stays closed until the user triggers another surface (or the in-app
 * failed-rows endpoint's own verification piggyback covers it).
 */
export function preverifyRowEntitlements(
  candidates: readonly (string | null | undefined)[],
  limit: number = VERIFY_REQUEST_MAX_FQNS,
): void {
  const fqns = selectFqnsToVerify(candidates, attemptedThisSession, limit);
  if (fqns.length === 0) return;
  for (const fqn of fqns) attemptedThisSession.add(fqn);
  void verifyGenieEntitlements({ table_fqns: fqns }).catch(() => {
    // Fire-and-forget: verification failures are per-FQN outcomes the UI
    // deliberately ignores; a network failure just leaves the view closed.
  });
}

/** Test-only: clear the per-session dedupe set. */
export function resetPreverifiedThisSession(): void {
  attemptedThisSession.clear();
}
