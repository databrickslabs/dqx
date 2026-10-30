import { test } from "../../fixtures";
import { E2E_LABEL_KEY_PREFIX, E2E_PREFIX, E2E_REVIEW_STATUS_PREFIX } from "../../support/names";
import { ALL_SAMPLE_TABLES } from "../../support/sample-tables";

// Setup project for the journeys: removes what an aborted earlier run left
// behind so journeys start clean. Conservative on purpose: a sample table is
// only un-monitored when the current user created it and every rule applied to
// it came from this suite; anything else is reported, never deleted.

test("sweep leftovers from earlier e2e runs", async ({ studio }) => {
  const me = await studio.currentUserEmail();
  const leftoverRules = await studio.leftoverRules();
  const leftoverRuleIds = new Set(leftoverRules.map((r) => r.rule_id));

  // Settings entries the isolated journeys add (they restore them on teardown,
  // but an interrupted run or an expired token can skip that).
  const statuses = await studio.reviewStatuses();
  const keptStatuses = statuses.filter((s) => !s.value.startsWith(E2E_REVIEW_STATUS_PREFIX));
  if (keptStatuses.length !== statuses.length) await studio.setReviewStatuses(keptStatuses);
  const labels = await studio.labelDefinitions();
  const keptLabels = labels.filter((d) => !d.key.startsWith(E2E_LABEL_KEY_PREFIX));
  if (keptLabels.length !== labels.length) await studio.setLabelDefinitions(keptLabels);

  // Collections reference tables, so they go first.
  for (const collection of await studio.listCollections()) {
    if (collection.name.startsWith(E2E_PREFIX) && collection.created_by === me) {
      await studio.deleteCollection(collection.product_id);
    }
  }

  const blocked: string[] = [];
  for (const fqn of ALL_SAMPLE_TABLES) {
    const table = await studio.findMonitoredTable(fqn);
    if (!table) continue;
    const detail = await studio.getMonitoredTableDetail(table.binding_id);
    const ours = table.created_by === me && detail.applied_rules.every((a) => leftoverRuleIds.has(a.rule_id));
    if (ours) await studio.deleteMonitoredTable(table.binding_id);
    else blocked.push(fqn);
  }
  for (const rule of leftoverRules) await studio.deleteRule(rule.rule_id);

  if (blocked.length) {
    throw new Error(
      `These sample tables are already monitored on this deployment (by someone else, or with non-e2e rules), ` +
        `and the journeys need them:\n  ${blocked.join("\n  ")}\n` +
        `Remove them from Tables, or run only the smoke suite (SUITE=smoke).`,
    );
  }
});
