/** The parts of a monitored table's state that decide whether "Submit for review" has anything to send. */
export interface SubmitGateState {
  /** The editor holds edits that are not saved yet. */
  isDirty: boolean;
  /** The table's lifecycle status. */
  status: string;
  /** Saved applied rules differ from the approved version (`unapproved_rule_changes` on the detail). */
  unapprovedRuleChanges?: boolean;
}

/** An approved table with no unsaved edits and no saved edits awaiting approval has nothing to submit. */
export function nothingToSubmit({ isDirty, status, unapprovedRuleChanges }: SubmitGateState): boolean {
  return !isDirty && status === "approved" && !unapprovedRuleChanges;
}

/** An approved table whose saved edits are not approved yet: runs still use the approved version. */
export function hasUnapprovedSavedChanges({ status, unapprovedRuleChanges }: SubmitGateState): boolean {
  return status === "approved" && Boolean(unapprovedRuleChanges);
}
