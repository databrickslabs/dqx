/** Status imported rules land in: a draft submitted for review, or published
 *  outright (created and approved in one step). */
export type ImportStatus = "draft" | "published";

export const IMPORT_STATUSES: readonly ImportStatus[] = ["draft", "published"];

export function isImportStatus(value: string): value is ImportStatus {
  return (IMPORT_STATUSES as readonly string[]).includes(value);
}

/** Whether the import request should auto-approve its rules. Publishing needs
 *  an approver role; the check is repeated here so a status picked before the
 *  permissions resolved (or since revoked) can never publish on its own. */
export function shouldAutoApproveImport(status: ImportStatus, canApproveRules: boolean): boolean {
  return status === "published" && canApproveRules;
}
