// Policy-made attachments carry `user_metadata {origin: "policy", policy_id}`
// (backend ORIGIN_POLICY / POLICY_ID_KEY in registry_models.py).

const ORIGIN_KEY = "origin";
const ORIGIN_POLICY = "policy";
const POLICY_ID_KEY = "policy_id";

type UserMetadata = Record<string, unknown> | null | undefined;

export function isPolicyOrigin(userMetadata: UserMetadata): boolean {
  return userMetadata?.[ORIGIN_KEY] === ORIGIN_POLICY;
}

export function policyIdOf(userMetadata: UserMetadata): string | null {
  if (!isPolicyOrigin(userMetadata)) return null;
  const id = userMetadata?.[POLICY_ID_KEY];
  return typeof id === "string" && id ? id : null;
}
