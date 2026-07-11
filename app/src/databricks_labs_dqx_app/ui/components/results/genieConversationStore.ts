import { useSyncExternalStore } from "react";

// Ported from dqlake's `components/results/genieConversationStore.ts` (the
// spec — keep the store semantics aligned with it). Sanctioned deviation:
// the localStorage key is renamed dqlake.* -> dqxStudio.*.

/** A single turn in the Genie chat. Lives in a module-level store so the
 *  conversation survives the Sheet unmounting when the panel is closed. */
export type ChatMessage = {
  role: "user" | "genie";
  text?: string;
  sql?: string | null;
  sqlDescription?: string | null;
  messageId?: string | null;
  error?: string | null;
  resultColumns?: string[] | null;
  resultRows?: (string | null)[][] | null;
};

type Conversation = {
  conversationId: string | null;
  messages: ChatMessage[];
};

type StoreState = Record<string, Conversation>;

const STORAGE_KEY = "dqxStudio.genie.conversations";

const EMPTY: Conversation = { conversationId: null, messages: [] };

function load(): StoreState {
  if (typeof localStorage === "undefined") return {};
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? (JSON.parse(raw) as StoreState) : {};
  } catch {
    return {};
  }
}

let state: StoreState = load();
const listeners = new Set<() => void>();

function persist() {
  if (typeof localStorage === "undefined") return;
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    // Quota / private mode — keep the in-memory copy and move on.
  }
}

function emit() {
  for (const l of listeners) l();
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** A conversation is scoped per chat-context (e.g. one per table/product) so
 *  reopening the panel on the same screen restores its own thread. */
function keyFor(context: string | undefined): string {
  return context ?? "__default__";
}

export function getConversation(context: string | undefined): Conversation {
  return state[keyFor(context)] ?? EMPTY;
}

function setConversation(context: string | undefined, next: Conversation) {
  state = { ...state, [keyFor(context)]: next };
  persist();
  emit();
}

export function appendMessage(context: string | undefined, message: ChatMessage) {
  const cur = getConversation(context);
  setConversation(context, { ...cur, messages: [...cur.messages, message] });
}

export function setConversationId(context: string | undefined, id: string | null) {
  const cur = getConversation(context);
  if (cur.conversationId === id) return;
  setConversation(context, { ...cur, conversationId: id });
}

export function resetConversation(context: string | undefined) {
  setConversation(context, { conversationId: null, messages: [] });
}

/** Subscribe a component to one context's conversation. Re-renders on change
 *  and survives unmount because the data lives outside React. */
export function useConversation(context: string | undefined): Conversation {
  return useSyncExternalStore(
    subscribe,
    () => getConversation(context),
    () => EMPTY,
  );
}
