import { describe, expect, it } from "bun:test";
import {
  appendMessage,
  getConversation,
  resetConversation,
  setConversationId,
} from "./genieConversationStore";

// The store is module-level state; each test uses its own context key so the
// tests stay isolated without a reset hook (matching real usage, where every
// table/product surface has its own key).

describe("genieConversationStore", () => {
  it("starts empty for an unknown context", () => {
    const c = getConversation("t:none");
    expect(c.conversationId).toBeNull();
    expect(c.messages).toEqual([]);
  });

  it("appends messages per context, isolated from other contexts", () => {
    appendMessage("t:a", { role: "user", text: "hi" });
    appendMessage("t:a", { role: "genie", text: "hello" });
    appendMessage("t:b", { role: "user", text: "other" });

    expect(getConversation("t:a").messages.map((m) => m.text)).toEqual([
      "hi",
      "hello",
    ]);
    expect(getConversation("t:b").messages).toHaveLength(1);
  });

  it("keys undefined context to a shared default bucket", () => {
    appendMessage(undefined, { role: "user", text: "global" });
    expect(getConversation(undefined).messages.at(-1)?.text).toBe("global");
  });

  it("stores and de-duplicates the conversation id", () => {
    setConversationId("t:cid", "c-1");
    expect(getConversation("t:cid").conversationId).toBe("c-1");
    const before = getConversation("t:cid");
    setConversationId("t:cid", "c-1"); // no-op — same id
    expect(getConversation("t:cid")).toBe(before);
    setConversationId("t:cid", "c-2");
    expect(getConversation("t:cid").conversationId).toBe("c-2");
  });

  it("reset clears messages AND the conversation id", () => {
    appendMessage("t:reset", { role: "user", text: "x" });
    setConversationId("t:reset", "c-9");
    resetConversation("t:reset");
    expect(getConversation("t:reset")).toEqual({
      conversationId: null,
      messages: [],
    });
  });

  it("keeps the full answer payload on a genie message", () => {
    appendMessage("t:payload", {
      role: "genie",
      text: "answer",
      sql: "SELECT 1",
      sqlDescription: "desc",
      messageId: "m-1",
      error: null,
      resultColumns: ["a"],
      resultRows: [["1"], [null]],
    });
    const m = getConversation("t:payload").messages[0];
    expect(m.sql).toBe("SELECT 1");
    expect(m.messageId).toBe("m-1");
    expect(m.resultRows).toEqual([["1"], [null]]);
  });
});
