import { expect, test } from "bun:test";
import { filterGroups, filterTags, groupTags } from "./tag-picker-utils";

test("drops selected and substring-filters case-insensitively", () => {
  expect(filterTags(["class.pii", "class.name", "class.other"], ["class.pii"], "NAM")).toEqual(["class.name"]);
});

test("empty query returns all non-selected in order", () => {
  expect(filterTags(["class.a", "class.b"], [], "")).toEqual(["class.a", "class.b"]);
});

test("nothing matches", () => {
  expect(filterTags(["class.a"], [], "zzz")).toEqual([]);
});

test("whitespace-only query behaves like empty query", () => {
  expect(filterTags(["class.x", "class.y"], [], "   ")).toEqual(["class.x", "class.y"]);
});

test("already selected tags are excluded even when they match the query", () => {
  expect(filterTags(["class.pii", "class.name"], ["class.pii"], "pii")).toEqual([]);
});

test("preserves input order", () => {
  expect(filterTags(["class.z", "class.a", "class.m"], [], "")).toEqual(["class.z", "class.a", "class.m"]);
});

test("all tags selected returns empty list", () => {
  expect(filterTags(["class.a", "class.b"], ["class.a", "class.b"], "")).toEqual([]);
});

// ── groupTags ──────────────────────────────────────────────────────────────

test("flat keys with no values become bare-tag groups", () => {
  expect(groupTags(["AIRBNBPOC", "Adidas"], [])).toEqual([
    { key: "AIRBNBPOC", hasBareTag: true, values: [] },
    { key: "Adidas", hasBareTag: true, values: [] },
  ]);
});

test("a key with only values has no bare tag but keeps its values", () => {
  expect(groupTags(["ABAC_DEMO_MASK=age", "ABAC_DEMO_MASK=email"], [])).toEqual([
    { key: "ABAC_DEMO_MASK", hasBareTag: false, values: ["age", "email"] },
  ]);
});

test("a key with a bare tag plus values collapses into one group", () => {
  expect(groupTags(["ABAC_DEMO_MASK", "ABAC_DEMO_MASK=age", "ABAC_DEMO_MASK=email"], [])).toEqual([
    { key: "ABAC_DEMO_MASK", hasBareTag: true, values: ["age", "email"] },
  ]);
});

test("selected bare tag drops hasBareTag but keeps the group for its values", () => {
  expect(groupTags(["ABAC_DEMO_MASK", "ABAC_DEMO_MASK=age"], ["ABAC_DEMO_MASK"])).toEqual([
    { key: "ABAC_DEMO_MASK", hasBareTag: false, values: ["age"] },
  ]);
});

test("selected value is filtered out of its sub-list", () => {
  expect(groupTags(["ABAC_DEMO_MASK=age", "ABAC_DEMO_MASK=email"], ["ABAC_DEMO_MASK=age"])).toEqual([
    { key: "ABAC_DEMO_MASK", hasBareTag: false, values: ["email"] },
  ]);
});

test("a group with no selectable bare tag and no remaining values is omitted", () => {
  expect(groupTags(["ABAC_DEMO_MASK", "ABAC_DEMO_MASK=age"], ["ABAC_DEMO_MASK", "ABAC_DEMO_MASK=age"])).toEqual([]);
});

test("groups and values preserve first-appearance order", () => {
  expect(groupTags(["B=two", "A", "B=one", "A=x"], [])).toEqual([
    { key: "B", hasBareTag: false, values: ["two", "one"] },
    { key: "A", hasBareTag: true, values: ["x"] },
  ]);
});

test("duplicate value strings are de-duplicated", () => {
  expect(groupTags(["K=v", "K=v"], [])).toEqual([{ key: "K", hasBareTag: false, values: ["v"] }]);
});

// ── filterGroups ─────────────────────────────────────────────────────────────

test("filterGroups keeps everything for an empty query", () => {
  const groups = groupTags(["ABAC_DEMO_MASK=age", "AIRBNBPOC"], []);
  expect(filterGroups(groups, "  ")).toEqual(groups);
});

test("filterGroups surfaces a parent key when a value matches", () => {
  const groups = groupTags(["ABAC_DEMO_MASK=age", "ABAC_DEMO_MASK=email", "AIRBNBPOC"], []);
  expect(filterGroups(groups, "age")).toEqual([
    { key: "ABAC_DEMO_MASK", hasBareTag: false, values: ["age", "email"] },
  ]);
});

test("filterGroups matches on the key itself", () => {
  const groups = groupTags(["ABAC_DEMO_MASK=age", "AIRBNBPOC"], []);
  expect(filterGroups(groups, "airbnb")).toEqual([{ key: "AIRBNBPOC", hasBareTag: true, values: [] }]);
});
