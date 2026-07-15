import { expect, test } from "bun:test";
import { filterTags } from "./tag-picker-utils";

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
