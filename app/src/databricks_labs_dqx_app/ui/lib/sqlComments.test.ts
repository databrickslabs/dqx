import { describe, expect, test } from "bun:test";
import { stripSqlLineComments } from "./sqlComments";

// The comment stripper feeds the client-side SQL-safety mirror (item 6). Its
// job: remove exactly what Spark's lexer ignores, never more — a hole here
// would let a live forbidden keyword slip past the keyword scan.

describe("stripSqlLineComments", () => {
  test("removes a leading line comment but keeps the predicate + newline", () => {
    expect(stripSqlLineComments("-- explanation\n{{email}} IS NOT NULL")).toBe("\n{{email}} IS NOT NULL");
  });

  test("removes multiple leading comment lines (the explain block shape)", () => {
    expect(stripSqlLineComments("-- line one\n-- line two\n\n{{a}} > 0")).toBe("\n\n\n{{a}} > 0");
  });

  test("comment prose containing a forbidden word is fully removed", () => {
    // "delete" / "update" in prose must not survive to the keyword scan.
    expect(stripSqlLineComments("-- this deletes and updates rows\n{{a}} > 0").toLowerCase()).not.toContain("delete");
    expect(stripSqlLineComments("-- this deletes and updates rows\n{{a}} > 0")).toBe("\n{{a}} > 0");
  });

  test("a trailing line comment on the same line as live SQL is removed", () => {
    expect(stripSqlLineComments("{{a}} > 0 -- note")).toBe("{{a}} > 0 ");
  });

  test("block comment /* */ is removed", () => {
    expect(stripSqlLineComments("{{a}} /* inline */ > 0")).toBe("{{a}}  > 0");
  });

  test("SECURITY: -- inside a string literal is NOT a comment (live SQL after it survives)", () => {
    // The `--` is inside 'a--b', so ` OR DROP` after the string is real SQL and
    // must remain visible to the keyword scan.
    const sql = "{{a}} = 'a--b' OR DROP";
    expect(stripSqlLineComments(sql)).toBe(sql);
  });

  test("SECURITY: doubled-quote escape keeps the string region intact", () => {
    const sql = "{{a}} = 'O''Brien -- x' AND {{b}} > 0";
    expect(stripSqlLineComments(sql)).toBe(sql);
  });

  test("SECURITY: -- inside a backtick identifier is not a comment", () => {
    const sql = "`weird--col` > 0";
    expect(stripSqlLineComments(sql)).toBe(sql);
  });

  test("live keyword on a line after a comment is preserved (would still be rejected)", () => {
    expect(stripSqlLineComments("-- comment\nDROP TABLE x")).toBe("\nDROP TABLE x");
  });

  test("plain predicate with no comments is returned unchanged", () => {
    expect(stripSqlLineComments("{{a}} BETWEEN 1 AND 10")).toBe("{{a}} BETWEEN 1 AND 10");
  });
});
