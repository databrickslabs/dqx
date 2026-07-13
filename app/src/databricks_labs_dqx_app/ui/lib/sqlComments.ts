/**
 * Strip SQL comments from a predicate/query for the client-side safety mirror.
 *
 * The SQL "Explain" affordance (item 6) prepends the AI explanation to the
 * predicate as line comments (double-dash). Those comment lines are inert at
 * runtime (Spark's SQL lexer skips line comments and block comments), but their
 * PROSE can contain words that look like forbidden DDL/DML keywords ("this
 * deletes duplicates", "update the total") — so the client keyword mirror and
 * the app-side is_sql_query_safe gates must scan the code with comments removed,
 * or a harmless explanation would falsely block a save.
 *
 * SECURITY: the stripper is quote-aware. A comment marker that appears INSIDE a
 * string literal (or a quoted identifier) is NOT a comment and is left intact —
 * otherwise a crafted string ending in a double-dash could hide a live
 * forbidden keyword after a fake comment marker from the safety scan while Spark
 * still executes it. Block comments are treated as NON-nesting (stop at the
 * first close marker); this only ever removes LESS than Spark would, so it can
 * never hide live SQL.
 *
 * Newlines are preserved so the de-commented copy keeps its line structure
 * (and so a leading comment's terminating newline is never collapsed away).
 */

const QUOTES = new Set(["'", '"', "`"]);

export function stripSqlLineComments(sql: string): string {
  let out = "";
  let i = 0;
  const n = sql.length;
  while (i < n) {
    const ch = sql[i];

    // Quoted region (string literal ' " or backtick identifier). Spark escapes
    // an embedded quote by doubling it, so a doubled quote stays in-region.
    if (QUOTES.has(ch)) {
      const q = ch;
      out += ch;
      i++;
      while (i < n) {
        out += sql[i];
        if (sql[i] === q) {
          if (sql[i + 1] === q) {
            out += sql[i + 1];
            i += 2;
            continue;
          }
          i++;
          break;
        }
        i++;
      }
      continue;
    }

    // Line comment `-- ...` → drop to end of line, keep the newline.
    if (ch === "-" && sql[i + 1] === "-") {
      i += 2;
      while (i < n && sql[i] !== "\n") i++;
      continue;
    }

    // Block comment `/* ... */` (non-nesting) → drop the whole span.
    if (ch === "/" && sql[i + 1] === "*") {
      i += 2;
      while (i < n && !(sql[i] === "*" && sql[i + 1] === "/")) i++;
      i += 2; // skip the closing */ (harmless if unterminated: i overshoots n)
      continue;
    }

    out += ch;
    i++;
  }
  return out;
}
