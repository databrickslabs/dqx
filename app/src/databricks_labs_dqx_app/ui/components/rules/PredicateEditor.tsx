import { useMemo } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { sql } from "@codemirror/lang-sql";
import { linter, type Diagnostic } from "@codemirror/lint";
import { autocompletion, type CompletionContext } from "@codemirror/autocomplete";
import { EditorView } from "@codemirror/view";
import { findRefRanges, findUnknownRefs } from "@/lib/columnRefs";
import { useTranslation } from "react-i18next";
import { useResolvedTheme } from "@/hooks/use-resolved-theme";

export type PredicateEditorDeclaredColumn = { name: string; family: string };

type Props = {
  value: string;
  onChange: (value: string) => void;
  declaredColumns: PredicateEditorDeclaredColumn[];
  placeholder?: string;
  disabled?: boolean;
  /** When true, the editor starts one line tall and grows with its content
   * instead of being fixed at the default 132px. Used for the row-filter field
   * (a short WHERE clause) so it isn't a big empty box by default. */
  autoHeight?: boolean;
};

/**
 * Real code editor for the SQL predicate field, ported from dqlake's
 * `PredicateEditor.tsx`. Provides SQL syntax highlighting, autocompletion of
 * the rule's declared `{{slot}}` references, and client-side squiggles for
 * unknown `{{ref}}`s. Unlike dqlake, there is no server-side `/api/lint`
 * endpoint in DQX, so linting here is purely client-side — the DDL/semicolon
 * guard (`validateSqlPredicate`) stays a separate check layered on top by the
 * caller.
 */
export function PredicateEditor({ value, onChange, declaredColumns, placeholder, disabled, autoHeight }: Props) {
  const { t } = useTranslation();
  // Item 14: the editor was hard-locked to CodeMirror's default light theme
  // regardless of the app's own dark mode. `@uiw/react-codemirror` always
  // injects its own base theme (light by default) into the extension list
  // *after* the caller's own `extensions`, so passing `oneDark` as just
  // another entry in `extensions` loses to that internal default on the CSS
  // rules it duplicates (only the syntax-highlighting colors, added via
  // `syntaxHighlighting`, actually won). The `theme` prop is the sanctioned
  // way to swap the library's own base theme, tracked live so toggling the
  // app's theme switch restyles the editor immediately.
  const resolvedTheme = useResolvedTheme();
  const slotNames = useMemo(() => declaredColumns.map((c) => c.name), [declaredColumns]);

  const unknownRefLinter = useMemo(
    () =>
      linter((view) => {
        const text = view.state.doc.toString();
        const diagnostics: Diagnostic[] = [];
        const unknown = new Set(findUnknownRefs(text, slotNames));
        for (const ref of findRefRanges(text)) {
          if (unknown.has(ref.name)) {
            diagnostics.push({
              from: ref.from,
              to: ref.to,
              severity: "error",
              message: t("rulesRegistry.unknownColumnReference", { name: ref.name }),
            });
          }
        }
        return diagnostics;
      }, { delay: 300 }),
    [slotNames, t],
  );

  const slotCompletion = useMemo(
    () =>
      autocompletion({
        override: [
          (context: CompletionContext) => {
            const before = context.matchBefore(/\{\{[a-zA-Z0-9_]*$/);
            if (!before) return null;
            const head = before.text.slice(2);
            return {
              from: before.from + 2,
              options: declaredColumns
                .filter((c) => c.name.startsWith(head))
                .map((c) => ({
                  label: c.name,
                  detail: c.family,
                  apply: (view: EditorView, _completion: unknown, from: number, to: number) => {
                    const hasClosing = view.state.sliceDoc(to, to + 2) === "}}";
                    const insert = hasClosing ? c.name : `${c.name}}}`;
                    view.dispatch({
                      changes: { from, to, insert },
                      selection: { anchor: from + c.name.length + 2 },
                    });
                  },
                  type: "variable",
                })),
            };
          },
        ],
      }),
    [declaredColumns],
  );

  const fontSizeTheme = useMemo(
    () =>
      EditorView.theme({
        "&": { fontSize: "13px" },
        ".cm-content": { fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace" },
        // Fixed-height editors reserve a comfortable 132px min; auto-height
        // ones start at a single line and grow with their content.
        ".cm-scroller": { minHeight: autoHeight ? "auto" : "132px" },
      }),
    [autoHeight],
  );

  return (
    <CodeMirror
      value={value}
      onChange={onChange}
      editable={!disabled}
      // Auto-height: let CodeMirror size to its content (grows as lines are
      // added); otherwise pin the default 132px box.
      height={autoHeight ? undefined : "132px"}
      placeholder={placeholder}
      theme={resolvedTheme}
      extensions={[sql(), unknownRefLinter, slotCompletion, fontSizeTheme, EditorView.lineWrapping]}
      className="border rounded-md overflow-hidden"
      basicSetup={{ foldGutter: false }}
    />
  );
}
