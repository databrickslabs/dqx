import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import CodeMirror from "@uiw/react-codemirror";
import { sql } from "@codemirror/lang-sql";
import { EditorView } from "@codemirror/view";
import { useTheme } from "@/components/layout/theme-provider";
import { Label } from "@/components/ui/label";

type Props = {
  value: string;
  onChange: (next: string) => void;
  disabled?: boolean;
};

// Ported from dqlake's GroupBySqlField. Unlike dqlake (hard-coded dark
// theme), this respects the app theme (item 14 dark-mode fix applied here
// too). The group-by SQL references the rule's declared `{{slot}}`
// placeholders, e.g. `{{region}}, COALESCE({{country}}, 'XX')`.
export function GroupBySqlField({ value, onChange, disabled }: Props) {
  const { t } = useTranslation();
  const { theme } = useTheme();
  const isDark =
    theme === "dark" ||
    (theme === "system" && typeof window !== "undefined" && window.matchMedia("(prefers-color-scheme: dark)").matches);

  const fontSizeTheme = useMemo(
    () =>
      EditorView.theme({
        "&": { fontSize: "12px" },
        ".cm-content": { fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace" },
      }),
    [],
  );

  return (
    <div className="flex flex-col gap-2">
      <Label className="text-xs">{t("rulesRegistry.lowcodeGroupByLabel")}</Label>
      <p className="text-[10px] text-muted-foreground">{t("rulesRegistry.lowcodeGroupByHelp")}</p>
      <CodeMirror
        value={value}
        onChange={onChange}
        editable={!disabled}
        minHeight="28px"
        theme={isDark ? "dark" : "light"}
        placeholder="{{region}}, COALESCE({{country}}, 'XX')"
        extensions={[sql(), fontSizeTheme, EditorView.lineWrapping]}
        className="border rounded-md overflow-hidden"
        basicSetup={{ foldGutter: false, lineNumbers: false }}
      />
    </div>
  );
}
