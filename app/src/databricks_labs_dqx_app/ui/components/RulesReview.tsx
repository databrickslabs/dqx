import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Trash2 } from "lucide-react";
import { toast } from "sonner";
import yaml from "js-yaml";
import { LabelsBadges } from "@/components/Labels";
import { getUserMetadata } from "@/lib/format-utils";

interface RulesReviewProps {
  checks: Record<string, unknown>[];
  onChange: (checks: Record<string, unknown>[]) => void;
}

export function RulesReview({ checks, onChange }: RulesReviewProps) {
  const [yamlText, setYamlText] = useState(() =>
    yaml.dump(checks, { sortKeys: false }),
  );
  const [yamlError, setYamlError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState("table");

  const handleYamlChange = (text: string) => {
    setYamlText(text);
    try {
      const parsed = yaml.load(text);
      if (Array.isArray(parsed)) {
        onChange(parsed);
        setYamlError(null);
      } else {
        setYamlError("YAML must be a list of check definitions");
      }
    } catch (e) {
      setYamlError((e as Error).message);
    }
  };

  const handleCriticalityChange = (index: number, value: string) => {
    const updated = [...checks];
    updated[index] = { ...updated[index], criticality: value };
    onChange(updated);
    setYamlText(yaml.dump(updated, { sortKeys: false }));
  };

  const handleRemoveCheck = (index: number) => {
    const updated = checks.filter((_, i) => i !== index);
    onChange(updated);
    setYamlText(yaml.dump(updated, { sortKeys: false }));
  };

  const handleTabChange = (tab: string) => {
    // Sync YAML text when switching to YAML tab
    if (tab === "yaml") {
      setYamlText(yaml.dump(checks, { sortKeys: false }));
      setYamlError(null);
    }
    setActiveTab(tab);
  };

  const handleCopyYaml = () => {
    navigator.clipboard.writeText(yaml.dump(checks, { sortKeys: false }));
    toast.success("YAML copied to clipboard");
  };

  return (
    <Tabs value={activeTab} onValueChange={handleTabChange}>
      <div className="flex items-center justify-between mb-4">
        <TabsList>
          <TabsTrigger value="table">Rules</TabsTrigger>
          <TabsTrigger value="yaml">YAML</TabsTrigger>
        </TabsList>
        <Button variant="outline" size="sm" onClick={handleCopyYaml}>
          Copy YAML
        </Button>
      </div>

      <TabsContent value="table">
        {checks.length === 0 ? (
          <p className="text-muted-foreground text-sm text-center py-4">
            No rules defined.
          </p>
        ) : (
          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="text-left p-3 font-medium">#</th>
                  <th className="text-left p-3 font-medium">Function</th>
                  <th className="text-left p-3 font-medium">Column(s)</th>
                  <th className="text-left p-3 font-medium">Criticality</th>
                  <th className="text-left p-3 font-medium">Labels</th>
                  <th className="text-left p-3 font-medium">Parameters</th>
                  <th className="text-right p-3 font-medium"></th>
                </tr>
              </thead>
              <tbody>
                {checks.map((check, idx) => {
                  const checkDef =
                    (check.check as Record<string, unknown>) ?? {};
                  const fn =
                    (checkDef.function as string) ?? "unknown";
                  const args =
                    (checkDef.arguments as Record<string, unknown>) ?? {};
                  const column =
                    (args.column as string) ??
                    (checkDef.for_each_column
                      ? (checkDef.for_each_column as string[]).join(", ")
                      : "-");
                  const criticality =
                    (check.criticality as string) ?? "error";
                  // Surface labels from user_metadata. Fold any legacy
                  // top-level numeric ``weight`` into the labels map so it
                  // round-trips through the same UI as everything else.
                  const labels = getUserMetadata(check);
                  if (typeof check.weight === "number" && !("weight" in labels)) {
                    labels.weight = String(check.weight);
                  }
                  const otherArgs = Object.entries(args).filter(
                    ([k]) => k !== "column",
                  );

                  return (
                    <tr
                      key={idx}
                      className="border-b last:border-b-0 hover:bg-muted/30 transition-colors"
                    >
                      <td className="p-3 text-muted-foreground tabular-nums">
                        {idx + 1}
                      </td>
                      <td className="p-3">
                        <Badge
                          variant="secondary"
                          className="font-mono text-xs"
                        >
                          {fn}
                        </Badge>
                      </td>
                      <td className="p-3 font-mono text-xs">{column}</td>
                      <td className="p-3">
                        <Select
                          value={criticality}
                          onValueChange={(val) =>
                            handleCriticalityChange(idx, val)
                          }
                        >
                          <SelectTrigger className="h-7 w-24 text-xs">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="error">Error</SelectItem>
                            <SelectItem value="warn">Warn</SelectItem>
                          </SelectContent>
                        </Select>
                      </td>
                      <td className="p-3">
                        {Object.keys(labels).length === 0 ? (
                          <span className="text-xs italic text-muted-foreground/60">
                            —
                          </span>
                        ) : (
                          <LabelsBadges labels={labels} max={3} size="sm" />
                        )}
                      </td>
                      <td className="p-3 text-xs text-muted-foreground">
                        {otherArgs.length > 0
                          ? otherArgs
                              .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
                              .join(", ")
                          : "-"}
                      </td>
                      <td className="p-3 text-right">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleRemoveCheck(idx)}
                          className="h-7 text-destructive"
                        >
                          <Trash2 className="h-3 w-3" />
                        </Button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </TabsContent>

      <TabsContent value="yaml">
        <div className="space-y-2">
          <Textarea
            value={yamlText}
            onChange={(e) => handleYamlChange(e.target.value)}
            className="min-h-[300px] font-mono text-xs"
            spellCheck={false}
          />
          {yamlError && (
            <p className="text-destructive text-xs">{yamlError}</p>
          )}
        </div>
      </TabsContent>
    </Tabs>
  );
}
