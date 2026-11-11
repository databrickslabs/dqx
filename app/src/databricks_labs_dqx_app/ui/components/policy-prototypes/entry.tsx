// THROWAWAY: standalone dev-only entry for the rule-policies UI prototypes.
// Open http://localhost:9001/policy-prototypes.html while `yarn vite` runs.
// No backend, no auth, mock data only (see ./mock.ts). Not shipped.
import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import "@/styles/globals.css";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "sonner";
import { i18nReady } from "@/lib/i18n";
import { TooltipProvider } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { MOCK_RULES } from "./mock";
import { Prototype as LongForm } from "./long-form";
import { Prototype as Wizard } from "./wizard";
import { Prototype as Split } from "./split";

const VARIANTS = [
  { key: "a", label: "A · Long form", Comp: LongForm },
  { key: "b", label: "B · Wizard sheet", Comp: Wizard },
  { key: "c", label: "C · Split preview", Comp: Split },
] as const;

const RULE_TABS = ["About", "Permissions", "Implementation", "Policies", "Results", "Test", "History"];

function Shell() {
  const initial = new URLSearchParams(location.search).get("v") ?? "a";
  const [variant, setVariant] = useState<string>(initial);
  const [ruleId, setRuleId] = useState("r-email");
  const [dark, setDark] = useState(true);
  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    document.documentElement.classList.toggle("light", !dark);
  }, [dark]);
  const rule = MOCK_RULES.find((r) => r.rule_id === ruleId)!;
  const Active = VARIANTS.find((v) => v.key === variant)?.Comp ?? LongForm;
  return (
    <div className="min-h-screen bg-background text-foreground">
      <div className="sticky top-0 z-50 flex flex-wrap items-center gap-2 border-b bg-amber-500/15 px-4 py-2 text-xs">
        <span className="font-semibold">Prototype switcher (throwaway)</span>
        {VARIANTS.map((v) => (
          <button
            key={v.key}
            onClick={() => {
              setVariant(v.key);
              history.replaceState(null, "", `?v=${v.key}`);
            }}
            className={cn("rounded border px-2 py-1", variant === v.key && "bg-primary text-primary-foreground")}
          >
            {v.label}
          </button>
        ))}
        <span className="ml-4">Rule:</span>
        <select className="rounded border bg-background px-1 py-1" value={ruleId} onChange={(e) => setRuleId(e.target.value)}>
          {MOCK_RULES.map((r) => (
            <option key={r.rule_id} value={r.rule_id}>
              {r.name}
            </option>
          ))}
        </select>
        <button className="ml-auto rounded border px-2 py-1" onClick={() => setDark((d) => !d)}>
          {dark ? "Light" : "Dark"}
        </button>
      </div>
      <div className="mx-auto max-w-7xl px-6 py-6">
        <div className="text-xs text-muted-foreground">Rules registry / {rule.name}</div>
        <h1 className="mt-1 text-2xl font-semibold">{rule.name}</h1>
        <p className="text-sm text-muted-foreground">{rule.description}</p>
        <div className="mt-4 flex gap-1 border-b">
          {RULE_TABS.map((tab) => (
            <div
              key={tab}
              className={cn(
                "px-3 py-2 text-sm",
                tab === "Policies" ? "border-b-2 border-primary font-medium" : "text-muted-foreground",
              )}
            >
              {tab}
            </div>
          ))}
        </div>
        <div className="pt-6">
          <Active key={`${variant}-${ruleId}`} ruleId={ruleId} />
        </div>
      </div>
      <Toaster position="bottom-right" />
    </div>
  );
}

const queryClient = new QueryClient();
void i18nReady.then(() =>
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <TooltipProvider>
          <Shell />
        </TooltipProvider>
      </QueryClientProvider>
    </StrictMode>,
  ),
);
