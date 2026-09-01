import { useTranslation } from "react-i18next";
import { Sparkles, User } from "lucide-react";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { AI_GRADIENT_URL } from "@/lib/ai-style";
import type { RegistryRuleOutAuthorKind } from "@/lib/api";

// Which icons does this author kind show? Ported from dqlake's
// AuthorKindIcon — human gets the User icon, AI-only gets Sparkles, and a
// human+AI collaboration gets both.
const ICONS: Record<string, { human: boolean; ai: boolean }> = {
  human: { human: true, ai: false },
  ai_generated: { human: false, ai: true },
  ai_assisted: { human: true, ai: true },
};

/**
 * Renders a rule's authorship (human / AI-generated / AI-assisted) as a
 * compact icon combo with a tooltip, matching dqlake's Rules Registry list.
 * Falls back to the human icon when `kind` is missing (rules created before
 * this field existed).
 */
export function AuthorKindIcon({ kind }: { kind?: RegistryRuleOutAuthorKind }) {
  const { t } = useTranslation();
  const which = (kind && ICONS[kind]) || ICONS.human;
  const labels: Record<string, string> = {
    human: t("rulesRegistry.authorKindHuman"),
    ai_generated: t("rulesRegistry.authorKindAiGenerated"),
    ai_assisted: t("rulesRegistry.authorKindAiAssisted"),
  };
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex items-center gap-1">
          {which.ai && <Sparkles className="h-3.5 w-3.5" stroke={AI_GRADIENT_URL} />}
          {which.human && <User className="h-3.5 w-3.5 text-muted-foreground" />}
        </span>
      </TooltipTrigger>
      <TooltipContent>{labels[kind ?? "human"] ?? kind}</TooltipContent>
    </Tooltip>
  );
}
