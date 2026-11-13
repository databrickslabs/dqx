import { useTranslation } from "react-i18next";
import { Link } from "@tanstack/react-router";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { useListRulePolicies } from "@/lib/api";
import { policyIdOf } from "./policy-origin";

interface ViaPolicyChipProps {
  ruleId: string;
  userMetadata: Record<string, unknown> | null | undefined;
}

/** Quiet marker on a policy-made attachment. Only rendered for policy-origin
 *  rows, so the policy list is fetched lazily (and shared per rule). */
export function ViaPolicyChip({ ruleId, userMetadata }: ViaPolicyChipProps) {
  const { t } = useTranslation();
  const policyId = policyIdOf(userMetadata);
  const { data } = useListRulePolicies({ rule_id: ruleId });
  const policyName = policyId ? data?.data.find((p) => p.policy_id === policyId)?.name : undefined;

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Link
          to="/registry-rules/$ruleId"
          params={{ ruleId }}
          search={{ tab: "policies" }}
          target="_blank"
          onClick={(e) => e.stopPropagation()}
          className="inline-flex shrink-0 items-center"
        >
          <Badge variant="outline" className="text-[10px] font-normal text-muted-foreground">
            {t("rulePolicies.viaPolicy")}
          </Badge>
        </Link>
      </TooltipTrigger>
      <TooltipContent>{policyName ?? t("rulePolicies.viaPolicyFallback")}</TooltipContent>
    </Tooltip>
  );
}
