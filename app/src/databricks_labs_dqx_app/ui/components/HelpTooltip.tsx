import { HelpCircle } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";

/**
 * Small inline "?" affordance that reveals a one-line plain-language
 * explanation of a jargon term (e.g. *polarity*, *slot*, *dimension*) on
 * hover/focus. Keep the *text* short — this isn't a place for full docs.
 *
 * `rich` renders the text as HTML so a translated string may carry inline
 * emphasis (e.g. `<b>rule</b>`). Only pass trusted i18n literals here — never
 * user-supplied content — since the value is injected as raw HTML. The
 * `aria-label` always uses the plain text (tags stripped) for screen readers.
 */
export function HelpTooltip({ text, rich = false }: { text: string; rich?: boolean }) {
  const plain = rich ? text.replace(/<[^>]+>/g, "") : text;
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            className="inline-flex items-center justify-center text-muted-foreground hover:text-foreground focus-visible:outline-none"
            aria-label={plain}
          >
            <HelpCircle className="h-3 w-3" />
          </button>
        </TooltipTrigger>
        <TooltipContent className="max-w-xs text-xs">
          {rich ? <p dangerouslySetInnerHTML={{ __html: text }} /> : <p>{text}</p>}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
