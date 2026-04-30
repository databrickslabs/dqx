import { createContext, useContext, useState, useCallback, type ReactNode } from "react";
import { Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "@/components/ui/sheet";
import { AICheckGenerator } from "@/components/AICheckGenerator";
import { aiAssistedChecksGeneration } from "@/lib/api";

export interface RunContext {
  runName: string;
  yaml: string;
}

interface AIAssistantContextValue {
  open: boolean;
  setOpen: (open: boolean) => void;
  runContext: RunContext | null;
  setRunContext: (ctx: RunContext | null) => void;
}

const AIAssistantContext = createContext<AIAssistantContextValue | null>(null);

export function useAIAssistant() {
  const ctx = useContext(AIAssistantContext);
  if (!ctx) throw new Error("useAIAssistant must be used within AIAssistantProvider");
  return ctx;
}

export function AIAssistantTrigger() {
  const { setOpen } = useAIAssistant();
  return (
    <Button
      variant="outline"
      size="sm"
      onClick={() => setOpen(true)}
      className="gap-2"
    >
      <Sparkles className="h-4 w-4" />
      <span className="hidden sm:inline">AI Rules Assistant</span>
    </Button>
  );
}

export function AIAssistantProvider({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false);
  const [isGenerating, setIsGenerating] = useState(false);
  const [runContext, setRunContext] = useState<RunContext | null>(null);

  const handleGenerate = useCallback(async (userInput: string, contextYaml?: string) => {
    setIsGenerating(true);
    try {
      const prompt = contextYaml
        ? `Given this existing run configuration:\n\`\`\`yaml\n${contextYaml}\n\`\`\`\n\n${userInput}`
        : userInput;

      const response = await aiAssistedChecksGeneration({ user_input: prompt });
      return response.data;
    } finally {
      setIsGenerating(false);
    }
  }, []);

  return (
    <AIAssistantContext.Provider value={{ open, setOpen, runContext, setRunContext }}>
      {children}
      <Sheet open={open} onOpenChange={setOpen}>
        <SheetContent side="right" className="sm:max-w-lg w-[500px] p-0">
          <SheetHeader className="sr-only">
            <SheetTitle>AI Rules Assistant</SheetTitle>
            <SheetDescription>
              Generate data quality rules using AI
            </SheetDescription>
          </SheetHeader>
          <AICheckGenerator
            onGenerate={handleGenerate}
            isGenerating={isGenerating}
            runContext={runContext}
          />
        </SheetContent>
      </Sheet>
    </AIAssistantContext.Provider>
  );
}
