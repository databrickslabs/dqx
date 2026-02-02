import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Card } from "@/components/ui/card";
import { Loader2, ArrowRight, Copy, Sparkles, Check } from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import { toast } from "sonner";

interface AICheckGeneratorProps {
  onGenerate: (userInput: string) => Promise<{ yaml_output: string; checks: any[] }>;
  isGenerating: boolean;
}

export function AICheckGenerator({ onGenerate, isGenerating }: AICheckGeneratorProps) {
  const [userInput, setUserInput] = useState("");
  const [generatedYaml, setGeneratedYaml] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const handleGenerate = async () => {
    if (!userInput.trim()) {
      toast.error("Please enter a description of your data quality requirements");
      return;
    }

    try {
      const result = await onGenerate(userInput);
      setGeneratedYaml(result.yaml_output);
      toast.success("Checks generated successfully!");
    } catch (error) {
      console.error("Failed to generate checks:", error);
      toast.error("Failed to generate checks. Please try again.");
    }
  };

  const handleCopy = () => {
    if (generatedYaml) {
      navigator.clipboard.writeText(generatedYaml);
      setCopied(true);
      toast.success("YAML copied to clipboard");
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleGenerate();
    }
  };

  return (
    <div className="flex flex-col h-full bg-gradient-to-br from-primary/5 via-background to-secondary/5 p-6">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <div className="p-2 bg-primary/10 rounded-lg">
          <Sparkles className="h-6 w-6 text-primary" />
        </div>
        <div>
          <h2 className="text-2xl font-bold">AI-Assisted Rules Generation</h2>
          <p className="text-sm text-muted-foreground">
            Describe your data quality needs
          </p>
        </div>
      </div>

      {/* Generated YAML Output */}
      <div className="flex-1 mb-4 overflow-hidden">
        <AnimatePresence mode="wait">
          {generatedYaml ? (
            <motion.div
              key="yaml-output"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              transition={{ duration: 0.3 }}
              className="h-full flex flex-col"
            >
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-sm font-semibold text-muted-foreground">
                  Generated Checks
                </h3>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={handleCopy}
                  className="gap-2"
                >
                  {copied ? (
                    <>
                      <Check className="h-3 w-3" />
                      Copied
                    </>
                  ) : (
                    <>
                      <Copy className="h-3 w-3" />
                      Copy
                    </>
                  )}
                </Button>
              </div>
              <Card className="flex-1 overflow-auto p-4 bg-card/50 backdrop-blur-sm">
                <pre className="text-xs font-mono whitespace-pre-wrap break-words">
                  {generatedYaml}
                </pre>
              </Card>
            </motion.div>
          ) : (
            <motion.div
              key="placeholder"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.3 }}
              className="h-full flex items-center justify-center"
            >
              <div className="text-center space-y-4 text-muted-foreground">
                <Sparkles className="h-16 w-16 mx-auto opacity-20" />
                <div>
                  <p className="font-medium">No rules generated yet</p>
                  <p className="text-sm">
                    Enter your requirements below to get started
                  </p>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* Input Area */}
      <div className="space-y-3">
        <div className="relative">
          <Textarea
            value={userInput}
            onChange={(e) => setUserInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Example: Sales amount must be positive"
            className="min-h-[120px] resize-none pr-12 bg-card/50 backdrop-blur-sm"
            disabled={isGenerating}
          />
          <div className="absolute bottom-3 right-3">
            <Button
              size="icon"
              onClick={handleGenerate}
              disabled={isGenerating || !userInput.trim()}
              className="rounded-full"
            >
              {isGenerating ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <ArrowRight className="h-4 w-4" />
              )}
            </Button>
          </div>
        </div>
        <p className="text-xs text-muted-foreground">
          Press <kbd className="px-1.5 py-0.5 text-xs font-semibold bg-muted rounded">Enter</kbd> to generate or{" "}
          <kbd className="px-1.5 py-0.5 text-xs font-semibold bg-muted rounded">Shift+Enter</kbd> for a new line
        </p>
      </div>
    </div>
  );
}

