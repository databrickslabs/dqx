import { createFileRoute, Link } from "@tanstack/react-router";
import { Button } from "@/components/ui/button";
import Navbar from "@/components/apx/Navbar";
import { motion } from "motion/react";
import { Settings } from "lucide-react";
import { AICheckGenerator } from "@/components/AICheckGenerator";
import { useState } from "react";
import axios from "axios";

export const Route = createFileRoute("/")({
  component: () => <Index />,
});

function Index() {
  const [isGenerating, setIsGenerating] = useState(false);

  const handleGenerate = async (userInput: string) => {
    setIsGenerating(true);
    try {
      const response = await axios.post<{ yaml_output: string; checks: any[] }>(
        "/api/ai-generate-checks",
        { user_input: userInput },
        { withCredentials: true }
      );
      return response.data;
    } finally {
      setIsGenerating(false);
    }
  };

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden">
      {/* Navbar - Top */}
      <Navbar />

      {/* Main Content - Side by side */}
      <div className="flex-1 flex relative overflow-hidden">
        {/* Left side - AI-Assisted Rules Generation (full height) */}
        <div className="w-1/2 h-full relative border-r border-border">
          <AICheckGenerator onGenerate={handleGenerate} isGenerating={isGenerating} />
        </div>

        {/* Right side - Content (full height) */}
        <div className="w-1/2 h-full flex items-center justify-center bg-background relative z-10">
          <div className="flex flex-col items-center justify-center space-y-8 px-6 max-w-xl mx-auto text-center">
            {/* Logo with fade-in and scale animation */}
            <motion.div
              initial={{ opacity: 0, scale: 0.8 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ duration: 0.8, ease: "easeOut" }}
              className="flex items-center justify-center"
            >
              <img
                src="/dqx-logo.svg"
                alt="DQX Logo"
                className="h-24 w-24 md:h-32 md:w-32"
              />
            </motion.div>

            {/* Title with fade-in and slide-up animation */}
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.8, delay: 0.2, ease: "easeOut" }}
              className="space-y-2"
            >
              <h1 className="text-5xl md:text-6xl lg:text-7xl font-bold">
                DQX
              </h1>
              <p className="text-xl md:text-2xl text-muted-foreground">
                Data Quality Framework
              </p>
            </motion.div>

            {/* Welcome text with fade-in and slide-up animation */}
            <motion.p
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.8, delay: 0.4, ease: "easeOut" }}
              className="text-lg md:text-xl text-foreground/90 max-w-lg leading-relaxed"
            >
              Welcome to DQX. Define, monitor, and address data quality issues
              in your Apache Spark pipelines with ease.
            </motion.p>

            {/* CTA Button with fade-in and slide-up animation with hover pulse */}
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.8, delay: 0.6, ease: "easeOut" }}
              className="pt-4"
            >
              <motion.div
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
              >
                <Button
                  size="lg"
                  variant="default"
                  className="text-lg px-8 py-6"
                  asChild
                >
                  <Link to="/config">
                    <Settings className="mr-2 h-5 w-5" />
                    Configuration
                  </Link>
                </Button>
              </motion.div>
            </motion.div>
          </div>
        </div>
      </div>
    </div>
  );
}
