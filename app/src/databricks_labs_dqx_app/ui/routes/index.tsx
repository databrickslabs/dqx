import { createFileRoute, Link } from "@tanstack/react-router";
import { Button } from "@/components/ui/button";
import Navbar from "@/components/apx/Navbar";
import { motion } from "motion/react";
import { FileCode } from "lucide-react";

export const Route = createFileRoute("/")({
  component: () => <Index />,
});

function Index() {
  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden">
      <Navbar />

      <div className="flex-1 flex items-center justify-center bg-background relative overflow-hidden">
        <div className="flex flex-col items-center justify-center space-y-8 px-6 max-w-xl mx-auto text-center">
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

          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8, delay: 0.4, ease: "easeOut" }}
            className="text-lg md:text-xl text-foreground/90 max-w-lg leading-relaxed"
          >
            Welcome to DQX. Define, monitor, and address data quality issues
            in your Apache Spark pipelines with ease.
          </motion.p>

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
                <Link to="/runs">
                  <FileCode className="mr-2 h-5 w-5" />
                  Get started
                </Link>
              </Button>
            </motion.div>
          </motion.div>
        </div>
      </div>
    </div>
  );
}
