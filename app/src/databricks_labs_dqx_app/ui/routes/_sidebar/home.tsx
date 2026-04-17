import { createFileRoute, Link } from "@tanstack/react-router";
import { Button } from "@/components/ui/button";
import {
  Zap,
  Clock,
  ArrowRight,
  CheckCircle2,
  Layers,
} from "lucide-react";

export const Route = createFileRoute("/_sidebar/home")({
  component: LandingPage,
});

const highlights = [
  { icon: <Layers className="h-4 w-4" />, text: "Unity Catalog integration" },
  { icon: <Zap className="h-4 w-4" />, text: "AI-powered rule generation" },
  { icon: <Clock className="h-4 w-4" />, text: "Automated scheduling" },
  { icon: <CheckCircle2 className="h-4 w-4" />, text: "Role-based approval workflow" },
];

function LandingPage() {
  return (
    <div className="flex flex-col items-center -mt-4">
      <section className="flex flex-col items-center text-center px-6 pt-14 pb-12 w-full">
        <div className="flex items-center gap-3 mb-6">
          <img src="/dqx-logo.svg" alt="DQX" className="h-11 w-11" />
          <h1 className="text-4xl sm:text-5xl font-bold tracking-tight">
            {__APP_NAME__}
          </h1>
        </div>
        <p className="text-lg text-muted-foreground max-w-2xl leading-relaxed mb-8">
          Monitor, validate, and improve data quality across your Databricks
          Lakehouse — with automated rules, scheduling, and approval workflows.
        </p>
        <div className="flex flex-wrap items-center justify-center gap-3">
          <Button asChild size="lg" className="gap-2">
            <Link to="/profiler">
              Get Started
              <ArrowRight className="h-4 w-4" />
            </Link>
          </Button>
          <Button asChild variant="outline" size="lg">
            <Link to="/rules">View Rules</Link>
          </Button>
        </div>

        <div className="flex flex-wrap items-center justify-center gap-3 mt-10">
          {highlights.map((h) => (
            <span
              key={h.text}
              className="flex items-center gap-1.5 text-xs text-muted-foreground bg-muted/60 rounded-full px-3 py-1.5"
            >
              {h.icon}
              {h.text}
            </span>
          ))}
        </div>
      </section>
    </div>
  );
}
