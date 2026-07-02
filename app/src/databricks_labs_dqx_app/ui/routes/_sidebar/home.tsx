import { createFileRoute, Link } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import {
  Zap,
  Clock,
  ArrowRight,
  CheckCircle2,
  Layers,
  BookMarked,
  Boxes,
} from "lucide-react";

export const Route = createFileRoute("/_sidebar/home")({
  component: LandingPage,
});

function LandingPage() {
  const { t } = useTranslation();
  const highlights = [
    { icon: <Layers className="h-4 w-4" />, text: t("home.highlights.uc") },
    { icon: <Zap className="h-4 w-4" />, text: t("home.highlights.ai") },
    { icon: <Clock className="h-4 w-4" />, text: t("home.highlights.schedule") },
    { icon: <CheckCircle2 className="h-4 w-4" />, text: t("home.highlights.rbac") },
  ];

  return (
    <div className="flex flex-col items-center -mt-4">
      <section className="flex flex-col items-center text-center px-6 pt-14 pb-12 w-full">
        <div className="flex items-center gap-3 mb-6">
          <img src="/dqx-logo.svg" alt="DQX Studio" className="h-11 w-11" />
          <h1 className="text-4xl sm:text-5xl font-bold tracking-tight">
            {__APP_NAME__}
          </h1>
        </div>
        <p className="text-lg text-muted-foreground max-w-2xl leading-relaxed mb-8">
          {t("home.subtitle")}
        </p>
        <div className="flex flex-wrap items-center justify-center gap-3">
          <Button asChild size="lg" className="gap-2">
            <Link to="/registry-rules">
              {t("home.getStarted")}
              <ArrowRight className="h-4 w-4" />
            </Link>
          </Button>
          <Button asChild variant="outline" size="lg" className="gap-2">
            <Link to="/monitored-tables">
              <Boxes className="h-4 w-4" />
              {t("home.applyToTable")}
            </Link>
          </Button>
        </div>

        <div className="flex items-center justify-center gap-2 mt-6 text-sm text-muted-foreground max-w-2xl">
          <BookMarked className="h-4 w-4 shrink-0" />
          <p>{t("home.howItWorks")}</p>
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
