import { Suspense } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import type { User } from "@/lib/api";
import { useCurrentUserSuspense } from "@/lib/api";
import selector from "@/lib/selector";
import { HomeGrid } from "@/components/home/HomeGrid";
import { HomeStats } from "@/components/home/HomeStats";
import { FadeIn } from "@/components/anim/FadeIn";

/**
 * Faithful port of dqlake's `routes/_sidebar/home.tsx` — welcome heading,
 * "At a Glance" stat cards, and the "Get Started" nav-card grid — with the
 * established adaptations: our `useCurrentUserSuspense` (SCIM User) instead
 * of dqlake's `useGet_me`, t() for all display text, and no
 * `useDocumentTitle` (this app has no per-route document-title convention).
 * Replaces the previous marketing-style landing page.
 */
export const Route = createFileRoute("/_sidebar/home")({
  component: HomePage,
});

// Shared grey, capitalised section subheader (matches the app's stat-card and
// results-section label style).
const SECTION_LABEL = "text-xs font-medium uppercase tracking-wide text-muted-foreground";

function WelcomeHeading() {
  const { t } = useTranslation();
  const { data: me } = useCurrentUserSuspense(selector<User>());
  const name = me.display_name?.trim();
  return (
    <h1 className="text-2xl font-semibold">
      {name ? t("home.welcomeName", { name }) : t("home.welcome")}
    </h1>
  );
}

function HomePage() {
  const { t } = useTranslation();
  return (
    <FadeIn>
      <div className="space-y-6 max-w-4xl">
        <Suspense fallback={<h1 className="text-2xl font-semibold">{t("home.welcome")}</h1>}>
          <WelcomeHeading />
        </Suspense>
        <HomeStats sectionLabelClass={SECTION_LABEL} />
        <section className="space-y-3">
          <h2 className={SECTION_LABEL}>{t("home.getStarted")}</h2>
          <HomeGrid />
        </section>
      </div>
    </FadeIn>
  );
}
