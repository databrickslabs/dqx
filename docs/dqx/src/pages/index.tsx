import Layout from '@theme/Layout';
import { JSX, useState } from 'react';
import ThemedImage from '@theme/ThemedImage';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Link from '@docusaurus/Link';
import Button from '../components/Button';
import {
  AppWindow, Code, Sparkles, BarChart2, ShieldCheck, LineChart, ScrollText,
  PlusCircle, HeartPulse, Settings2, Boxes, Store, ArrowRight, Library,
  Info, FileText, Activity, AlertTriangle, Grid, PieChart, Radar, Calculator,
} from 'lucide-react';

const Hero = (): JSX.Element => {
  return (
    <div className="px-4 md:px-10 pt-20 pb-12 flex flex-col items-center w-full text-center">
      <img src={useBaseUrl('/img/logo.svg')} alt="DQX Logo" className="w-24 md:w-32 mb-6" />
      <h1 className="text-4xl md:text-6xl font-semibold mb-4">
        Data quality you can trust
      </h1>
      <p className="text-lg md:text-xl max-w-2xl text-balance text-gray-600 dark:text-gray-400 mb-2">
        DQX is the data quality framework for Databricks — define, monitor, and act on
        data quality issues across your lakehouse.
      </p>
      <p className="text-sm text-gray-500 mb-10">
        Provided by{' '}
        <a href="https://github.com/databrickslabs" className="underline hover:text-blue-500">
          Databricks Labs
        </a>
      </p>

      {/* Two doorways: Core (Python) first, Studio (no-code) second */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 w-full max-w-3xl">
        <Link
          to="/docs/guide/"
          className="group flex flex-col items-start text-left p-6 rounded-xl border-2 border-gray-300 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 shadow-sm hover:shadow-xl hover:border-blue-500 transition-all no-underline"
        >
          <Code className="w-8 h-8 text-blue-500 mb-3" />
          <h2 className="text-xl font-semibold mb-1 text-gray-900 dark:text-white">DQX Core</h2>
          <p className="text-sm text-gray-700 dark:text-gray-300 mb-3">
            The Python package. Define and run checks in your PySpark pipelines and
            notebooks — batch and streaming.
          </p>
          <span className="inline-flex items-center gap-1.5 rounded-full bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300 text-xs font-semibold px-2.5 py-1 mb-3">
            Used by 700+ Databricks customers
          </span>
          <span className="text-blue-600 dark:text-blue-400 text-sm font-medium inline-flex items-center gap-1 mt-auto">
            Open the Core guide
            <ArrowRight className="w-4 h-4 group-hover:translate-x-1 transition-transform" />
          </span>
        </Link>

        <Link
          to="/docs/studio/"
          className="group flex flex-col items-start text-left p-6 rounded-xl border-2 border-gray-300 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 shadow-sm hover:shadow-xl hover:border-red-500 transition-all no-underline"
        >
          <AppWindow className="w-8 h-8 text-red-500 mb-3" />
          <h2 className="text-xl font-semibold mb-1 text-gray-900 dark:text-white">DQX Studio</h2>
          <p className="text-sm text-gray-700 dark:text-gray-300 mb-3">
            The no-code web app. Author, run, and monitor quality rules from your browser —
            no code required.
          </p>
          <span className="inline-flex items-center gap-1.5 rounded-full bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300 text-xs font-semibold px-2.5 py-1 mb-3">
            No code required
          </span>
          <span className="text-red-600 dark:text-red-400 text-sm font-medium inline-flex items-center gap-1 mt-auto">
            Open the Studio guide
            <ArrowRight className="w-4 h-4 group-hover:translate-x-1 transition-transform" />
          </span>
        </Link>
      </div>
    </div>
  );
};

const coreFeatures = [
  { title: 'Data format agnostic', description: 'Works seamlessly with PySpark DataFrames.', icon: FileText },
  { title: 'Batch & streaming', description: 'Spark batch and Structured Streaming, with Lakeflow (DLT) pipeline integration.', icon: Activity },
  { title: 'Row & column-level rules', description: 'Define quality rules at both the row and column level.', icon: Grid },
  { title: 'Rich failure detail', description: 'Get detailed insight into exactly why a check failed.', icon: Info },
  { title: 'Custom reactions to failures', description: 'Drop, mark, or quarantine invalid data flexibly.', icon: AlertTriangle },
  { title: 'Profiling & rule generation', description: 'Profile input data and auto-generate data quality rule candidates.', icon: BarChart2 },
  { title: 'Code or config checks', description: 'Define checks in code, or declaratively as configuration.', icon: Code },
  { title: 'Validation summary & dashboard', description: 'Track and pinpoint data quality issues over time.', icon: PieChart },
  { title: 'Row anomaly detection', description: 'Detect unusual rows with trained ML models and explanations.', icon: Radar },
  { title: 'Data contracts', description: 'Generate quality rules from ODCS contracts, including schema validation.', icon: ScrollText },
];

const studioFeatures = [
  { title: 'Reusable rules repository', description: 'Build reusable checks in low code, with a built-in AI assistant.', icon: Library, link: '/docs/studio/authoring/create-a-rule' },
  { title: 'Monitor tables & thresholds', description: 'Apply rules to tables, set pass thresholds, and improve quality gradually.', icon: BarChart2, link: '/docs/studio/monitoring/assign-rules' },
  { title: 'Suggest rules with AI', description: 'Point the app at a table and it proposes a tailored set of checks for its columns and data.', icon: Sparkles, link: '/docs/studio/monitoring/assign-rules#suggest-rules-with-ai' },
  { title: 'Data products with Collections', description: 'Group related tables into a data product and see quality across all of them at once.', icon: Boxes, link: '/docs/studio/monitoring/collections' },
  { title: 'Results & drill-down', description: 'Follow the score down by dimension, severity, rule, and table — all the way to the failing rows.', icon: LineChart, link: '/docs/studio/running/results' },
  { title: 'Governed by design', description: 'Four-eyes approvals, roles, audit trails, and access that respects Unity Catalog.', icon: ShieldCheck, link: '/docs/studio/governance/approval-workflow' },
];

// Screenshot thumbnails for the Studio feature cards (properly-framed crops).
const studioFeatureImages: Record<string, string> = {
  'Reusable rules repository': 'feat_authoring',
  'Monitor tables & thresholds': 'feat_thresholds',
  'Suggest rules with AI': 'feat_suggest',
  'Data products with Collections': 'feat_collections',
  'Results & drill-down': 'feat_results',
  'Governed by design': 'feat_governed',
};

// Resolve base URLs at module scope via a small hook wrapper so we never call
// useBaseUrl conditionally or inside a loop (React requires a stable hook count).
const useStudioThumbs = () => {
  const base = useBaseUrl('/img/studio/');
  const thumbs: Record<string, { light: string; dark: string }> = {};
  for (const image of new Set(Object.values(studioFeatureImages))) {
    thumbs[image] = {
      light: `${base}${image}_light.png`,
      dark: `${base}${image}_dark.png`,
    };
  }
  return thumbs;
};

const FeatureTabs = ({ tab, setTab }: { tab: 'core' | 'studio'; setTab: (t: 'core' | 'studio') => void }): JSX.Element => {
  const isCore = tab === 'core';
  const thumbs = useStudioThumbs();

  return (
    <div className="px-4 md:px-10 py-12 w-full">
      <div className="max-w-6xl mx-auto">
        <h2 className="text-2xl md:text-3xl font-semibold text-center mb-2">
          Compare what each option offers
        </h2>
        <p className="text-center text-gray-600 dark:text-gray-400 mb-8">
          DQX comes in two flavors. Pick one to see its features.
        </p>

        {/* Full-width segmented tab picker */}
        <div className="flex w-full gap-1.5 p-1.5 rounded-xl bg-gray-100 dark:bg-gray-800/60 mb-8">
          <button
            onClick={() => setTab('core')}
            className={`flex-1 flex items-center justify-center gap-2 py-3.5 px-4 rounded-lg text-base font-semibold transition-all ${
              isCore
                ? 'bg-blue-500 text-white shadow'
                : 'text-gray-600 dark:text-gray-300 hover:bg-white/60 dark:hover:bg-gray-700/50'
            }`}
            aria-pressed={isCore}
          >
            <Code className="w-5 h-5" /> DQX Core
            <span className="hidden sm:inline font-normal opacity-80">· Python package</span>
          </button>
          <button
            onClick={() => setTab('studio')}
            className={`flex-1 flex items-center justify-center gap-2 py-3.5 px-4 rounded-lg text-base font-semibold transition-all ${
              !isCore
                ? 'bg-red-500 text-white shadow'
                : 'text-gray-600 dark:text-gray-300 hover:bg-white/60 dark:hover:bg-gray-700/50'
            }`}
            aria-pressed={!isCore}
          >
            <AppWindow className="w-5 h-5" /> DQX Studio
            <span className="hidden sm:inline font-normal opacity-80">· No-code app</span>
          </button>
        </div>

        {/* Revealed feature panel */}
        {isCore ? (
          <div>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {coreFeatures.map((f, i) => {
                const Icon = f.icon;
                return (
                  <div key={i} className="flex flex-col rounded-xl border border-gray-200 dark:border-gray-800 p-5 bg-white dark:bg-gray-900">
                    <Icon className="w-6 h-6 text-blue-500 mb-2" />
                    <h3 className="text-lg font-semibold mb-1 text-gray-900 dark:text-white">{f.title}</h3>
                    <p className="text-sm text-gray-600 dark:text-gray-400">{f.description}</p>
                  </div>
                );
              })}
            </div>
            <div className="text-center mt-8 flex flex-wrap gap-3 justify-center">
              <Button variant="secondary" outline link="/docs/motivation" size="medium" label="Motivation" />
              <Button variant="secondary" outline link="/docs/installation" size="medium" label="Install DQX Core" />
              <Button variant="secondary" outline link="/docs/guide/" size="medium" label="DQX Core guide" />
              <Button variant="secondary" outline link="/docs/demos" size="medium" label="Demos" />
            </div>
          </div>
        ) : (
          <div>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {studioFeatures.map((f, i) => {
                const Icon = f.icon;
                const image = studioFeatureImages[f.title];
                const thumb = image ? thumbs[image] : undefined;
                return (
                  <Link
                    key={i}
                    to={f.link}
                    className="group flex flex-col rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden hover:shadow-lg hover:border-red-400 transition-all no-underline bg-white dark:bg-gray-900"
                  >
                    {thumb && (
                      <div className="overflow-hidden border-b border-gray-100 dark:border-gray-800 bg-gray-100 dark:bg-gray-950/60 aspect-[16/10] flex items-center justify-center p-2">
                        <ThemedImage
                          className="max-w-full max-h-full object-contain rounded group-hover:scale-[1.02] transition-transform"
                          alt={f.title}
                          sources={{ light: thumb.light, dark: thumb.dark }}
                        />
                      </div>
                    )}
                    <div className="p-5">
                      <Icon className="w-6 h-6 text-red-500 mb-2" />
                      <h3 className="text-lg font-semibold mb-1 text-gray-900 dark:text-white">{f.title}</h3>
                      <p className="text-sm text-gray-600 dark:text-gray-400">{f.description}</p>
                    </div>
                  </Link>
                );
              })}
            </div>
            <div className="text-center mt-8 flex flex-wrap gap-3 justify-center">
              <Button
                variant="primary"
                link="/docs/studio/start-here/quickstart"
                size="medium"
                label="Get started in 10 minutes"
                className="bg-gradient-to-r from-red-500 to-orange-500 text-white hover:from-red-600 hover:to-orange-600 transition-all"
              />
              <Button variant="secondary" outline link="/docs/studio/" size="medium" label="DQX Studio guide" />
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

const TaskLauncher = (): JSX.Element => {
  const tasks = [
    { title: 'Check my data’s health', description: 'See quality scores and trends across your tables.', icon: HeartPulse, link: '/docs/studio/running/results' },
    { title: 'Add a rule', description: 'Define an expectation your data should meet.', icon: PlusCircle, link: '/docs/studio/authoring/create-a-rule' },
    { title: 'Automate quality checks', description: 'Run checks on a schedule across related tables.', icon: Settings2, link: '/docs/studio/running/run-and-schedule' },
    { title: 'Govern a rollout', description: 'Set roles, approvals, and standards for your org.', icon: ShieldCheck, link: '/docs/studio/governance/' },
  ];

  return (
    <div className="px-4 md:px-10 py-12 w-full bg-gray-50 dark:bg-gray-950/40">
      <h2 className="text-2xl md:text-3xl font-semibold text-center mb-2">
        Using DQX Studio? Jump straight in
      </h2>
      <p className="text-center text-gray-600 dark:text-gray-400 mb-8">
        Go directly to the task you came to do.
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 max-w-5xl mx-auto">
        {tasks.map((t, i) => {
          const Icon = t.icon;
          return (
            <Link
              key={i}
              to={t.link}
              className="group flex flex-col items-start text-left p-5 rounded-lg border border-gray-200 dark:border-gray-800 hover:border-red-400 hover:shadow-md transition-all no-underline bg-white dark:bg-gray-900"
            >
              <Icon className="w-6 h-6 text-red-500 mb-2" />
              <h3 className="text-base font-semibold mb-1 text-gray-900 dark:text-white">{t.title}</h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">{t.description}</p>
            </Link>
          );
        })}
      </div>
    </div>
  );
};

const Marketplace = (): JSX.Element => {
  return (
    <div className="px-4 md:px-10 py-12 w-full">
      <div className="max-w-5xl mx-auto rounded-2xl border border-dashed border-purple-300 dark:border-purple-800/60 bg-gradient-to-br from-purple-50 to-blue-50 dark:from-purple-950/20 dark:to-blue-950/20 p-8 md:p-12 text-center">
        <span className="inline-flex items-center gap-2 rounded-full bg-purple-100 dark:bg-purple-900/40 text-purple-700 dark:text-purple-300 text-xs font-semibold px-3 py-1 mb-4">
          <Store className="w-3.5 h-3.5" /> COMING SOON
        </span>
        <h2 className="text-2xl md:text-3xl font-semibold mb-3">
          DQX Studio on the Databricks Marketplace
        </h2>
        <p className="text-gray-600 dark:text-gray-400 max-w-2xl mx-auto text-balance">
          A one-click listing on the Databricks Marketplace to install DQX Studio into
          your workspace — no manual deployment steps. We’re working on it.
        </p>
      </div>
    </div>
  );
};

const CostCalculator = (): JSX.Element => {
  // Placeholder inputs — the calculator itself is not wired up yet.
  const inputs = [
    { label: 'Table size', value: '250 GB', min: '1 GB', max: '10 TB' },
    { label: 'Results views per user per day', value: '20', min: '1', max: '200' },
    { label: 'Number of users', value: '50', min: '1', max: '1,000' },
  ];

  return (
    <div className="px-4 md:px-10 py-12 w-full">
      <div className="max-w-5xl mx-auto rounded-2xl border border-dashed border-emerald-300 dark:border-emerald-800/60 bg-gradient-to-br from-emerald-50 to-teal-50 dark:from-emerald-950/20 dark:to-teal-950/20 p-8 md:p-12">
        <div className="text-center">
          <span className="inline-flex items-center gap-2 rounded-full bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300 text-xs font-semibold px-3 py-1 mb-4">
            <Calculator className="w-3.5 h-3.5" /> COMING SOON
          </span>
          <h2 className="text-2xl md:text-3xl font-semibold mb-3">
            Cost Calculator
          </h2>
          <p className="text-gray-600 dark:text-gray-400 max-w-2xl mx-auto text-balance mb-8">
            Estimate what running DQX Studio will cost for your workspace. Set your
            table size, how often people look at results, and how many users you
            have — we’ll do the maths. We’re working on it.
          </p>
        </div>

        {/* Preview of the inputs — disabled until the calculator ships */}
        <div
          className="max-w-2xl mx-auto flex flex-col gap-6 opacity-60 select-none pointer-events-none"
          aria-hidden="true"
        >
          {inputs.map((input, i) => (
            <div key={i}>
              <div className="flex items-baseline justify-between mb-2">
                <span className="text-sm font-medium text-gray-900 dark:text-white">{input.label}</span>
                <span className="text-sm font-semibold text-emerald-700 dark:text-emerald-300">{input.value}</span>
              </div>
              <input
                type="range"
                disabled
                readOnly
                min={0}
                max={100}
                defaultValue={i === 0 ? 30 : i === 1 ? 45 : 55}
                className="w-full accent-emerald-500 cursor-not-allowed"
              />
              <div className="flex justify-between text-xs text-gray-500 dark:text-gray-500 mt-1">
                <span>{input.min}</span>
                <span>{input.max}</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default function Home(): JSX.Element {
  const [tab, setTab] = useState<'core' | 'studio'>('core');
  return (
    <Layout>
      <main>
        <div className="flex flex-col items-center mx-auto w-full max-w-screen-xl">
          <Hero />
          <FeatureTabs tab={tab} setTab={setTab} />
          {tab === 'studio' && (
            <>
              <TaskLauncher />
              <Marketplace />
              <CostCalculator />
            </>
          )}
        </div>
      </main>
    </Layout>
  );
}
