import Layout from '@theme/Layout';
import { JSX, useState } from 'react';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Link from '@docusaurus/Link';
import Button from '../components/Button';
import {
  AppWindow, Code, Sparkles, BarChart2, ShieldCheck, LineChart, ScrollText,
  Boxes, Store, ArrowRight, Library,
  Info, FileText, Activity, AlertTriangle, Grid, PieChart, Radar,
  Bell, BotMessageSquare,
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
            Battle-tested data quality framework
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
          <h2 className="text-xl font-semibold mb-1 text-gray-900 dark:text-white flex items-center gap-2">
            DQX Studio
            <span className="inline-flex items-center rounded-full bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300 text-[0.65rem] font-semibold uppercase tracking-wide px-2 py-0.5 align-middle">
              Beta
            </span>
          </h2>
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
  { title: 'MCP for AI coding agents', description: 'Expose DQX tools to Genie Code, Cursor, and Claude Code over MCP.', icon: BotMessageSquare },
  { title: 'Actions and alerting', description: 'Send Slack, Teams, or webhook alerts, or fail the pipeline, when summary metrics cross a threshold.', icon: Bell },
];

const studioFeatures = [
  { title: 'Reusable rules repository', description: 'Build reusable checks in low code, with a built-in AI assistant.', icon: Library, link: '/docs/studio/authoring/create-a-rule' },
  { title: 'Row & column-level rules', description: 'Define quality rules at both the row and column level.', icon: Grid, link: '/docs/studio/authoring/create-a-rule' },
  { title: 'Rich failure detail', description: 'Drill into exactly which rows failed and why, right down to the failing records.', icon: Info, link: '/docs/studio/running/' },
  { title: 'Custom reactions to failures', description: 'Set pass thresholds and severities to decide what a failure means and how to triage it.', icon: AlertTriangle, link: '/docs/studio/monitoring/assign-rules' },
  { title: 'Profiling & rule generation', description: 'Point the app at a table to profile it and auto-generate tailored rule candidates.', icon: PieChart, link: '/docs/studio/monitoring/profiling' },
  { title: 'Data contracts support', description: 'Generate quality rules from ODCS data contracts, schema validation included.', icon: ScrollText, link: '/docs/studio/authoring/import-rules' },
  { title: 'Monitor tables & thresholds', description: 'Apply rules to tables, set pass thresholds, and improve quality gradually.', icon: BarChart2, link: '/docs/studio/monitoring/assign-rules' },
  { title: 'Suggest rules with AI', description: 'Point the app at a table and it proposes a tailored set of checks for its columns and data.', icon: Sparkles, link: '/docs/studio/monitoring/assign-rules#suggest-rules-with-ai' },
  { title: 'Data products with Collections', description: 'Group related tables into a data product and see quality across all of them at once.', icon: Boxes, link: '/docs/studio/monitoring/collections' },
  { title: 'Results & drill-down', description: 'Follow the score down by dimension, severity, rule, and table — all the way to the failing rows.', icon: LineChart, link: '/docs/studio/running/' },
  { title: 'Governed by design', description: 'Four-eyes approvals, roles, audit trails, and access that respects Unity Catalog.', icon: ShieldCheck, link: '/docs/studio/governance/approval-workflow' },
  { title: 'Marketplace-based installation', description: 'A one-click installation into your workspace using the Databricks Marketplace — no manual deployment steps.', icon: Store, link: '/docs/installation#dqx-studio-installation' },
];

const FeatureTabs = ({ tab, setTab }: { tab: 'core' | 'studio'; setTab: (t: 'core' | 'studio') => void }): JSX.Element => {
  const isCore = tab === 'core';

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
                return (
                  <Link
                    key={i}
                    to={f.link}
                    className="group flex flex-col rounded-xl border border-gray-200 dark:border-gray-800 p-5 hover:shadow-lg hover:border-red-400 transition-all no-underline bg-white dark:bg-gray-900"
                  >
                    <Icon className="w-6 h-6 text-red-500 mb-2" />
                    <h3 className="text-lg font-semibold mb-1 text-gray-900 dark:text-white">{f.title}</h3>
                    <p className="text-sm text-gray-600 dark:text-gray-400">{f.description}</p>
                  </Link>
                );
              })}
            </div>
            <div className="text-center mt-8 flex flex-wrap gap-3 justify-center">
              <Button
                variant="primary"
                link="/docs/studio/quickstart"
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

export default function Home(): JSX.Element {
  const [tab, setTab] = useState<'core' | 'studio'>('core');
  return (
    <Layout>
      <main>
        <div className="flex flex-col items-center mx-auto w-full max-w-screen-xl">
          <Hero />
          <FeatureTabs tab={tab} setTab={setTab} />
        </div>
      </main>
    </Layout>
  );
}
