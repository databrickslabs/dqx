import Layout from '@theme/Layout';
import { JSX } from 'react';
import ThemedImage from '@theme/ThemedImage';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Link from '@docusaurus/Link';
import Button from '../components/Button';
import {
  AppWindow, Code, Sparkles, BarChart2, ShieldCheck, LineChart, ScrollText,
  PlusCircle, HeartPulse, Settings2, Boxes, Store, ArrowRight,
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

      {/* Two doorways: Studio (no-code) and Core (Python) */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 w-full max-w-3xl">
        <Link
          to="/docs/studio/"
          className="group flex flex-col items-start text-left p-6 rounded-xl border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 shadow-sm hover:shadow-lg hover:border-red-400 transition-all no-underline"
        >
          <AppWindow className="w-8 h-8 text-red-500 mb-3" />
          <h2 className="text-xl font-semibold mb-1 text-gray-900 dark:text-white">DQX Studio</h2>
          <p className="text-sm text-gray-600 dark:text-gray-400 mb-3">
            The no-code web app. Author, run, and monitor quality rules from your browser —
            no code required.
          </p>
          <span className="text-red-500 text-sm font-medium inline-flex items-center gap-1">
            Open the Studio guide
            <ArrowRight className="w-4 h-4 group-hover:translate-x-1 transition-transform" />
          </span>
        </Link>

        <Link
          to="/docs/guide/"
          className="group flex flex-col items-start text-left p-6 rounded-xl border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 shadow-sm hover:shadow-lg hover:border-blue-400 transition-all no-underline"
        >
          <Code className="w-8 h-8 text-blue-500 mb-3" />
          <h2 className="text-xl font-semibold mb-1 text-gray-900 dark:text-white">DQX Core</h2>
          <p className="text-sm text-gray-600 dark:text-gray-400 mb-3">
            The Python package. Define and run checks in your PySpark pipelines and
            notebooks — batch and streaming.
          </p>
          <span className="text-blue-500 text-sm font-medium inline-flex items-center gap-1">
            Open the Core guide
            <ArrowRight className="w-4 h-4 group-hover:translate-x-1 transition-transform" />
          </span>
        </Link>
      </div>
    </div>
  );
};

const TaskLauncher = (): JSX.Element => {
  const tasks = [
    {
      title: 'Check my data’s health',
      description: 'See quality scores and trends across your tables.',
      icon: HeartPulse,
      link: '/docs/studio/running/measure-quality-health',
    },
    {
      title: 'Add a rule',
      description: 'Define an expectation your data should meet.',
      icon: PlusCircle,
      link: '/docs/studio/authoring/create-a-rule',
    },
    {
      title: 'Automate quality checks',
      description: 'Run checks on a schedule across related tables.',
      icon: Settings2,
      link: '/docs/studio/running/run-checks',
    },
    {
      title: 'Govern a rollout',
      description: 'Set roles, approvals, and standards for your org.',
      icon: ShieldCheck,
      link: '/docs/studio/start-here/set-up-for-your-org',
    },
  ];

  return (
    <div className="px-4 md:px-10 py-12 w-full">
      <h2 className="text-2xl md:text-3xl font-semibold text-center mb-2">
        What do you want to do?
      </h2>
      <p className="text-center text-gray-600 dark:text-gray-400 mb-8">
        Jump straight to the task in DQX Studio.
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

const StudioShowcase = (): JSX.Element => {
  return (
    <div className="px-4 md:px-10 py-12 w-full bg-gray-50 dark:bg-gray-950/40">
      <div className="max-w-5xl mx-auto">
        <div className="text-center mb-8">
          <span className="inline-flex items-center gap-2 text-red-500 font-medium text-sm mb-2">
            <AppWindow className="w-4 h-4" /> DQX STUDIO
          </span>
          <h2 className="text-2xl md:text-3xl font-semibold mb-2">
            See your data quality at a glance
          </h2>
          <p className="text-gray-600 dark:text-gray-400 max-w-2xl mx-auto text-balance">
            A no-code home for data stewards: an overall quality score, trends over time,
            and a clear path from rule to result.
          </p>
        </div>
        <div className="rounded-xl overflow-hidden border border-gray-200 dark:border-gray-800 shadow-lg">
          <ThemedImage
            alt="The DQX Studio home page showing quality scores and a trend chart"
            sources={{
              light: useBaseUrl('/img/studio/home_light.png'),
              dark: useBaseUrl('/img/studio/home_dark.png'),
            }}
          />
        </div>
        <div className="text-center mt-8">
          <Button
            variant="primary"
            link="/docs/studio/start-here/quickstart"
            size="large"
            label="Get started in 10 minutes"
            className="font-mono bg-gradient-to-r from-red-500 to-orange-500 text-white hover:from-red-600 hover:to-orange-600 transition-all"
          />
        </div>
      </div>
    </div>
  );
};

const Features = (): JSX.Element => {
  const features = [
    {
      title: 'No-code rule authoring',
      description: 'Build checks from a catalog of ready-made rules, or describe them and let AI draft them for you.',
      icon: Sparkles,
      image: 'create_rule_basic_checks',
      link: '/docs/studio/authoring/create-a-rule',
    },
    {
      title: 'Monitor tables & thresholds',
      description: 'Apply rules to your tables, set pass thresholds, and improve quality gradually.',
      icon: BarChart2,
      image: 'table_apply_rules',
      link: '/docs/studio/monitoring/monitor-a-table',
    },
    {
      title: 'Data products with Collections',
      description: 'Group related tables into a data product and see quality across all of them at once.',
      icon: Boxes,
      image: 'collection_results',
      link: '/docs/studio/monitoring/collections',
    },
    {
      title: 'Measure & report health',
      description: 'Overall scores, trends, and breakdowns by dimension, severity, rule, and table.',
      icon: LineChart,
      image: 'collection_results',
      link: '/docs/studio/running/measure-quality-health',
    },
    {
      title: 'Governed by design',
      description: 'Four-eyes approvals, roles, audit trails, and access that respects Unity Catalog permissions.',
      icon: ShieldCheck,
      image: 'review_approve',
      link: '/docs/studio/governance/approval-workflow',
    },
    {
      title: 'Import from data contracts',
      description: 'Bring rules in from DQX YAML or an Open Data Contract — reviewed before they go live.',
      icon: ScrollText,
      image: 'import_contract',
      link: '/docs/studio/authoring/import-rules',
    },
  ];

  return (
    <div className="px-4 md:px-10 py-12 w-full">
      <h2 className="text-2xl md:text-3xl font-semibold text-center mb-8">
        Everything you need for data quality
      </h2>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 max-w-6xl mx-auto">
        {features.map((f, i) => {
          const Icon = f.icon;
          return (
            <Link
              key={i}
              to={f.link}
              className="group flex flex-col rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden hover:shadow-lg hover:border-red-400 transition-all no-underline bg-white dark:bg-gray-900"
            >
              <div className="overflow-hidden border-b border-gray-100 dark:border-gray-800 bg-gray-50 dark:bg-gray-950/40 aspect-[16/10]">
                <ThemedImage
                  className="w-full h-full object-cover object-top group-hover:scale-[1.02] transition-transform"
                  alt={f.title}
                  sources={{
                    light: useBaseUrl(`/img/studio/${f.image}_light.png`),
                    dark: useBaseUrl(`/img/studio/${f.image}_dark.png`),
                  }}
                />
              </div>
              <div className="p-5">
                <Icon className="w-6 h-6 text-red-500 mb-2" />
                <h3 className="text-lg font-semibold mb-1 text-gray-900 dark:text-white">{f.title}</h3>
                <p className="text-sm text-gray-600 dark:text-gray-400">{f.description}</p>
              </div>
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
          The DQX Marketplace
        </h2>
        <p className="text-gray-600 dark:text-gray-400 max-w-2xl mx-auto text-balance">
          Browse and install ready-made rule packs for common domains and standards —
          so you can start with a trusted set of data quality rules instead of a blank page.
          We’re building it now.
        </p>
      </div>
    </div>
  );
};

const CoreCallout = (): JSX.Element => {
  return (
    <div className="px-4 md:px-10 pb-16 pt-4 w-full">
      <div className="max-w-5xl mx-auto flex flex-col md:flex-row items-center justify-between gap-6 rounded-xl border border-gray-200 dark:border-gray-800 p-8 bg-white dark:bg-gray-900">
        <div className="text-center md:text-left">
          <h2 className="text-xl md:text-2xl font-semibold mb-1">Prefer to work in code?</h2>
          <p className="text-gray-600 dark:text-gray-400 text-balance">
            DQX Core is the Python package behind the Studio — define and run checks
            directly in your PySpark pipelines and notebooks.
          </p>
        </div>
        <div className="flex gap-3 shrink-0">
          <Button variant="secondary" outline link="/docs/installation" size="medium" label="Install" />
          <Button variant="secondary" outline link="/docs/guide/" size="medium" label="DQX Core guide" />
        </div>
      </div>
    </div>
  );
};

export default function Home(): JSX.Element {
  return (
    <Layout>
      <main>
        <div className="flex flex-col items-center mx-auto w-full max-w-screen-xl">
          <Hero />
          <TaskLauncher />
          <StudioShowcase />
          <Features />
          <Marketplace />
          <CoreCallout />
        </div>
      </main>
    </Layout>
  );
}
