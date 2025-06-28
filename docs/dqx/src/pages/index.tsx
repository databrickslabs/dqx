import Layout from '@theme/Layout';
import { JSX } from 'react';
import Button from '../components/Button';
import { Info, FileText, Activity, AlertTriangle, CheckCircle, Grid, BarChart2, Code, PieChart } from 'lucide-react';

const CallToAction = () => {
  return (
    <div className="flex flex-col justify-center h-screen items-center">
      <h2 className="text-3xl md:text-4xl font-semibold text-center mb-6">
        Improve your Data Quality now 🚀
      </h2>
      <p className="text-center mb-6 text-pretty">
        Follow our comprehensive guide to get up and running with DQX in no time.
      </p>
      <Button
        variant="primary"
        link="/docs/installation"
        size="large"
        label="Start using DQX ✨"
        className="w-full p-4 font-mono md:w-auto bg-gradient-to-r from-blue-500 to-purple-500 text-white hover:from-blue-600 hover:to-purple-600 transition-all duration-300"
      />
    </div>
  )
};

const Capabilities = () => {

  const capabilities = [
    {
      title: 'Info of Failed Checks',
      description: 'Get detailed insights into why a check has failed.',
      icon: Info,
    },
    {
      title: 'Data Format Agnostic',
      description: 'Works seamlessly with PySpark DataFrames.',
      icon: FileText,
    },
    {
      title: 'Spark Batch & Spark Structured Streaming Support',
      description: 'Includes Lakeflow Pipelines (DLT) integration.',
      icon: Activity,
    },
    {
      title: 'Custom Reactions to Failed Checks',
      description: 'Drop, mark, or quarantine invalid data flexibly.',
      icon: AlertTriangle,
    },
    {
      title: 'Check Levels',
      description: 'Use warning or error levels for failed checks.',
      icon: CheckCircle,
    },
    {
      title: 'Row & Column Level Rules',
      description: 'Define quality rules at both row and column levels.',
      icon: Grid,
    },
    {
      title: 'Profiling & Quality Rules Generation',
      description: 'Automatically profile input data and generate data quality rule candidates.',
      icon: BarChart2,
    },
    {
      title: 'Code or Config Checks',
      description: 'Define checks as code or configuration.',
      icon: Code,
    },
    {
      title: 'Validation Summary & Quality Dashboard',
      description: 'Track and identify data quality issues effectively.',
      icon: PieChart,
    },
  ];

  return (
    <div className='my-6 px-10'>
      {/* Capabilities Section */}

      <h2 className="text-3xl md:text-4xl font-semibold text-center mb-6">
        Capabilities
      </h2>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 w-full">
        {capabilities.map((capability, index) => {
          const Icon = capability.icon;
          return (
            <div
              key={index}
              className="bg-white shadow-lg rounded-lg p-6 text-center border border-gray-200 hover:shadow-xl transition-shadow"
            >
              <Icon className="w-8 h-8 mx-auto mb-3 text-red-500" />
              <h3 className="text-lg font-semibold mb-3 text-gray-800">{capability.title}</h3>
              <p className="text-gray-600 text-sm">{capability.description}</p>
            </div>
          );
        })}
      </div>
    </div>
  )
};

const Hero = () => {
  return (

    <div className="px-4 md:px-10 min-h-screen flex flex-col justify-center items-center w-full">
      {/* Logo Section */}
      <div className="m-2">
        <img src="img/logo.svg" alt="DQX Logo" className="w-32 md:w-48" />
      </div>

      <h1 className="text-4xl md:text-5xl font-semibold text-center mb-6">
        DQX - Data Quality Framework
      </h1>
      <p className="text-center text-gray-600 dark:text-gray-500 mb-4">
        Provided by <a href="https://github.com/databrickslabs" className="underline text-blue-500 hover:text-blue-700">Databricks Labs</a>
      </p>
      <p className="text-lg text-center text-balance">
        DQX is a data quality framework for Apache Spark that enables you to define, monitor, and
        address data quality issues in your Python-based data pipelines.
      </p>

      {/* Call to Action Buttons */}
      <div className="mt-12 flex flex-col space-y-4 md:flex-row md:space-y-0 md:space-x-4">
        <Button
          variant="secondary"
          outline={true}
          link="/docs/motivation"
          size="large"
          label={"Motivation"}
          className="w-full md:w-auto"
        />
        <Button
          variant="secondary"
          outline={true}
          link="/docs/installation"
          size="large"
          label={"Installation"}
          className="w-full md:w-auto"
        />
        <Button
          variant="secondary"
          outline={true}
          link="/docs/guide"
          size="large"
          label="User guide"
          className="w-full md:w-auto"
        />
        <Button
          variant="secondary"
          outline={true}
          link="/docs/demos"
          size="large"
          label="Demos"
          className="w-full md:w-auto"
        />
        <Button
          variant="secondary"
          outline={true}
          link="/docs/reference"
          size="large"
          label="Reference"
          className="w-full md:w-auto"
        />
      </div>
    </div>
  );
};


export default function Home(): JSX.Element {
  return (
    <Layout>
      <main>
        <div className='flex justify-center mx-auto'>
          <div className='max-w-screen-lg'>
            <Hero />
            <Capabilities />
            <CallToAction />
          </div>
        </div>
      </main>
    </Layout>
  );
}
