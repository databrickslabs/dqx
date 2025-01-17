import Layout from '@theme/Layout';
import { JSX } from 'react';
import Button from '../components/Button';


const Hero = () => {
  return (
    <main className='flex flex-col items-center pt-20 w-full'>
      <div className="px-4 md:px-10 max-w-screen-xl h-[calc(100vh-140px)] flex flex-col md:flex-row justify-between w-full">
        {/* <!-- Left Column (Text) --> */}
        <div className="md:w-2/3 text-center md:text-left">
          <h1 className="text-5xl font-semibold leading-tight text-balance mb-6"><span className='font-mono'>UCX</span> - Unity Catalog
            Migration Assistant</h1>
          <p className="text-xl mb-6 text-balance">
            Ready-to-use toolkit to migrate to UC,<br /> provided by <a href="https://github.com/databrickslabs" className="underline">Databricks Labs</a>
          </p>
          <div className="flex flex-col space-y-4 md:flex-row md:space-y-0 md:space-x-4 md:justify-start justify-center items-center w-full px-20 md:px-0">
            <Button variant='secondary' outline={true} link='/docs/process' size='large' label='Process' linkClassName='w-full md:w-fit' className='w-full' />
            <Button variant='secondary' outline={true} link='/docs/reference' size='large' label='Reference' linkClassName='w-full md:w-fit' className='w-full' />
          </div>
        </div>

        {/* <!-- Right Column (Image) --> */}
        <div className="hidden md:block md:w-1/3 mt-10 md:mt-0">
          <img src="img/logo.svg" alt="UCX Logo" className="w-full rounded-lg hidden md:block" />
        </div>
      </div>
    </main>
  )
}

export default function Home(): JSX.Element {
  return (
    <Layout>
      <Hero />
    </Layout >
  );
}
