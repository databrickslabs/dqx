import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'DQX',
  tagline: 'Simplified Data Quality checking at Scale for PySpark Workloads on streaming and standard DataFrames.',
  favicon: 'img/logo.svg',

  url: 'https://databrickslabs.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/dqx/',
  trailingSlash: true,

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'databrickslabs', // Usually your GitHub org/user name.
  projectName: 'dqx', // Usually your repo name.

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'throw',
  onDuplicateRoutes: 'throw',
  onBrokenAnchors: 'throw',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  plugins: [
    async (context, options) => {
      return {
        name: "docusaurus-plugin-tailwindcss",
        configurePostCss(postcssOptions) {
          postcssOptions.plugins = [
            require('tailwindcss'),
            require('autoprefixer'),
          ];
          return postcssOptions;
        },
      }
    },
    'docusaurus-plugin-image-zoom',
    'docusaurus-lunr-search',
    [
      '@signalwire/docusaurus-plugin-llms-txt',
      {
        // Markdown file generation with hierarchical structure
        markdown: {
          enableFiles: true,
          relativePaths: true,
          includeBlog: false,
          includePages: true,
          includeDocs: true,
          includeVersionedDocs: false,
          excludeRoutes: [],
        },

        // llms.txt index file configuration
        llmsTxt: {
          enableLlmsFullTxt: true,
          includeBlog: false,
          includePages: true,
          includeDocs: true,
          excludeRoutes: [],

          // Site metadata
          siteTitle: 'DQX',
          siteDescription: 'Simplified Data Quality checking at Scale for PySpark Workloads on streaming and standard DataFrames.',

          // Auto-section organization (set to 1 to minimize auto-sections)
          autoSectionDepth: 1, // Group by first path segment only
          autoSectionPosition: 100, // Auto-sections appear after manual sections

          // Manual section organization
          sections: [
            {
              id: 'getting-started',
              name: 'Getting Started',
              description: 'Installation and motivation for using DQX',
              position: 1,
              routes: [
                { route: '/dqx/docs/installation' },
                { route: '/dqx/docs/motivation' }
              ],
            },
            {
              id: 'user-guide',
              name: 'User Guide',
              description: 'Complete guide for using DQX features',
              position: 2,
              routes: [
                { route: '/dqx/docs/guide/**' }
              ],
            },
            {
              id: 'reference',
              name: 'Reference',
              description: 'API reference, CLI commands, and technical documentation',
              position: 3,
              routes: [
                { route: '/dqx/docs/reference/**' }
              ],
            },
            {
              id: 'development',
              name: 'Development',
              description: 'Contributing and development documentation',
              position: 4,
              routes: [
                { route: '/dqx/docs/dev/**' }
              ],
            },
            {
              id: 'demos',
              name: 'Demos',
              description: 'Example implementations and demos',
              position: 5,
              routes: [
                { route: '/dqx/docs/demos' }
              ],
            },
            {
              id: 'home',
              name: 'Home',
              description: 'DQX homepage',
              position: 0,
              routes: [
                { route: '/dqx/' },
                { route: '/dqx/index' }
              ],
            },
          ],
        },
      }
    ]
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          // routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/databrickslabs/dqx/tree/main/docs/dqx/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],


  themeConfig: {
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: 'DQX',
      logo: {
        alt: 'DQX Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'search',
          position: 'right',
        },
        {
          href: 'https://github.com/databrickslabs/dqx',
          position: 'right',

          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },

      ],
    },
    footer: {
      links: [
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Databricks Labs. Docs built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.oneLight,
      darkTheme: prismThemes.oneDark,
    },
    zoom: {
      selector: 'article img',
      background: {
        light: '#F8FAFC',
        dark: '#F8FAFC',
      },
    }
  } satisfies Preset.ThemeConfig,
};

export default config;
