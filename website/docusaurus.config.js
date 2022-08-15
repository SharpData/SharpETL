// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Sharp ETL website',
  tagline: 'Write ETL using your favorite SQL dialects',
  url: 'https://sharpdata.github.io/SharpETL/',
  baseUrl: '/SharpETL/',
  onBrokenLinks: 'ignore',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',

  organizationName: 'SharpData',
  projectName: 'SharpETL',

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl:
            'https://github.com/SharpData/SharpETL/tree/pages/website',
        },
        blog: {
          showReadingTime: true,
          editUrl:
            'https://github.com/SharpData/SharpETL/tree/pages/website',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        gtag: {
          trackingID: 'G-59696D1TKP',
          anonymizeIP: false
        },
      }),
    ],
  ],

  themeConfig:
  /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
  {
    navbar: {
      title: 'Sharp ETL',
      logo: {
        alt: 'Sharp ETL Logo',
        src: 'img/sharp_etl.png',
      },
      items: [
        {
          type: 'doc',
          docId: 'quick-start-guide',
          position: 'left',
          label: 'Docs',
        },
        { to: 'blog', label: 'Blog', position: 'left' },
        // { to: 'concept', label: 'Concept', position: 'left' },
        {
          href: 'https://github.com/SharpData/SharpETL',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Learn',
          items: [
            {
              label: 'Style Guide',
              to: 'docs/',
            },
            {
              label: 'Documents',
              to: 'docs/',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Blog',
              to: 'blog',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/SharpData/SharpETL',
            },
          ],
        },
      ],
      // logo: {
      //   alt: 'Sharp ETL Logo',
      //   src: '/img/sharp_etl.png',
      //   href: 'https://github.com/SharpData/SharpETL',
      //   width: 128,
      //   height: 128,
      // },
      copyright: `Copyright © ${new Date().getFullYear()} Sharp Data`,
    },
    prism: {
      theme: require('prism-react-renderer/themes/dracula'),
      additionalLanguages: ['java', 'scala', 'sql'],
    },
    announcementBar: {
      id: 'announcementBar-2',
      content:
        '⭐️ If you like Sharp ETL, give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/SharpData/SharpETL">GitHub</a>! ⭐',
    },
    algolia: {
       // The application ID provided by Algolia
       appId: 'LC78S7LHSQ',

       // Public API key: it is safe to commit it
       apiKey: '0ebe9ea80f7f9aac056d184401394f79',

       indexName: 'sharpetl',

       // Optional: see doc section below
       contextualSearch: true,

       // Optional: Algolia search parameters
       searchParameters: {},

       // Optional: path for search page that enabled by default (`false` to disable it)
       searchPagePath: 'search',

       //... other Algolia params
     },
  },
};

module.exports = config;
