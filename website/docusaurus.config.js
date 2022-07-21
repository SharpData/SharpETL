/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 * @format
 */
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
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Sharp ETL',
        logo: {
          alt: 'Sharp ETL Logo',
          src: 'img/logo.svg',
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
        logo: {
          alt: 'Sharp ETL Logo',
          src: '/img/meta_opensource_logo_negative.svg',
          href: 'https://github.com/SharpData/SharpETL',
        },
        copyright: `Copyright © ${new Date().getFullYear()} Sharp Data, Built with Docusaurus.`,
      },
      prism: {
        theme: require('prism-react-renderer/themes/dracula'),
        additionalLanguages: ['java', 'scala', 'sql'],
      },
      announcementBar: {
        id: 'announcementBar',
        content:
            '⭐️ If you like Sharp ETL, give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/SharpData/SharpETL">GitHub</a>! ⭐',
      },
    }),
};

module.exports = config;
