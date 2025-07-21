import { defineConfig } from "vitepress";

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "Schema",
  description: "Schema docs",
  base: "/schema/",
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: "Home", link: "/" },
      { text: "Docs", link: "/schema/install" },
    ],

    sidebar: [
      {
        text: "Schema",
        items: [
          { text: "Install", link: "/schema/install" },
          { text: "Get Started", link: "/schema/get-started" },
          { text: "Migrations", link: "/schema/migrations" },
          { text: "Inserts", link: "/schema/inserts" },
          { text: "Selects", link: "/schema/selects" },
          { text: "Reference", link: "/schema/reference" },
        ],
      },
      {
        text: "SQLite",
        collapsed: false,
        items: [
          { text: "Create Table", link: "/sqlite/create" },
          { text: "Alter Table", link: "/sqlite/alter" },
        ],
      },
    ],

    socialLinks: [
      { icon: "github", link: "https://github.com/gigagrug/schema" },
      { icon: "githubsponsors", link: "https://github.com/sponsors/gigagrug" },
      { icon: "discord", link: "https://discord.com/invite/HV2344GmcQ" },
    ],

    search: {
      provider: "local",
    },
  },
});
