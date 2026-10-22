# Rule Marketplace

The **Rule Marketplace** is a catalogue of ready-made, reusable data-quality rules — so you don't have to write the common ones from scratch. Browse rules grouped by domain, tick the ones you want, and import them into your Rules Registry as reusable templates you can map onto your tables.

Admin only

The Rule Marketplace is available to workspace **admins**. Find it in the sidebar, just above **Documentation**.

![Rule Marketplace page: industry and region filters, a search box, and domain-grouped rule packs](/dqx/img/studio/marketplace_light.png)![Rule Marketplace page: industry and region filters, a search box, and domain-grouped rule packs](/dqx/img/studio/marketplace_dark.png)

## Browse, preview, and import[​](#browse-preview-and-import "Direct link to Browse, preview, and import")

Rules are grouped into **domain packs** — Contacts & People, Addresses & Geo, Pricing & Money, and more. Two filters narrow what's shown: **Industry** (e.g. Banking, Retail, Healthcare) and **Region** (hierarchical — choosing **EU** shows every EU rule, choosing **France** narrows to that country). The **search** box finds a rule by name across every pack.

Click a rule to expand it and see exactly what it does — its logic, parameters, and whether the row passes or fails when the condition is true. Not sure a rule behaves as you expect? Hit **Try it out** to open a test panel, type a few sample values (or point at one of your tables), and see per-row pass/fail before you import anything.

Tick the rules you want — individually, or a whole pack via its header checkbox — then click **Import *N* selected**. They land in your Rules Registry, ready to [assign to your tables](/dqx/docs/studio/monitoring/assign-rules.md).

Imported rules arrive approved

Because Rule Marketplace packs are curated and the Rule Marketplace is admin-only, imported rules are **published on import** — they don't sit in the approval queue. Re-importing is safe: a rule you already have is reused, not duplicated.

## Deploy demo content[​](#deploy-demo-content "Direct link to Deploy demo content")

Use **Deploy demo content** — the first row on the page — to deploy example tables, rules, and past run results, so you can explore the Studio with realistic data. Click the row and confirm (the default wipes existing Studio data for a clean slate); it loads in the background and takes a while, so leave it to run.

![Deploy demo content row at the top of the Rule Marketplace, with a Deploy button](/dqx/img/studio/marketplace_demo_light.png)![Deploy demo content row at the top of the Rule Marketplace, with a Deploy button](/dqx/img/studio/marketplace_demo_dark.png)

## Contribute a pack[​](#contribute-a-pack "Direct link to Contribute a pack")

The rule packs are DQX-YAML files in the [DQX repository](https://github.com/databrickslabs/dqx/tree/main/app/src/databricks_labs_dqx_app/backend/marketplace/packs). To add a rule or a whole new domain pack, open a pull request — see the README in that directory for the format. There's a **Contribute on GitHub** link in the Rule Marketplace header too.
