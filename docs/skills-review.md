# Skills review — 2026-10-12

## Existing capabilities

- **ui-ux-pro-max**: primary design guidance for this Flutter web/mobile project. Includes palettes, typography, charts, icons, accessibility and Flutter layout guidance. Local color and Flutter searches executed successfully. Broad color queries can return unrelated products; recommendations must be checked against agriculture and monitoring requirements before adoption.
- **computer-use**: available browser interaction and visual inspection workflow.
- **visualize**: interactive explanatory artifacts; production dashboard code still belongs in the app.
- **imagegen**: optional illustration assets; existing vector icons and code-based charts are preferable for dashboard controls.
- **sites**: separate website hosting workflow; existing Flutter application does not require migration to it.
- **documents, pdf, presentations, spreadsheets**: reporting and export workflows when requested.
- **openai-docs, skill-creator, skill-installer, plugin-management**: development environment and capability management.
- **template-creator and work-pets skills**: available but outside the current agriculture system requirements.

## Newly installed

Source: `openai/skills`, curated skills, installed with the official skill-installer helper into `C:/Users/Esmail/.codex/skills`. Installation verified through the curated inventory.

| Skill | Intended use | Runtime prerequisite / scope |
| --- | --- | --- |
| playwright | Browser flow checks and screenshots | Node/npm; browser tooling setup when used |
| playwright-interactive | Iterative functional and visual browser QA | Compatible persistent JS REPL and Playwright runtime |
| security-best-practices | Python API secure implementation and reviews | Load framework-specific references; Flutter coverage is limited |
| security-threat-model | Trust boundaries and abuse cases | Explicit threat-model request; confirm deployment assumptions |
| gh-fix-ci | Investigate GitHub Actions failures | GitHub CLI authentication and relevant repository access |
| jupyter-notebook | Reproducible data and ML experiments | Notebook execution dependencies only when needed |

## Usage priorities

Additional skills visible in the refreshed inventory: **frontend-design** (intentional product-specific visual direction), **web-design-guidelines** (web accessibility/interaction review), **dashboarding** (Grafana JSON/API authoring), and **prometheus** (PromQL/alerting guidance). These complement the installed UI toolkit. **laravel-best-practices**, **vercel-react-best-practices**, and **vercel-composition-patterns** target other stacks; the current Flutter/FastAPI application does not need a framework migration to use design guidance.

1. Use UI/UX Pro Max to establish semantic colors, typography, responsive layout, accessible charts, Arabic RTL and dark/light consistency.
2. Validate user journeys and visual states with browser tooling, alongside Flutter tests. Skill installation alone does not install all browser runtimes.
3. Use security guidance for authentication, farm scopes and administrator controls.
4. Use notebooks for targeted weather/ML experiments and CI tooling for actual failed checks.

New skills become available to the assistant on the next turn. No project UI, permissions, integrations, deployment or system configuration was changed by this installation. No Figma account connection is required for the existing local design workflow.
