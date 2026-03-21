# gstack

Use the `/browse` skill from gstack for **all web browsing**. Never use `mcp__claude-in-chrome__*` tools directly.

## Setup

gstack is bundled in this project at `.claude/skills/gstack`. If skills aren't working, run:
```bash
cd .claude/skills/gstack && ./setup
```

## Available gstack skills

- `/office-hours` — collaborative brainstorming / Q&A session
- `/plan-ceo-review` — review a plan from a CEO perspective
- `/plan-eng-review` — review a plan from an engineering perspective
- `/plan-design-review` — review a plan from a design perspective
- `/design-consultation` — consult on design decisions
- `/review` — code review
- `/ship` — ship a feature end-to-end
- `/browse` — web browsing (use this for ALL web browsing)
- `/qa` — QA a feature
- `/qa-only` — QA without shipping
- `/design-review` — review designs
- `/setup-browser-cookies` — set up browser cookies for authenticated browsing
- `/retro` — run a retrospective
- `/investigate` — investigate a bug or issue
- `/document-release` — document a release
- `/codex` — use Codex for a task
- `/careful` — proceed carefully with a high-stakes task
- `/freeze` — freeze the codebase
- `/guard` — guard against unwanted changes
- `/unfreeze` — unfreeze the codebase
- `/gstack-upgrade` — upgrade gstack to the latest version
