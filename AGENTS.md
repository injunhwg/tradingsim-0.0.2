# AGENTS.md

## Project goal
This repository is a browser-based classroom multiplayer stock trading game.

## Non-negotiable rules
- PostgreSQL is the single source of truth.
- No correctness-critical state may live only in memory.
- Do not bypass the matching engine with duplicate business logic in routes.
- Preserve price-time priority and existing trading invariants.
- Use transactions and row-level locking where needed.
- Every trade record must be immutable.
- Keep the MVP simple and reliable.

## Product scope
- One stock only
- Around 40 simultaneous students
- Student browser access
- Instructor dashboard
- Render deployment target

## Workflow
- Before coding, inspect the repository and summarize the current state.
- Before major changes, propose a short plan.
- After coding, list changed files and exact run/test commands.
- Keep documentation in sync with implementation.