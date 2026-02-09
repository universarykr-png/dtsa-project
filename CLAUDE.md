# CLAUDE.md - AI Assistant Guide for dtsa-project

## Project Overview

**dtsa-project** is a financial analysis, stock management, and predictive system.
(금융에 대한 분석. 주식에 대한 관리를 한다. 예측성 시스템)

**Status:** Early-stage repository. No source code, build system, or dependencies have been implemented yet. The project is ready for initial development.

## Repository Structure

```
dtsa-project/
├── CLAUDE.md          # This file - AI assistant guide
└── README.md          # Project description (Korean)
```

## Project Intent

Based on the README, the system is intended to cover:
- **Financial analysis** - analyzing financial data and markets
- **Stock management** - tracking and managing stock portfolios
- **Predictive system** - forecasting / prediction capabilities

## Current State

- **Language/framework:** Not yet chosen
- **Build system:** None configured
- **Package manager:** None configured
- **Testing:** No test framework set up
- **Linting/formatting:** No tools configured
- **CI/CD:** No pipelines defined
- **Dependencies:** None declared
- **Environment config:** No .env or Docker files

## Git Conventions

- **Remote:** GitHub (universarykr-png/dtsa-project)
- **Default branch:** `main`
- **Commit signing:** GPG signing is enabled in git config
- Write clear, descriptive commit messages summarizing the "why" not just the "what"

## Guidelines for AI Assistants

### When Adding New Code
1. Confirm the intended language/framework with the user before scaffolding
2. Set up proper project structure with standard conventions for the chosen stack
3. Include a `.gitignore` appropriate for the chosen language/framework
4. Add dependency management (package.json, requirements.txt, etc.) early
5. Configure linting and formatting from the start

### When Working on This Repo
- The README is in Korean; the project author communicates in Korean
- Always read existing files before modifying them
- Keep changes focused and minimal - avoid over-engineering
- Do not add features, tooling, or abstractions beyond what is requested
- Commit work to the designated feature branch, not `main`

### Key Reminders
- This is a greenfield project - there is no legacy code to maintain compatibility with
- Financial data projects often involve sensitive information; never commit API keys, credentials, or personal financial data
- Predictive/ML systems may require large data files; use `.gitignore` to exclude datasets and model artifacts
