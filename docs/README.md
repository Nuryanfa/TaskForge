# TaskForge documentation

| Document | Read it when you want to know… |
| --- | --- |
| [Project README](../README.md) | What TaskForge is and how to run it in five minutes. |
| [PRD.md](PRD.md) | The product requirements, principles, and acceptance criteria. |
| [CURRENT_STATE.md](CURRENT_STATE.md) | What is implemented today and what is not. |
| [DESIGN.md](DESIGN.md) | How the system works: components, packages, protocols, concurrency, failure modes. |
| [DATA_MODEL.md](DATA_MODEL.md) | The PostgreSQL schema, invariants, indexes, and migration history. |
| [WORKFLOWS.md](WORKFLOWS.md) | Workflow DAGs, saga compensation, guarantees, and how to write idempotent compensations. |
| [API.md](API.md) | The full HTTP contract, including every error code. |
| [EVENTS.md](EVENTS.md) | The NATS JetStream job lifecycle event contract for consumers. |
| [CONFIGURATION.md](CONFIGURATION.md) | Every environment variable with defaults, bounds, and tuning advice. |
| [OPERATIONS.md](OPERATIONS.md) | Deployment order, log events, failure codes, and runbooks. |
| [TESTING.md](TESTING.md) | The test layers, what each test proves, and how to run them. |
| [adr/](adr/README.md) | Why each architectural decision was made. |
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Engineering principles and the pull-request checklist. |
| [SECURITY.md](../SECURITY.md) | Security posture and how to report vulnerabilities. |

## Keeping documentation current

A change that alters behavior updates, in the same pull request:

- `CURRENT_STATE.md` for what is implemented,
- `DESIGN.md` for protocols or component changes,
- `DATA_MODEL.md` for any new migration,
- `API.md` for any HTTP change,
- `EVENTS.md` for any change to published events,
- `CONFIGURATION.md` for any new or changed environment variable
  (and `.env.example`),
- a new ADR for any decision that is hard to reverse.
