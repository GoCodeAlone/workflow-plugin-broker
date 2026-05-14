# Retro: Strict-Proto Contracts — Issue #5

**Date:** 2026-05-14
**PR:** #6 (feat/issue-5-strict-contracts → main, squash-merged)

## What was shipped

- `proto/broker/v1/broker.proto` — typed messages for `NATSModuleConfig`, `BrokerPublishConfig/Input/Output`, `BrokerSubscribeConfig/Input/Output`
- `gen/broker.pb.go` — generated via protoc-gen-go v1.36.11 / protoc v34.1
- `plugin.contracts.json` — `strict_proto` mode with fully-qualified `workflow.plugin.broker.v1.*` names
- `internal/contracts.go` — `ContractRegistry()` on `BrokerProvider`, compile-time interface assertion
- `internal/contracts_test.go` — 4 tests: surface count, FDS content, config presence, step I/O
- `.github/workflows/ci.yml` — `strict-contracts` job renamed, `WFCTL_VERSION` resolved from `go.mod`
- `plugin.json` — legacy `contracts` array removed (force-cutover, no compat)

## Gates that worked

- **Copilot review** found 2 real issues: `jetstream` field in proto conflicting with `ModuleSchemas()` (backed by existing test asserting the field is absent), and test package inconsistency (`package internal` vs `package internal_test`). Both were substantive catches.
- **wfctl strict-contracts** local gate caught nothing new (passed first run) confirming the proto+ContractRegistry wiring was correct from the start.
- **CI `test` and `strict-contracts` jobs** both passed on the final commit.

## Gates that didn't fire / delays

- **CI did not trigger** on the initial PR push or the fix commit. Root cause: the branch was based on `chore/bump-workflow-v0.51.7` which included commits not yet on `main` at PR creation time, causing a CONFLICTING merge state. GitHub Actions didn't queue the CI workflow until the branch was rebased onto current main via a merge commit. Future branches should start from the current tip of `main`.

## Pattern conformance

Followed websocket PR #6 exactly:
- Proto at `proto/<name>/v1/<name>.proto`, flat gen output at `gen/<name>.pb.go`
- `ContractRegistry()` method on the plugin's provider struct (not a separate type)
- `plugin.contracts.json` as the canonical contract declaration; `plugin.json` contracts array removed
- Test file in `package internal_test` with explicit package import
