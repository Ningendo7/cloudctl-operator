# Contributing

## Workflow

Standard fork-and-PR: fork the repo, branch off `main`, open a PR against
`main`. There's no CLA/DCO requirement.

## Local Development Loop

```bash
make manifests generate   # after changing any +kubebuilder:... marker
make test                 # unit tier - fast, no external dependencies
make build                # confirm it compiles
```

If you touched anything under `config/`, also run `make helm-generate` to
keep the Helm chart (`charts/chart/`) in sync - see its own comment in the
Makefile for two known, harmless side effects it cleans up automatically,
and one (hand-tuned `values.yaml` entries) it doesn't.

See [`docs/testing.md`](docs/testing.md) for the full three-tier test
breakdown (unit / LocalStack integration / E2E in a real kind cluster) and
how to run each locally.

## CI Is the Gate, Not a Suggestion

Every PR runs Lint, Tests, Integration Tests, E2E Tests, and Manifests
Drift. **All of them must pass before merge** - this isn't optional for
contributors, and it isn't something you're expected to pre-verify
perfectly before opening a PR either. In particular:

- **Lint runs in CI on every push.** If it fails, fix it - don't expect a
  maintainer to merge around a red lint check.
- **Manifests Drift** fails if `config/`, the generated deepcopy code, or
  the Helm chart are out of sync with what their own `make` targets would
  produce. If it fails, run `make manifests generate helm-generate` and
  commit the result.

## Code Style

- No comments that just restate what the code does - comment the *why*
  (a non-obvious constraint, a workaround, an invariant that would
  surprise a reader), never a narration of what changed or when.
- Follow the existing per-resource-type structure under
  `internal/resources/` (one file per concern: the resource itself,
  cleanup, tests) rather than introducing a different layout.
- Don't hand-edit anything under `config/crd/bases/`, `config/rbac/role.yaml`,
  `charts/chart/` (besides `values.yaml`), or any `zz_generated.*.go` file -
  these are regenerated, and hand edits will be silently overwritten and
  then flagged as drift anyway.

## Design Context

This project has real architectural decisions behind it (the ownership
ledger, the `sharedWith`/`consumes` authorization model, deterministic
naming, deletion-safety semantics) - see
[`docs/architecture.md`](docs/architecture.md) and
[`docs/threat-model.md`](docs/threat-model.md) before proposing a change
that touches any of them, so a PR doesn't end up re-litigating a decision
that already has documented reasoning behind it.
