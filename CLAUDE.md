# CLAUDE.md

Qurious — an in-memory SQL query engine in Rust on Apache Arrow, modeled on Apache DataFusion. Early stage, not production-ready.

## Workspace

Two crates (`Cargo.toml` workspace, resolver 2):

- `sqlparser/` — hand-written SQL lexer + recursive-descent parser producing its own AST. No external sqlparser dependency. Key files: `lexer.rs`, `parser.rs` (largest file in repo), `ast.rs`, `datatype.rs`.
- `qurious/` — the engine.

## Query pipeline

`ExecuteSession::sql()` in [session.rs](qurious/src/execution/session.rs) is the entry point and the best place to start reading:

```
SQL → sqlparser::Parser → Statement
    → SqlQueryPlanner (planner/sql.rs)      → LogicalPlan
    → RuleBaseOptimizer (optimizer/rule/)   → LogicalPlan
    → DefaultQueryPlanner (planner/mod.rs)  → Arc<dyn PhysicalPlan>
    → .execute()                            → Vec<RecordBatch>
```

DDL/DML statements bypass the optimizer and are handled directly by `ExecuteSession`.

### Module map (`qurious/src/`)

| Path | Role |
| --- | --- |
| `execution/` | `ExecuteSession`, `SessionConfig`, catalog/schema registries, `information_schema` |
| `planner/sql.rs` | AST → `LogicalPlan` (the other large file; most SQL feature work lands here) |
| `planner/mod.rs` | `QueryPlanner` trait: logical → physical plan and expr |
| `logical/plan/`, `logical/expr/` | `LogicalPlan` enum and `LogicalExpr` tree; `logical/builder.rs` is `LogicalPlanBuilder` |
| `optimizer/rule/` | `OptimizerRule` impls, ordered in `RuleBaseOptimizer::new()` |
| `physical/plan/`, `physical/expr/` | `PhysicalPlan` / `PhysicalExpr` traits and impls (hash & nested-loop & cross join, hash aggregate, sort, limit, …) |
| `provider/` | `CatalogProvider` / `SchemaProvider` / `TableProvider` traits |
| `datasource/` | `MemoryTable`, `file/` (csv, parquet, json) |
| `common/` | `TableRelation`, `TableSchema`, `Transformed` tree-rewrite plumbing, `JoinType` |
| `datatypes/scalar.rs` | `ScalarValue` |
| `functions/` | `UserDefinedFunction` trait + builtins (`all_builtin_functions()`) |
| `error.rs` | `Error`, `Result<T>`, `internal_err!` / `arrow_err!` macros |

### Conventions worth knowing

- Optimizer rule order in `RuleBaseOptimizer::new()` is load-bearing — `TypeCoercion` runs **last**, after subquery decorrelation. Don't reorder casually.
- Arrow `Schema` fields are name-only, so per-field table qualifiers are smuggled through schema metadata under `FIELD_QUALIFIERS_META_KEY` ([table_schema.rs](qurious/src/common/table_schema.rs)). Needed for self-joins (`nation n1, nation n2`). Preserve it when building physical schemas.
- Plan/expr rewrites go through the `Transformed` / `TransformNode` API in `common/transformed.rs`, not manual recursion.
- `LogicalPlan::children()` does **not** include the subquery plans held by `Exists`/`SubQuery` expressions, so a plain rule transform never reaches them. The subquery rules call `optimize_subquery_plan` on a subquery before inlining it as a join input; everything after that point sees it as an ordinary child. Alias generators are shared across nesting levels (`SubqueryAliases`) so nested aliases cannot collide.
- `unused_imports = "deny"` at workspace level — unused imports fail the build.
- rustfmt: `max_width = 120`.
- `datasource/connectorx/` (Postgres via connectorx) is **not** wired into `datasource/mod.rs` and has no cargo feature; it does not compile today. CI still passes `--features postgres`, which is also stale.

## Testing

Primary test surface is [sqllogictest](https://github.com/risinglightdb/sqllogictest-rs) files, not Rust unit tests:

- `qurious/tests/sql/*.slt` — feature tests, run in parallel by [sqllogictests.rs](qurious/tests/sqllogictests.rs) (custom harness, `harness = false`).
- `qurious/tests/tpch/*.slt` — the full TPC-H suite, q1–q22, all passing; skipped unless `INCLUDE_TPCH=true`. `tpch.slt` globs `q*.slt`, so a new `qN.slt` is picked up automatically. Expected results were cross-checked against sqlite3 loaded with the same scale-factor 0.01 data.
- Unit tests live inline in `#[cfg(test)]` modules; helpers in `src/test_utils.rs` (`sql_to_plan`, `build_mem_datasource!`).

```bash
cargo test                    # unit + .slt, TPC-H excluded
make test                     # same, with INCLUDE_TPCH=true
make tpch-data                # generate TPC-H data via docker (SF 0.01)
cargo test --lib              # unit tests only
RUST_LOG=debug cargo test ... # harness uses env_logger; logs each SQL run
```

When adding a SQL feature, add cases to the relevant `.slt` file — that's the expected form of coverage here.

The harness runs every file even when earlier ones fail and catches panics, so one run reports all failing cases. Keep it that way — surveying progress toward "all of TPC-H green" needs the full failure list, not just the first one.

## Committing

- **Never commit or push without the maintainer's explicit approval.** Prepare the change, describe what the commit would contain, and wait for a go-ahead.
- **No Claude/AI attribution in commit messages.** No `Co-Authored-By: Claude ...` trailer, no "Generated with Claude Code" line, no mention of Claude or AI assistance anywhere in the message or PR body. Write plain messages in the repo's existing style (`feat:` / `refactor:` prefixes, imperative summary).
