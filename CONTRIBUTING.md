# Contributing to Infino for OpenSearch

Thanks for your interest in the plugin. This guide gets you from a fresh
clone to a green pull request.

## Prerequisites

- **Rust**, installed with [rustup](https://www.rust-lang.org/tools/install),
  to build the native bridge in `infino-capi/`. CI builds it with 1.95.0.
- **JDK 21**, for the plugin. Point `JAVA_HOME` at it.
- **Gradle**, in the version the target OpenSearch release's build tools
  want (see the table in the README's
  [Build and test](README.md#build-and-test)): 9.4.1 or later for the
  default release.
- **Docker**, only for the quick start's `docker compose` node.

No OpenSearch checkout is needed: the plugin builds against the release
named by `OPENSEARCH_VERSION`, pulled from Maven.

## Build and test

```bash
git clone git@github.com:infino-ai/opensearch-infino.git
cd opensearch-infino
./scripts/build.sh          # the bridge and the plugin zip, for OPENSEARCH_VERSION (default 3.9.0)
cd plugin && gradle check   # precommit, unit tests, and the REST suites on a test cluster with the bridge
```

`./scripts/check-all.sh` runs the same check on every supported release,
and the `coreYamlRestTest*` tasks run OpenSearch's own REST suites; the
README's [Build and test](README.md#build-and-test) section describes both.

## Before you open a pull request

- `gradle check` passes in `plugin/`. CI runs the same check on every pull
  request.
- For a change to how documents are written or searched, the
  `coreYamlRestTest*` tasks report 0 failures, and `core-denylist.txt` drops
  the entries the change fixes. CI does not run these tasks.
- If the change touches the security integration, or anything that differs
  between OpenSearch releases, `./scripts/check-all.sh` passes too.
- New behaviour comes with tests. A query the plugin cannot answer the way
  stock OpenSearch would is refused with an error that names what is
  unsupported, never answered differently; keep it that way, and add a test
  for the refusal.
- If you skip one of OpenSearch's own REST tests, add it to
  `plugin/src/yamlRestTest/core-denylist.txt` with the reason.

## Workflow

1. Fork the repository and create a branch off `main`.
2. Make your change with tests.
3. Sign off every commit (see below).
4. Open a pull request against `main`. Each pull request needs a review
   from a maintainer before it merges.

For a larger change, such as a new query type, a new setting or a change to
how documents are stored, open an issue first so the approach can be agreed
before you write it.

## Sign your commits (DCO)

Contributions are accepted under the project's
[Apache-2.0 license](LICENSE), and each commit must carry a
[Developer Certificate of Origin](https://developercertificate.org/)
sign-off: a line certifying that you wrote the change or otherwise have the
right to submit it under that license.

```text
Signed-off-by: Your Name <you@example.com>
```

`git commit -s` adds it, using your `user.name` and `user.email`. To sign off
commits you have already made on a branch, run
`git rebase --signoff main` and force-push the branch.

## Code of conduct

Everyone taking part in the project follows the
[code of conduct](CODE_OF_CONDUCT.md).
