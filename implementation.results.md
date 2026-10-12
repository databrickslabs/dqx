# Implementation results — anomaly PR #1489

The requested implementation is available as signed local commits. Local checks passed; complete classic/serverless acceptance and the current GitHub checks are still unverified. The final workspace outcomes and cleanup below distinguish confirmed results from remaining gates. No changes have been pushed.

## Source and artifact identity

- Starting revision: `e9f6fd977bae2791032665dbdc53d78253578fca`.
- Initial packaged E2E source revision: `adfbcaf53fedde87b5a7a852139cc9c42d9ed893`.
- Final code and test revision: `f54738c68e0cfeea83fa09f92c02f1683fc0fffd`.
- Initial broad verification used `30bec88b52a880aa19fdeffe64ef8ec15c3f2232`; its packaged source bytes matched the earlier E2E artifact.
- The final wheel includes the subsequent native-Spark validation-count repair; earlier E2E success does not establish acceptance of this final artifact.
- Built wheel: `databricks_labs_dqx-0.16.0-py3-none-any.whl`.
- Final wheel SHA-256: `d1fb5265548f99b64b1b5108033ea2489cd1953b7672352522b3496cef225c1f`.
- Earlier verified wheel SHA-256: `f22704dd78769f5a572c49b028bb9d160ff48fbf06a0efb83ba899606ca4ef3d`.
- Initial broad verification test archive SHA-256: `3a44bd9572f68cbfe1829b871b62db270240ad3679cac256d284ad2078b55ba1`.
- Initial diagnostic test archive SHA-256: `85eed2dfdb724bf5c78364489dc4cd7e15dd8e84099ec81342cb3123388bc879`; earlier attempts remain separate.
- Attempt-four private runner SHA-256: `21a289f30245a1de0cb972df72120be60cacda5783f6e9c3cb6624042565adcd`.
- Attempt-five private runner SHA-256: `9bcd664c34061a216c13df0cc29bd0bd6f561e949fd39f6447fc794d945d8656`.
- Final repair test archive SHA-256: `347e0713acb8719e6bf4ac68b74fbb05dc4a4563f474f76f1944bdb4a45c56a4`.
- Final repair runner SHA-256: `c5d158b8d6df5e08aad91a3a5289745c21ccdd7ada538ada5a4d5024c1cc7f05`.
- Separate initialization-recheck runner SHA-256: `6a2bd2732089b1c23aa2edf36fb0522b2f76eabcba4292f6b51b53fe1e966fdf`.
- All 144 packaged Python source files matched the recorded manifest. The packaged E2E separately verifies installed driver and worker source bytes.

## Confirmed causes and repairs

The discovery failure was a real product regression: grouping columns were removed before the type map was built. Feature selection now excludes resolved grouping columns while restoring their original inferred types. The ten-feature limit and omission of excess feature types remain unchanged. The original cardinality regression was kept unchanged.

The distributed collision failures were caused by worker dependency imports. SHAP 0.49.1 loaded Numba 0.64.0, which referenced `coverage.types.Tracer`; coverage 7.4.4 did not provide that interface. A test-only compatibility patch masked the problem on the driver. A standalone diagnostic script exercised the unchanged original wheel serially on STANDARD serverless: both driver cases passed and both distributed cases failed with coverage 7.4.4; all four collision-path reproductions passed with coverage 7.6.12. Test dependencies now resolve the compatible version and the compatibility patch is removed. Scorer output isolation was retained. [Upstream Numba issue](https://github.com/numba/numba/issues/10239).

Stronger tests also exposed acceptance of NaN training parameters and a Spark severity-display boundary that rounded a value just below 0.9 up to 0.9. The fixes reject non-finite parameters and preserve the intended floor operation using an explicit high-precision decimal intermediate. Reporting schema, profile names and model metadata formats remain unchanged.

Final native-Spark verification exposed a separate single-model persistence defect: validation metrics stored an integer validation-row count inside a map declared to contain doubles. Spark Connect had accepted that value; native Spark rejected it before scoring. Ensemble averaging converted the count to a float, explaining why the packaged ensemble E2E passed. Both empty and nonempty validation now emit a float count. The actual new fitted-model/registry regressions fail on the previous source and pass with the two-line repair; the registry schema is unchanged.

## Test and CI changes

The behavior inventory reviews every anomaly unit/integration module and the related explanation and packaged E2E suites. Twenty-two literal-only validation checks were replaced with production calls. Stronger integration assertions verify complete row coverage, duplicate multiplicity, finite/null scoring policy, nested flagged-row sets across thresholds, and meaningful public contribution output. Real Spark explanation tests materialize redaction, malformed-response, contributor and severity behavior while replacing only the serving boundary.

A compact persisted-model matrix covers distribution single-model, distribution ensemble and correlation single-model through driver/distributed execution with contributions off/on. Representative combined grouping/time cases also cover reordered inputs, partition changes, row filters and repeated checks. The maintained packaged E2E uses the exact wheel in separate training and scoring tasks. Factory-owned cleanup is registered before training and reports unresolved resources.

Eight jobs in the existing GitHub workflows are configured to preserve anomaly-scoped per-test/process/attempt diagnostics and available branch coverage for seven days, including after failures. The reporter covers the anomaly integration and dedicated explanation subdirectories; packaged E2E emits sanitized task provenance in its test log, and root-level redaction tests retain the acceptance action's existing reporting. The anomaly merge-queue gate retains fork and secret-access protections. No new GitHub workflow or custom action was introduced.

## Local validation

| Check | Result |
| --- | --- |
| Full unit suite | 3,203 passed, one platform skip; 100.09 seconds on final signed source |
| `make fmt` | Formatting, Ruff, mypy and pylint passed |
| `make docs-build` | Documentation tests and production build passed |
| Wheel build | Passed; source manifest matched |
| Original profiler regression | Reproduced before the fix; nine targeted real-Spark discovery cases passed after it |
| Selected real-Spark scorer/explanation contracts | 26 passed |
| Collected synthetic generator contracts | Two passed |
| Validation sensitivity | Removing the finite guard made four NaN cases fail |
| Grouping sensitivity | Removing type restoration made the original cardinality regression fail |
| Restoration sensitivity | Single and ensemble output-restoration mutants failed duplicate-aware row assertions |
| Attribution sensitivity | Six real local Spark model/context probes produced positive planted-outlier attribution; all six all-null replacements were rejected |
| Cleanup sensitivity | The new factory lifecycle regression fails against the old missing-version behavior and passes with continued cleanup |
| Final assertion sensitivity | 27 bounded local probes passed, rejecting lost duplicates, null/invalid verdicts, reordered columns, and incorrect repeated-check output |
| Filtered reporting contract | Real Spark public-engine proof confirmed two excluded checks retain two null info slots; the new matrix now asserts that existing contract |
| Native registry sensitivity | Both actual fitted-model validation branches (empty and nonempty) fail against the previous integer metadata and pass after the float-count repair |
| Diagnostic portability | 49 focused diagnostics tests passed independently on Python 3.10 and 3.12, including grouped teardown errors, innermost frames and sanitized Spark conditions |

Focused checkpoints overlap the full suite and are not additional suite totals. The one unit skip requires PowerShell, which was unavailable on the validation host. The disposable restoration exercise used local Spark with the old local dependency compatibility setup; remote production evidence is recorded separately below.

## Workspace campaign

Earlier discovery evidence uses source `30bec88b` and wheel `f22704dd`. The full discovery test file and profiler source are byte-identical between that revision and final source `f54738c`; this supports the original repair without relabeling the execution as a final-artifact run. These cases are included in the broader attempt-five totals below.

The four-hour manual window began at 12:37:51 UTC on 12 October 2026 and ends at 16:37:51 UTC. No more than two STANDARD-serverless tasks may be active concurrently. Live narratives have a separate ten-group cap; numerical suites disable AI.

Dispatch controls used a shared serverless lock and live capacity checks across the campaign ledgers, limiting requests to two STANDARD-serverless task slots. The recorded final E2E dispatch observed one occupied slot before requesting its sequential train/score slot. Complete start/end timestamps were not retained for every task, so the historical peak was not independently reconstructed. All verification runs were observed terminal by 16:16:18 UTC, before the 16:37:51 UTC deadline.

| Attempt | First-attempt outcome / limitation |
| --- | --- |
| Original collision comparison, coverage 7.4.4 | Driver single and ensemble passed; distributed single and ensemble failed with the worker import error |
| Same original wheel, coverage 7.6.12 | All four collision-path reproductions passed serially without test retries; this diagnostic script did not invoke the maintained pytest regression |
| Discovery regression on earlier repaired artifact | The unchanged cardinality case passed all phases on classic and serverless; all 32 discovery cases passed on classic, including 12 focused grouping/type/feature-limit cases |
| Initial packaged serverless E2E (earlier artifact) | Passed train, fresh-session score, setup and teardown; contributions enabled; actual Spark 4.2.0 and STANDARD serverless observed |
| Initial broad classic selection | 92 cases passed all phases; eight call failures (seven integer metric-type errors and one incorrect filtered-info expectation); two started without a final outcome; 187 not started of 289 |
| Initial broad serverless selection | 34 cases passed all phases; 20 model-loading EmitterError call failures; 235 not started of 289 |
| Final repaired-wheel classic selection | All 20 selected cases passed setup/call/teardown; both pytest shards exited zero; Jobs wrapper status recorded separately below |
| Final repaired-wheel serverless selection | Three all-phase passes; 17 call failures with EmitterError during MLflow metadata YAML writing; all 20 setups and teardowns passed |
| Initialized final-wheel serverless collision recheck | All four unchanged maintained single/ensemble × driver/distributed tests passed: 12 setup/call/teardown phases, pytest exit zero, no skips or test retries |
| Final packaged classic E2E (final artifact) | Passed train, fresh-session distributed score with contributions, setup/call/teardown and owned compute termination; Spark 4.0.0 |
| Final packaged serverless E2E (final artifact) | Passed train, fresh-session distributed score with contributions and setup/call/teardown; actual STANDARD serverless Spark 4.2.0 |
| Initial packaged classic E2E (earlier artifact) | Passed train, fresh-session score, setup and teardown; contributions enabled; actual Spark 4.0.0 on DBR 17.3 observed |
| Live one-group explanation smoke | Passed on classic; one availability probe and one narrative group shared by two synthetic rows; exact source verified; owned resources removed and compute termination confirmed |

The final classic packaged E2E first stopped during cluster startup before any job or test submission. After confirmed termination, one bounded infrastructure retry on the same owned cluster started successfully; the maintained test then passed on its first execution. Both setup outcomes remain separate.

Two local E2E setup attempts preceded the successful serverless submission: a missing warehouse prerequisite and a configured warehouse that no longer existed. Neither submitted a job. Both outcomes were preserved before a GET-verified available warehouse was used. Initial numerical classic shards exited during collection before any test execution. A second private-runner attempt failed because Databricks did not define the assumed `__file__` variable. The corrected third attempt retained the exact collection traceback: importing the already-excluded CLI deployment module reached Blueprint project-root discovery, which expects checkout metadata absent from an installed wheel. The final runner excludes that module before import. Fresh-process collection against the exact installed wheel verified both final shards. Attempt four reached pytest execution but failed in the existing pytester workspace fixture because the native job environment lacked DATABRICKS_HOST. The next private runner supplies the already-verified workspace host explicitly; a fixture-level local regression reproduced the KeyError before that change and passed afterward. Source, tests, wheel and selected case identities remain unchanged. Attempt four produced 289 setup failures on each compute mode and no test-call outcomes; these are not 289 product assertion failures. All unsuccessful harness attempts are retained, not counted as product test passes. The broader attempt-five jobs were canceled after preserving their partial results so remaining workspace time could verify the repaired source. Completed test passes and failures remain recorded separately from job cancellation.

The optional AI smoke first stopped before submission because SOURCE notebook upload omitted the language. Its sole submitted attempt used explicit Python, passed and used one narrative group. The failed setup attempt and successful submitted attempt remain separate. No application retries occurred; provider-internal retry counts are unknown.

The packaged E2E uses the platform runtime with wheel-resolved dependencies, separately from the locked-dependency numerical runs. Each run records driver and worker package metadata separately and checks loaded DQX source fingerprints. Packaged E2E also checks the imported SHAP version against its package metadata; version metadata is not a proof of every dependency's in-memory object identity, as the YAML diagnostic demonstrates. For both successful final E2E runs, driver and workers agreed on the corresponding package versions below.

| Dependency | Final numerical client environment | Final classic packaged driver/workers | Final serverless packaged driver/workers |
| --- | --- | --- | --- |
| DQX | 0.16.0, exact final wheel | 0.16.0, exact final wheel | 0.16.0, exact final wheel |
| Python | 3.12.3 | 3.12.3 | 3.12.3 |
| MLflow | 3.15.2 | 3.17.0 | 3.17.0 |
| scikit-learn | 1.8.0 | 1.6.1 | 1.7.2 |
| SHAP | 0.49.1 | 0.49.1 | 0.49.1 |
| numba | 0.64.0 | 0.68.0 | 0.68.0 |
| coverage | 7.6.12 | Unavailable | Unavailable |
| NumPy | 1.26.4 | 2.1.3 | 2.3.4 |
| pandas | 2.2.3 | 2.2.3 | 2.3.3 |
| PyArrow | 19.0.1 | 19.0.1 | 21.0.0 |
| cloudpickle | 3.1.2 | 3.0.0 | 3.1.1 |

The numerical serverless client records Databricks Connect 17.3.14 and its PySpark package version 4.0.0+databricks.connect.17.3.14. Package metadata is distinct from the actual Spark session version. The final numerical runs measured Spark 4.0.0 on classic and Spark 4.2.0 on serverless; the final classic packaged run measured Spark 4.0.0. Missing package metadata is recorded as unavailable, never inferred from a configured compute flag. Final serverless packaged clients reported Connect 19.1/PySpark package 4.3.0.dev0+databricks.connect.19.1 while actual Spark measured 4.2.0. Public-safe final E2E evidence SHA-256 values are `4e93dc9b6af064db8de579634321d2d52bf8873d19bebab3073d052624d487ba` (classic) and `c37b71e0fb12c8375a704800cfa7683e597649db093de75c5867a07cc2f9cc27` (serverless).


The final locked-dependency serverless failures occur during MLflow model loading, when its C YAML dumper writes registered-model metadata. In the same runtime, dumping a harmless synthetic two-field mapping fails with the same EmitterError, independent of DQX data or model contents. A local installation of the same MLflow/PyYAML versions passes both with and without branch coverage. The separate diagnostic confirmed that all three YAML node classes held by the C extension differed from the loaded Python classes. The pure-Python dumper worked; the C/MLflow dumper failed. A fresh subprocess had matching identities and passed all dump controls with identical PyYAML module and binary hashes. This establishes inconsistent process state; the precise eviction/reimport trigger remains unresolved. No speculative production workaround or dependency bump was applied. A separate, bounded four-case initialization recheck uses the [documented Python restart](https://learn.microsoft.com/en-us/azure/databricks/libraries/restart-python-process); its preflight passed with matching node identities and working pure/C dumps against the same PyYAML file hashes; all four unchanged maintained collision tests then passed all 12 phases in 637.7 seconds, with pytest exit zero, Jobs `TERMINATED/SUCCESS`, and no skipped or failed phases. Its public-safe evidence SHA-256 is `285d4a0920d82f3f643071b0f17e88e4d12da01dc74507ce2637d898383c9e69`. The 17 original failures remain recorded. This four-case recheck does not establish passes for the other 13 previously failing cases or the complete serverless suite.

Both final classic shards passed all ten setup/call/teardown sequences apiece and wrote raw coverage and coverage summaries. Each Jobs wrapper nevertheless reported the exact same SystemExit:0 after the runner's explicit successful exit; neither recorded a bootstrap failure. This is recorded as a wrapper failure following passed tests, not as a successful Jobs run or a failed product assertion. The original runner and outcomes remain unchanged.

## Coverage, suite selection and remaining gates

The maintained CI paths include both complete anomaly suites, related explanation integrations and packaged E2E. The initial broad manual numerical selection comprises 289 anomaly and deterministic explanation/redaction cases, split into disjoint shards of 145 and 144. It excludes 15 live-endpoint or nested-deployment cases during collection and two CLI deployment cases before importing their module, respecting the campaign limits. The explicit selection digest is `d4be6216a4f256f740807eb3f1f5390247660eb0f98d84ad5ba70cdf950e23b0`. Exact selected and excluded case identities are preserved in private evidence. Manual shards are disjoint and retain original outcomes; a passing later attempt does not erase earlier failures. After the native-Spark repair and assertion corrections, final-wheel verification prioritizes 20 unique tests per compute: the four original collisions, six scoring-matrix cases, two metric-persistence cases and eight strengthened threshold/quality/explanation cases. This bounded final selection does not establish a complete classic/serverless suite pass.

CI changes extend only the existing anomaly, acceptance and nightly workflows and retain the existing pinned acceptance action. No custom GitHub action, workflow or retry-result gate was added. Per the subsequent user clarification, the acceptance action retains its current retry policy: it can classify a retry-success as flaky. Separate diagnostic attempts preserve the first failure for review; they do not change that action outcome. Manual numerical runs disable test retries.

Coverage reports measure client-side execution. Worker package versions and source fingerprints prove execution provenance; they do not establish remote-worker branch coverage. No unsupported worker coverage mechanism was introduced.

The old Codecov report lacked the two failed anomaly uploads. Its reported core patch coverage was 63.76% against a 93.93% target, and core project coverage was 85.68% (down 8.26 percentage points). The new workflow preserves available coverage after test failure. Thresholds and exclusions were not weakened. Current GitHub CI and Codecov acceptance remain unverified until these local commits are pushed and CI runs; pushing is outside this handoff.

## Signed implementation commits

| Commit | Change |
| --- | --- |
| `918ae22473d398a73243a12224e3ea003bb4b95c` | Discovery typing, worker dependencies and numeric boundaries |
| `8ec9cb97891230db5e86e492d33640e8ed04d2f1` | Production-backed unit contracts |
| `28f6d3792774b9eb09f3fb2836bd36f81f836c97` | Scoring matrix, explanations, packaged E2E and resource ownership |
| `adfbcaf53fedde87b5a7a852139cc9c42d9ed893` | Existing CI diagnostics, coverage retention and anomaly merge-queue condition |
| `33c1ac3d8964e93852828732b2a025e66e06817b` | Explanation integration coverage retention |
| `30bec88b52a880aa19fdeffe64ef8ec15c3f2232` | Collection diagnostics, attribution and cleanup regressions |
| `42ebfe7c24c8fd36619ad50d02c8ccf9d256be0c` | Native-Spark single-model validation metrics conform to the existing registry double map |
| `f54738c68e0cfeea83fa09f92c02f1683fc0fffd` | Complete integration assertions, correct filtered metadata expectations and grouped/inner failure diagnostics |

All listed commits have verified good GPG signatures. The handoff evidence is committed separately after final campaign reconciliation.

## Cleanup and handoff

Cleanup was reconciled at 16:28:05 UTC on 12 October 2026, before the campaign deadline, with **zero unresolved campaign-owned resources**. All recorded resources were resolved through deletion acknowledgments, confirmed absence or termination of the exact owned compute. Existing shared compute was untouched; cluster historical records are retained. The three clean temporary verification worktrees were also removed.

The final export downloaded 28 additional files and reused 2,837 checksum-verified earlier exports. One file was omitted by the export privacy allowlist. Exact ownership ledgers, sanitized per-attempt outcomes and local validation logs remain in private evidence.

The first cleanup audit retained five failed job DELETE requests. A separate read-only follow-up queried exactly those five acquired job IDs and received ResourceDoesNotExist for each; no mutation calls were made. These are confirmed-absent resources, not successful delete requests, and the original audit remains unchanged. The original cleanup summary SHA-256 is `21180aee5fbf53a7f597610143e6a66d3c9cb25a693c92271a71dcceb4b3678b`; the follow-up SHA-256 is `f2de92845c07e6026c902df590a4d6f7d17505be8d7b4f4a186d1146999d5d58`. The reconciled cleanup evidence SHA-256 is `b84dd71c84f387e9a0d9f69ae4f1682a447c19f0ce1ab55922afdf65c50a9f7c`.

All eight implementation commits were reverified with good GPG signatures. Formatting and code-quality checks passed again before the separate evidence-only handoff commit. No changes were pushed, no reviewer replies were posted and no new GitHub workflow or action was added.

The maintained behavior inventory is [ANOMALY_TEST_COVERAGE.md](tests/ANOMALY_TEST_COVERAGE.md).
