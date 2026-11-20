"""Check-job submission forwards stable references and owns failure cleanup."""

import copy
from dataclasses import dataclass, field

import pytest
from server.tools import submit_check_job

SOURCE = "cat.sch.children"
REFERENCE = "cat.sch.parents"
PRIMARY_VIEW = "cat.tmp.v_1234567890_000000000001"
REFERENCE_VIEW = "cat.tmp.v_1234567890_000000000002"


@dataclass
class SubmissionEnvironment:
    """Recording stand-in for the injected OBO and job-submission boundaries."""

    created: list[str] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    submitted: list[tuple[str, dict[str, object]]] = field(default_factory=list)
    fail_creation: str | None = None
    fail_submission: bool = False
    fail_first_drop: bool = False

    def create_view(self, source: str) -> str:
        self.created.append(source)
        if source == self.fail_creation:
            raise RuntimeError("access denied")
        return PRIMARY_VIEW if source == SOURCE else REFERENCE_VIEW

    def drop_view(self, view: str) -> None:
        self.dropped.append(view)
        if self.fail_first_drop and len(self.dropped) == 1:
            raise RuntimeError("drop unavailable")

    def submit(self, operation: str, params: dict[str, object]) -> int:
        self.submitted.append((operation, params))
        if self.fail_submission:
            raise RuntimeError("job submission failed")
        return 123


def fk_check(reference: str = REFERENCE) -> dict[str, object]:
    return {
        "name": "fk_parent",
        "check": {
            "function": "foreign_key",
            "arguments": {"columns": ["parent_id"], "ref_columns": ["id"], "ref_table": reference},
        },
    }


@pytest.mark.parametrize("operation", ("run_checks", "apply_checks_and_save_to_table"))
def test_forwards_rewritten_checks_reference_views_and_operation_params(operation: str) -> None:
    env = SubmissionEnvironment()
    checks = [fk_check()]
    original = copy.deepcopy(checks)
    extra: dict[str, object] = {"sample_size": 10} if operation == "run_checks" else {"output_name": "checked"}

    run_id = submit_check_job(
        operation, SOURCE, checks, extra, create_view=env.create_view, drop_view=env.drop_view, submit_job=env.submit
    )

    assert run_id == 123
    assert env.created == [SOURCE, REFERENCE]
    assert env.dropped == []  # The runner owns cleanup after successful submission.
    assert env.submitted == [
        (
            operation,
            {
                "view_name": PRIMARY_VIEW,
                "table_name": SOURCE,
                "checks": [
                    {
                        "name": "fk_parent",
                        "check": {
                            "function": "foreign_key",
                            "arguments": {"columns": ["parent_id"], "ref_columns": ["id"], "ref_df_name": REFERENCE},
                        },
                    }
                ],
                "ref_views": {REFERENCE: REFERENCE_VIEW},
                **extra,
            },
        )
    ]
    assert checks == original


@pytest.mark.parametrize("operation", ("run_checks", "apply_checks_and_save_to_table"))
def test_submission_failure_drops_monitored_and_reference_views(operation: str) -> None:
    env = SubmissionEnvironment(fail_submission=True)
    with pytest.raises(RuntimeError, match="job submission failed"):
        submit_check_job(
            operation,
            SOURCE,
            [fk_check()],
            {},
            create_view=env.create_view,
            drop_view=env.drop_view,
            submit_job=env.submit,
        )
    assert env.dropped == [PRIMARY_VIEW, REFERENCE_VIEW]


def test_reference_creation_failure_drops_the_monitored_view_and_never_submits() -> None:
    env = SubmissionEnvironment(fail_creation=REFERENCE)
    with pytest.raises(RuntimeError, match=REFERENCE):
        submit_check_job(
            "run_checks",
            SOURCE,
            [fk_check()],
            {},
            create_view=env.create_view,
            drop_view=env.drop_view,
            submit_job=env.submit,
        )
    assert env.dropped == [PRIMARY_VIEW]
    assert env.submitted == []


def test_later_reference_failure_cleans_up_prior_reference_and_monitored_views() -> None:
    env = SubmissionEnvironment(fail_creation="cat.sch.other")
    with pytest.raises(RuntimeError, match="cat.sch.other"):
        submit_check_job(
            "run_checks",
            SOURCE,
            [fk_check(), fk_check("cat.sch.other")],
            {},
            create_view=env.create_view,
            drop_view=env.drop_view,
            submit_job=env.submit,
        )
    assert set(env.dropped) == {PRIMARY_VIEW, REFERENCE_VIEW}
    assert len(env.dropped) == 2
    assert env.submitted == []


def test_failed_drop_does_not_hide_submission_failure_or_skip_other_views() -> None:
    env = SubmissionEnvironment(fail_submission=True, fail_first_drop=True)
    with pytest.raises(RuntimeError, match="job submission failed"):
        submit_check_job(
            "run_checks",
            SOURCE,
            [fk_check()],
            {},
            create_view=env.create_view,
            drop_view=env.drop_view,
            submit_job=env.submit,
        )
    assert env.dropped == [PRIMARY_VIEW, REFERENCE_VIEW]


def test_self_reference_is_not_dropped_twice_on_submission_failure() -> None:
    env = SubmissionEnvironment(fail_submission=True)
    with pytest.raises(RuntimeError, match="job submission failed"):
        submit_check_job(
            "run_checks",
            SOURCE,
            [fk_check(SOURCE)],
            {},
            create_view=env.create_view,
            drop_view=env.drop_view,
            submit_job=env.submit,
        )
    assert env.created == [SOURCE]
    assert env.dropped == [PRIMARY_VIEW]
    assert env.submitted[0][1]["ref_views"] == {SOURCE: PRIMARY_VIEW}


def test_no_reference_checks_keep_the_existing_payload_shape() -> None:
    env = SubmissionEnvironment()
    checks: list[dict[str, object]] = [{"check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
    submit_check_job(
        "run_checks", SOURCE, checks, {}, create_view=env.create_view, drop_view=env.drop_view, submit_job=env.submit
    )
    assert env.created == [SOURCE]
    assert "ref_views" not in env.submitted[0][1]
    assert env.submitted[0][1]["checks"] == checks
