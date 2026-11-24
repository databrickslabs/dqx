"""View-backed job submission forwards stable references and owns failure cleanup."""

import copy
from dataclasses import dataclass, field

import pytest
from server.tools import submit_view_job

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
        return f"cat.tmp.v_1234567890_{len(self.created):012d}"

    def drop_view(self, view: str) -> None:
        self.dropped.append(view)
        if self.fail_first_drop and len(self.dropped) == 1:
            raise RuntimeError("drop unavailable")

    def submit(self, operation: str, params: dict[str, object]) -> int:
        self.submitted.append((operation, params))
        if self.fail_submission:
            raise RuntimeError("job submission failed")
        return 123

    def submit_checks(
        self,
        checks: list[dict[str, object]],
        operation: str = "run_checks",
        extra: dict[str, object] | None = None,
    ) -> int:
        """Submit a check run the way the registered check tools do."""
        return submit_view_job(
            operation,
            SOURCE,
            lambda _view: {"table_name": SOURCE, **(extra or {})},
            create_view=self.create_view,
            drop_view=self.drop_view,
            submit_job=self.submit,
            checks=checks,
        )


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

    run_id = env.submit_checks(checks, operation, extra)

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
        env.submit_checks([fk_check()], operation)
    assert env.dropped == [PRIMARY_VIEW, REFERENCE_VIEW]


def test_reference_creation_failure_drops_the_monitored_view_and_never_submits() -> None:
    env = SubmissionEnvironment(fail_creation=REFERENCE)
    with pytest.raises(RuntimeError, match=REFERENCE):
        env.submit_checks([fk_check()])
    assert env.dropped == [PRIMARY_VIEW]
    assert env.submitted == []


def test_later_reference_failure_cleans_up_prior_reference_and_monitored_views() -> None:
    env = SubmissionEnvironment(fail_creation="cat.sch.other")
    with pytest.raises(RuntimeError, match="cat.sch.other"):
        env.submit_checks([fk_check(), fk_check("cat.sch.other")])
    assert set(env.dropped) == {PRIMARY_VIEW, REFERENCE_VIEW}
    assert len(env.dropped) == 2
    assert env.submitted == []


def test_invalid_later_reference_cleans_up_every_acquired_view() -> None:
    env = SubmissionEnvironment()
    with pytest.raises(ValueError, match="Invalid reference table name"):
        env.submit_checks([fk_check(), fk_check("sch.invalid")])
    assert env.created == [SOURCE, REFERENCE]
    assert env.dropped == [PRIMARY_VIEW, REFERENCE_VIEW]
    assert not env.submitted


def test_partial_creation_failure_still_attempts_all_drops_when_one_fails() -> None:
    env = SubmissionEnvironment(fail_creation="cat.sch.third", fail_first_drop=True)
    with pytest.raises(RuntimeError, match="cat.sch.third"):
        env.submit_checks([fk_check(), fk_check("cat.sch.second"), fk_check("cat.sch.third")])
    assert env.dropped == [PRIMARY_VIEW, REFERENCE_VIEW, "cat.tmp.v_1234567890_000000000003"]
    assert not env.submitted


def test_failed_drop_does_not_hide_submission_failure_or_skip_other_views() -> None:
    env = SubmissionEnvironment(fail_submission=True, fail_first_drop=True)
    with pytest.raises(RuntimeError, match="job submission failed"):
        env.submit_checks([fk_check()])
    assert env.dropped == [PRIMARY_VIEW, REFERENCE_VIEW]


def test_self_reference_is_not_dropped_twice_on_submission_failure() -> None:
    env = SubmissionEnvironment(fail_submission=True)
    with pytest.raises(RuntimeError, match="job submission failed"):
        env.submit_checks([fk_check(SOURCE)])
    assert env.created == [SOURCE]
    assert env.dropped == [PRIMARY_VIEW]
    assert env.submitted[0][1]["ref_views"] == {SOURCE: PRIMARY_VIEW}


def test_no_reference_checks_keep_the_existing_payload_shape() -> None:
    env = SubmissionEnvironment()
    checks: list[dict[str, object]] = [{"check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
    env.submit_checks(checks)
    assert env.created == [SOURCE]
    assert "ref_views" not in env.submitted[0][1]
    assert env.submitted[0][1]["checks"] == checks


def test_job_without_checks_receives_params_built_from_its_view() -> None:
    env = SubmissionEnvironment()
    submit_view_job(
        "load_checks",
        SOURCE,
        lambda view: {"location": view, "run_config_name": "default"},
        create_view=env.create_view,
        drop_view=env.drop_view,
        submit_job=env.submit,
    )
    assert env.submitted == [
        ("load_checks", {"location": PRIMARY_VIEW, "run_config_name": "default", "view_name": PRIMARY_VIEW})
    ]


def test_job_without_checks_drops_its_view_when_submission_fails() -> None:
    env = SubmissionEnvironment(fail_submission=True)
    with pytest.raises(RuntimeError, match="job submission failed"):
        submit_view_job(
            "profile_table",
            SOURCE,
            lambda _view: {"table_name": SOURCE},
            create_view=env.create_view,
            drop_view=env.drop_view,
            submit_job=env.submit,
        )
    assert env.dropped == [PRIMARY_VIEW]
