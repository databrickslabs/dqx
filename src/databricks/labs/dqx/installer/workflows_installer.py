from __future__ import annotations

import logging
import os.path
import re
import sys
from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.parallel import ManyError
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    Aborted,
    AlreadyExists,
    BadRequest,
    Cancelled,
    DataLoss,
    DeadlineExceeded,
    InternalError,
    InvalidParameterValue,
    NotFound,
    OperationFailed,
    PermissionDenied,
    RequestLimitExceeded,
    ResourceAlreadyExists,
    ResourceConflict,
    ResourceDoesNotExist,
    ResourceExhausted,
    TemporarilyUnavailable,
    TooManyRequests,
    Unauthenticated,
    Unknown,
)
from databricks.sdk.retries import retried
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.jobs import Run
from databricks.sdk.service.workspace import ObjectType

import databricks
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.installer.workflow_task import Task
from databricks.labs.dqx.installer.mixins import InstallationMixin
from databricks.labs.dqx.installer.logs import PartialLogRecord, parse_logs


logger = logging.getLogger(__name__)

TEST_RESOURCE_PURGE_TIMEOUT = timedelta(hours=1)
TEST_NIGHTLY_CI_RESOURCES_PURGE_TIMEOUT = timedelta(hours=3)  # Buffer for debugging nightly integration test runs
EXTRA_TASK_PARAMS = {
    "job_id": "{{job_id}}",
    "run_id": "{{run_id}}",
    "start_time": "{{job.start_time.iso_datetime}}",
    "attempt": "{{job.repair_count}}",
    "parent_run_id": "{{parent_run_id}}",
}


class DeployedWorkflows:
    def __init__(self, ws: WorkspaceClient, install_state: InstallState):
        self._ws = ws
        self._install_state = install_state

    def run_workflow(
        self,
        workflow: str,
        run_config_name: str,
        max_wait: timedelta = timedelta(minutes=20),
    ) -> int:
        # this dunder variable is hiding this method from tracebacks, making it cleaner
        # for the user to see the actual error without too much noise.
        __tracebackhide__ = True
        logger.debug(__tracebackhide__)

        job_id = int(self._install_state.jobs[workflow])
        logger.debug(f"starting {workflow} workflow: {self._ws.config.host}#job/{job_id}")
        job_initial_run = self._ws.jobs.run_now(job_id, python_named_params={"run_config_name": run_config_name})
        run_id = job_initial_run.run_id
        run_url = f"{self._ws.config.host}#job/{job_id}/runs/{run_id}"
        logger.info(f"Started {workflow} workflow: {run_url}")

        try:
            logger.debug(f"Waiting for completion of {workflow} workflow: {run_url}")
            job_run = self._ws.jobs.wait_get_run_job_terminated_or_skipped(run_id=run_id, timeout=max_wait)
            self._log_completed_job(workflow, run_id, job_run)
            logger.info('---------- REMOTE LOGS --------------')
            self._relay_logs(workflow, run_id)
            logger.info('---------- END REMOTE LOGS ----------')
            return run_id
        except TimeoutError:
            logger.warning(f"Timeout while waiting for {workflow} workflow to complete: {run_url}")
            logger.info('---------- REMOTE LOGS --------------')
            self._relay_logs(workflow, run_id)
            logger.info('------ END REMOTE LOGS (SO FAR) -----')
            raise
        except OperationFailed as err:
            logger.info('---------- REMOTE LOGS --------------')
            self._relay_logs(workflow, run_id)
            logger.info('---------- END REMOTE LOGS ----------')
            job_run = self._ws.jobs.get_run(run_id)
            raise self._infer_error_from_job_run(job_run) from err

    @staticmethod
    def _log_completed_job(step: str, run_id: int, job_run: Run) -> None:
        if job_run.state:
            result_state = job_run.state.result_state or "N/A"
            state_message = job_run.state.state_message
            state_description = f"{result_state} ({state_message})" if state_message else f"{result_state}"
            logger.info(f"Completed {step} workflow run {run_id} with state: {state_description}")
        else:
            logger.warning(f"Completed {step} workflow run {run_id} but end state is unknown.")
        if job_run.start_time or job_run.end_time:
            start_time = (
                datetime.fromtimestamp(job_run.start_time / 1000, tz=timezone.utc) if job_run.start_time else None
            )
            end_time = datetime.fromtimestamp(job_run.end_time / 1000, tz=timezone.utc) if job_run.end_time else None
            if job_run.run_duration:
                duration = timedelta(milliseconds=job_run.run_duration)
            elif start_time and end_time:
                duration = end_time - start_time
            else:
                duration = None
            logger.info(
                f"Completed {step} workflow run {run_id} duration: {duration or 'N/A'} ({start_time or 'N/A'} thru {end_time or 'N/A'})"
            )

    def latest_job_status(self) -> list[dict]:
        latest_status = []
        for job, job_id in self._install_state.jobs.items():
            job_state = None
            start_time = None
            try:
                job_runs = list(self._ws.jobs.list_runs(job_id=int(job_id), limit=1))
            except InvalidParameterValue as e:
                logger.warning(f"skipping {job}: {e}")
                continue
            if job_runs:
                state = job_runs[0].state
                if state and state.result_state:
                    job_state = state.result_state.name
                elif state and state.life_cycle_state:
                    job_state = state.life_cycle_state.name
                if job_runs[0].start_time:
                    start_time = job_runs[0].start_time / 1000
            latest_status.append(
                {
                    "workflow": job,
                    "workflow_id": job_id,
                    "state": "UNKNOWN" if not (job_runs and job_state) else job_state,
                    "started": (
                        "<never run>" if not (job_runs and start_time) else self._readable_timedelta(start_time)
                    ),
                }
            )
        return latest_status

    def relay_logs(self, workflow: str | None = None):
        latest_run = None
        if not workflow:
            runs = []
            for step in self._install_state.jobs:
                try:
                    _, latest_run = self._latest_job_run(step)
                    runs.append((step, latest_run))
                except InvalidParameterValue:
                    continue
            if not runs:
                logger.warning("No jobs to relay logs for")
                return
            runs = sorted(runs, key=lambda x: x[1].start_time, reverse=True)
            workflow, latest_run = runs[0]
        if not latest_run:
            assert workflow is not None
            _, latest_run = self._latest_job_run(workflow)
        self._relay_logs(workflow, latest_run.run_id)

    def _relay_logs(self, workflow, run_id):
        for record in self._fetch_last_run_attempt_logs(workflow, run_id):
            task_logger = logging.getLogger(record.component)
            MaxedStreamHandler.install_handler(task_logger)
            task_logger.setLevel(logger.getEffectiveLevel())
            log_level = logging.getLevelName(record.level)
            task_logger.log(log_level, record.message)
        MaxedStreamHandler.uninstall_handlers()

    def _fetch_last_run_attempt_logs(self, workflow: str, run_id: str) -> Iterator[PartialLogRecord]:
        """Fetch the logs for the last run attempt."""
        run_folders = self._get_log_run_folders(workflow, run_id)
        if not run_folders:
            return
        # sort folders based on the last repair attempt
        last_attempt = sorted(run_folders, key=lambda _: int(_.split('-')[-1]), reverse=True)[0]
        for object_info in self._ws.workspace.list(last_attempt):
            if not object_info.path:
                continue
            if '.log' not in object_info.path:
                continue
            task_name = os.path.basename(object_info.path).split('.')[0]
            with self._ws.workspace.download(object_info.path) as raw_file:
                text_io = StringIO(raw_file.read().decode())
            for record in parse_logs(text_io):
                yield replace(record, component=f'{record.component}:{task_name}')

    def _get_log_run_folders(self, workflow: str, run_id: str) -> list[str]:
        """Get the log run folders.

        The log run folders are located in the installation folder under the logs directory. Each job has a log run
        folder for each run id. Multiple runs occur for repair runs.
        """
        log_path = f"{self._install_state.install_folder()}/logs/{workflow}"
        try:
            # Ensure any exception is triggered early.
            log_path_objects = list(self._ws.workspace.list(log_path))
        except ResourceDoesNotExist:
            logger.warning(f"Cannot fetch logs as folder {log_path} does not exist")
            return []
        run_folders = []
        for run_folder in log_path_objects:
            if not run_folder.path or run_folder.object_type != ObjectType.DIRECTORY:
                continue
            if f"run-{run_id}-" not in run_folder.path:
                continue
            run_folders.append(run_folder.path)
        return run_folders

    @staticmethod
    def _readable_timedelta(epoch):
        when = datetime.utcfromtimestamp(epoch)
        duration = datetime.now() - when
        data = {}
        data["days"], remaining = divmod(duration.total_seconds(), 86_400)
        data["hours"], remaining = divmod(remaining, 3_600)
        data["minutes"], data["seconds"] = divmod(remaining, 60)

        time_parts = ((name, round(value)) for (name, value) in data.items())
        time_parts = [f"{value} {name[:-1] if value == 1 else name}" for name, value in time_parts if value > 0]
        if len(time_parts) > 0:
            time_parts.append("ago")
        if time_parts:
            return " ".join(time_parts)
        return "less than 1 second ago"

    def _latest_job_run(self, workflow: str):
        job_id = self._install_state.jobs.get(workflow)
        if not job_id:
            raise InvalidParameterValue("job does not exists hence skipping repair")
        job_runs = list(self._ws.jobs.list_runs(job_id=job_id, limit=1))
        if not job_runs:
            raise InvalidParameterValue("job is not initialized yet. Can't trigger repair run now")
        latest_job_run = job_runs[0]
        return job_id, latest_job_run

    def _infer_error_from_job_run(self, job_run) -> Exception:
        errors: list[Exception] = []
        timeouts: list[DeadlineExceeded] = []
        assert job_run.tasks is not None
        for run_task in job_run.tasks:
            error = self._infer_error_from_task_run(run_task)
            if not error:
                continue
            if isinstance(error, DeadlineExceeded):
                timeouts.append(error)
                continue
            errors.append(error)
        assert job_run.state is not None
        assert job_run.state.state_message is not None
        if len(errors) == 1:
            return errors[0]
        all_errors = errors + timeouts
        if len(all_errors) == 0:
            return Unknown(job_run.state.state_message)
        return ManyError(all_errors)

    def _infer_error_from_task_run(self, run_task: jobs.RunTask) -> Exception | None:
        if not run_task.state:
            return None
        if run_task.state.result_state == jobs.RunResultState.TIMEDOUT:
            msg = f"{run_task.task_key}: The run was stopped after reaching the timeout"
            return DeadlineExceeded(msg)
        if run_task.state.result_state != jobs.RunResultState.FAILED:
            return None
        assert run_task.run_id is not None
        run_output = self._ws.jobs.get_run_output(run_task.run_id)
        if not run_output:
            msg = f'No run output. {run_task.state.state_message}'
            return InternalError(msg)
        if logger.isEnabledFor(logging.DEBUG):
            if run_output.error_trace:
                sys.stderr.write(run_output.error_trace)
        if not run_output.error:
            msg = f'No error in run output. {run_task.state.state_message}'
            return InternalError(msg)
        return self._infer_task_exception(f"{run_task.task_key}: {run_output.error}")

    @staticmethod
    def _infer_task_exception(haystack: str) -> Exception:
        needles: list[type[Exception]] = [
            BadRequest,
            Unauthenticated,
            PermissionDenied,
            NotFound,
            ResourceConflict,
            TooManyRequests,
            Cancelled,
            databricks.sdk.errors.NotImplemented,
            InternalError,
            TemporarilyUnavailable,
            DeadlineExceeded,
            InvalidParameterValue,
            ResourceDoesNotExist,
            Aborted,
            AlreadyExists,
            ResourceAlreadyExists,
            ResourceExhausted,
            RequestLimitExceeded,
            Unknown,
            DataLoss,
            ValueError,
            KeyError,
        ]
        constructors: dict[re.Pattern, type[Exception]] = {
            re.compile(r".*\[TimeoutException] (.*)"): TimeoutError,
        }
        for klass in needles:
            constructors[re.compile(f".*{klass.__name__}: (.*)")] = klass
        for pattern, klass in constructors.items():
            match = pattern.match(haystack)
            if match:
                return klass(match.group(1))
        return Unknown(haystack)


class WorkflowsDeployment(InstallationMixin):
    def __init__(
        self,
        config: WorkspaceConfig,
        run_config_name: str,
        installation: Installation,
        install_state: InstallState,
        ws: WorkspaceClient,
        wheels: WheelsV2,
        product_info: ProductInfo,
        tasks: list[Task],
    ):
        self._config = config
        self._run_config = self._config.get_run_config(run_config_name)
        self._installation = installation
        self._ws = ws
        self._install_state = install_state
        self._wheels = wheels
        self._product_info = product_info
        self._tasks = tasks
        self._this_file = Path(__file__)
        super().__init__(config, installation, ws)

    def create_jobs(self) -> None:
        remote_wheels = self._upload_wheel()

        for task in self._tasks:
            settings = self._job_settings(task.workflow, remote_wheels, task.spark_conf)
            if task.override_clusters:
                settings = self._apply_cluster_overrides(
                    settings,
                    task.override_clusters,  # e.g. {"main": "0709-132523-cnhxf2p6"}
                )
            self._deploy_workflow(task.workflow, settings)

        self.remove_jobs(keep={task.workflow for task in self._tasks})
        self._install_state.save()

    def remove_jobs(self, *, keep: set[str] | None = None) -> None:
        for workflow_name, job_id in self._install_state.jobs.items():
            if keep and workflow_name in keep:
                continue
            try:
                if not self._is_managed_job_failsafe(int(job_id)):
                    logger.warning(f"Corrupt installation state. Skipping job_id={job_id} as it is not managed by DQX")
                    continue
                logger.info(f"Removing job_id={job_id}, as it is no longer needed")
                self._ws.jobs.delete(job_id)
            except InvalidParameterValue:
                logger.warning(f"step={workflow_name} does not exist anymore for some reason")
                continue

    def _is_testing(self):
        return self._product_info.product_name() != "dqx"

    @staticmethod
    def _is_nightly():
        ci_env = os.getenv("TEST_NIGHTLY")
        return ci_env is not None and ci_env.lower() == "true"

    @classmethod
    def _get_test_purge_time(cls) -> str:
        # Duplicate of mixins.fixtures.get_test_purge_time(); we don't want to import pytest as a transitive dependency.
        timeout = TEST_NIGHTLY_CI_RESOURCES_PURGE_TIMEOUT if cls._is_nightly() else TEST_RESOURCE_PURGE_TIMEOUT
        now = datetime.now(timezone.utc)
        purge_deadline = now + timeout
        # Round UP to the next hour boundary: that is when resources will be deleted.
        purge_hour = purge_deadline + (datetime.min.replace(tzinfo=timezone.utc) - purge_deadline) % timedelta(hours=1)
        return purge_hour.strftime("%Y%m%d%H")

    def _is_managed_job_failsafe(self, job_id: int) -> bool:
        try:
            return self._is_managed_job(job_id)
        except ResourceDoesNotExist:
            return False
        except InvalidParameterValue:
            return False

    def _is_managed_job(self, job_id: int) -> bool:
        job = self._ws.jobs.get(job_id)
        if not job.settings or not job.settings.tasks:
            return False
        for task in job.settings.tasks:
            if task.python_wheel_task and task.python_wheel_task.package_name == "databricks_labs_dqx":
                return True
        return False

    @property
    def _config_file(self):
        return f"{self._installation.install_folder()}/config.yml"

    def _job_cluster_spark_conf(self, cluster_key: str, spark_conf: dict[str, str] | None = None):
        conf_from_installation = spark_conf if spark_conf else {}
        if cluster_key == "main":
            spark_conf = {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]",
            }
            return spark_conf | conf_from_installation
        return conf_from_installation

    # Workflow creation might fail on an InternalError with no message
    @retried(on=[InternalError], timeout=timedelta(minutes=2))
    def _deploy_workflow(self, step_name: str, settings):
        if step_name in self._install_state.jobs:
            try:
                job_id = int(self._install_state.jobs[step_name])
                logger.info(f"Updating configuration for step={step_name} job_id={job_id}")
                return self._ws.jobs.reset(job_id, jobs.JobSettings(**settings))
            except InvalidParameterValue:
                del self._install_state.jobs[step_name]
                logger.warning(f"step={step_name} does not exist anymore for some reason")
                return self._deploy_workflow(step_name, settings)
        logger.info(f"Creating new job configuration for step={step_name}")
        new_job = self._ws.jobs.create(**settings)
        assert new_job.job_id is not None
        self._install_state.jobs[step_name] = str(new_job.job_id)
        return None

    @staticmethod
    def _library_dep_order(library: str):
        match library:
            case library if 'sdk' in library:
                return 0
            case library if 'blueprint' in library:
                return 1
            case _:
                return 2

    def _upload_wheel(self):
        wheel_paths = []
        with self._wheels:
            wheel_paths.sort(key=WorkflowsDeployment._library_dep_order)
            wheel_paths.append(self._wheels.upload_to_wsfs())
            wheel_paths = [f"/Workspace{wheel}" for wheel in wheel_paths]
            return wheel_paths

    @staticmethod
    def _apply_cluster_overrides(
        settings: dict[str, Any],
        overrides: dict[str, str],
    ) -> dict:
        settings["job_clusters"] = [_ for _ in settings["job_clusters"] if _.job_cluster_key not in overrides]
        for job_task in settings["tasks"]:
            if job_task.job_cluster_key is None:
                continue
            if job_task.job_cluster_key in overrides:
                job_task.existing_cluster_id = overrides[job_task.job_cluster_key]
                job_task.job_cluster_key = None
        return settings

    def _job_settings(
        self, step_name: str, remote_wheels: list[str], spark_conf: dict[str, str] | None = None
    ) -> dict[str, Any]:
        email_notifications = None
        if not self._is_testing() and "@" in self._my_username:
            # set email notifications only if we're running the real installation and not tests
            email_notifications = jobs.JobEmailNotifications(
                on_success=[self._my_username], on_failure=[self._my_username]
            )

        job_tasks = []
        job_clusters: set[str] = {Task.job_cluster}
        for task in self._tasks:
            if task.workflow != step_name:
                continue
            job_clusters.add(task.job_cluster)
            job_tasks.append(self._job_task(task, remote_wheels))

        version = self._product_info.version()
        version = version if not self._ws.config.is_gcp else version.replace("+", "-")
        tags = {"version": f"v{version}"}
        if self._is_testing():
            # add RemoveAfter tag for test job cleanup
            date_to_remove = self._get_test_purge_time()
            tags.update({"RemoveAfter": date_to_remove})
        return {
            "name": self._name(step_name),
            "tags": tags,
            "job_clusters": self._job_clusters(job_clusters, spark_conf),
            "email_notifications": email_notifications,
            "tasks": job_tasks,
        }

    def _job_task(self, task: Task, remote_wheels: list[str]) -> jobs.Task:
        jobs_task = jobs.Task(
            task_key=task.name,
            job_cluster_key=task.job_cluster,
            depends_on=[jobs.TaskDependency(task_key=d) for d in task.dependencies()],
        )
        return self._job_wheel_task(jobs_task, task.workflow, remote_wheels)

    def _job_wheel_task(self, jobs_task: jobs.Task, workflow: str, remote_wheels: list[str]) -> jobs.Task:
        libraries = []
        for wheel in remote_wheels:
            libraries.append(compute.Library(whl=wheel))
        named_parameters = {
            "config": f"/Workspace{self._config_file}",
            "run_config_name": self._run_config.name,
            "workflow": workflow,
            "task": jobs_task.task_key,
        }
        return replace(
            jobs_task,
            libraries=libraries,
            python_wheel_task=jobs.PythonWheelTask(
                package_name="databricks_labs_dqx",
                entry_point="runtime",  # [project.entry-points.databricks] in pyproject.toml
                named_parameters=named_parameters | EXTRA_TASK_PARAMS,
            ),
        )

    def _job_clusters(self, job_clusters: set[str], spark_conf: dict[str, str] | None = None):
        clusters = []
        if "main" in job_clusters:
            latest_lts_dbr = self._ws.clusters.select_spark_version(latest=True, long_term_support=True)
            node_type_id = self._ws.clusters.select_node_type(
                local_disk=True, min_memory_gb=16, min_cores=4, photon_worker_capable=True, is_io_cache_enabled=True
            )
            clusters = [
                jobs.JobCluster(
                    job_cluster_key="main",
                    new_cluster=compute.ClusterSpec(
                        spark_version=latest_lts_dbr,
                        node_type_id=node_type_id,
                        data_security_mode=compute.DataSecurityMode.SINGLE_USER,
                        spark_conf=self._job_cluster_spark_conf("main", spark_conf),
                        custom_tags={"ResourceClass": "SingleNode"},
                        num_workers=0,
                    ),
                )
            ]
        return clusters


class MaxedStreamHandler(logging.StreamHandler):

    MAX_STREAM_SIZE = 2**20 - 2**6  # 1 Mb minus some buffer
    _installed_handlers: dict[str, tuple[logging.Logger, MaxedStreamHandler]] = {}
    _sent_bytes = 0

    @classmethod
    def install_handler(cls, logger_: logging.Logger):
        if logger_.handlers:
            # already installed ?
            installed = next((h for h in logger_.handlers if isinstance(h, MaxedStreamHandler)), None)
            if installed:
                return
            # any handler to override ?
            handler = next((h for h in logger_.handlers if isinstance(h, logging.StreamHandler)), None)
            if handler:
                to_install = MaxedStreamHandler(handler)
                cls._installed_handlers[logger_.name] = (logger_, to_install)
                logger_.removeHandler(handler)
                logger_.addHandler(to_install)
                return
        if logger_.parent:
            cls.install_handler(logger_.parent)
        if logger_.root:
            cls.install_handler(logger_.root)

    @classmethod
    def uninstall_handlers(cls):
        for logger_, handler in cls._installed_handlers.values():
            logger_.removeHandler(handler)
            logger_.addHandler(handler.original_handler)
        cls._installed_handlers.clear()
        cls._sent_bytes = 0

    def __init__(self, original_handler: logging.StreamHandler):
        super().__init__()
        self._original_handler = original_handler

    @property
    def original_handler(self):
        return self._original_handler

    def emit(self, record):
        try:
            msg = self.format(record) + self.terminator
            if self._prevent_overflow(msg):
                return
            self.stream.write(msg)
            self.flush()
        except RecursionError:  # See issue 36272
            raise
        # the below is copied from Python source
        # so ensuring not to break the logging logic
        except Exception:
            self.handleError(record)

    def _prevent_overflow(self, msg: str):
        data = msg.encode("utf-8")
        if self._sent_bytes + len(data) > self.MAX_STREAM_SIZE:
            # ensure readers are aware of why the logs are incomplete
            self.stream.write(f"MAX LOGS SIZE REACHED: {self._sent_bytes} bytes!!!")
            self.flush()
            return True
        return False
