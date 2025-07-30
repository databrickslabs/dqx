import pytest
from databricks.labs.dqx.installer.workflow_task import workflow_task, Task, Workflow, remove_extra_indentation

def test_dependencies():
    task_with_deps = Task(
        workflow="test_workflow",
        name="test_task",
        doc="Test task with dependencies",
        fn=lambda x: x,
        depends_on=["task1", "task2"],
    )

    task_without_deps = Task(
        workflow="test_workflow", name="test_task", doc="Test task without dependencies", fn=lambda x: x
    )

    assert task_with_deps.dependencies() == ["task1", "task2"]
    assert task_without_deps.dependencies() == []


def test_workflow_task_decorator():
    @workflow_task
    def sample_task():
        """Sample task"""

    assert hasattr(sample_task, "__task__")
    task = sample_task.__task__
    assert task.name == "sample_task"
    assert task.doc == "Sample task"
    assert task.fn
    assert task.depends_on == []
    assert task.job_cluster == "main"


def test_workflow_task_register_task_without_doc():
    with pytest.raises(SyntaxError, match="must have some doc comment"):

        @workflow_task
        def task_without_docstring():
            pass


def test_workflow_task_raises_syntax_error_for_depends_on():
    with pytest.raises(SyntaxError, match="depends_on has to be a list"):

        @workflow_task(depends_on="not_a_list")
        def task_with_invalid_depends_on():
            """Task with invalid depends_on"""


class WorkflowTest(Workflow):
    @workflow_task
    def dependency_task(self):
        """Dependency task"""

    @workflow_task(depends_on=[dependency_task])
    def main_task(self):
        """Main task"""


def test_workflow_task_decorator_with_dependencies():
    main_task = WorkflowTest("test").main_task
    assert hasattr(main_task, "__task__")
    task = main_task.__task__
    assert task.name == "main_task"
    assert task.doc == "Main task"
    assert task.fn
    assert task.depends_on == ["dependency_task"]
    assert task.job_cluster == "main"


def test_workflow_task_returns_register():
    decorator = workflow_task()
    assert callable(decorator)
    assert decorator.__name__ == "register"


def test_remove_extra_indentation_no_indentation():
    doc = "This is a test docstring."
    expected = "This is a test docstring."
    assert remove_extra_indentation(doc) == expected


def test_remove_extra_indentation_with_indentation():
    doc = "    This is a test docstring with indentation."
    expected = "This is a test docstring with indentation."
    assert remove_extra_indentation(doc) == expected


def test_remove_extra_indentation_mixed_indentation():
    doc = "    This is a test docstring with indentation.\nThis line has no indentation."
    expected = "This is a test docstring with indentation.\nThis line has no indentation."
    assert remove_extra_indentation(doc) == expected


def test_remove_extra_indentation_multiple_lines():
    doc = "    Line one.\n    Line two.\n    Line three."
    expected = "Line one.\nLine two.\nLine three."
    assert remove_extra_indentation(doc) == expected


def test_remove_extra_indentation_empty_string():
    doc = ""
    expected = ""
    assert remove_extra_indentation(doc) == expected
