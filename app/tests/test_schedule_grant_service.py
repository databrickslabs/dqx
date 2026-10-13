"""Schedule authorization regressions through public APIs and SDK boundaries."""

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.catalog import (
    CatalogInfo,
    CatalogsAPI,
    EffectivePermissionsList,
    EffectivePrivilege,
    EffectivePrivilegeAssignment,
    GrantsAPI,
    Privilege,
    SchemaInfo,
    SchemasAPI,
    TableInfo,
    TablesAPI,
)
from databricks.sdk.service.iam import ComplexValue, CurrentUserAPI, ServicePrincipal, ServicePrincipalsAPI, User
from databricks.sdk.service.jobs import Job, JobSettings, JobsAPI, JobRunAs
from databricks.sdk.service.sql import (
    ColumnInfo,
    ResultData,
    ResultManifest,
    ResultSchema,
    ServiceError,
    StatementExecutionAPI,
    StatementResponse,
    StatementState,
    StatementStatus,
)

from databricks_labs_dqx_app.backend.services.schedule_grant_service import (
    CannotManageError,
    ScheduleGrantService,
    StatementFailedError,
    WarehouseUnavailableError,
    manage_block_detail,
)

FQN = "cat.sch.tbl"
GRANT_COLUMNS = ("principal", "actionType", "objectType", "objectKey")
GRANT_COLUMN_CASES = [
    GRANT_COLUMNS,
    ("principal", "actiontype", "objecttype", "objectkey"),
    ("PRINCIPAL", "ACTIONTYPE", "OBJECTTYPE", "OBJECTKEY"),
    ("Principal", "ActionType", "ObjectType", "ObjectKey"),
]


def sql_response(
    state: StatementState = StatementState.SUCCEEDED,
    rows: list[list[str]] | None = None,
    columns: tuple[str, ...] = GRANT_COLUMNS,
) -> StatementResponse:
    return StatementResponse(
        statement_id="statement-1",
        status=StatementStatus(
            state=state, error=ServiceError(message="rejected") if state == StatementState.FAILED else None
        ),
        manifest=ResultManifest(schema=ResultSchema(columns=[ColumnInfo(name=n) for n in columns])),
        result=ResultData(data_array=rows or []),
    )


def effective(principal: str, privileges: list[Privilege]) -> EffectivePermissionsList:
    return EffectivePermissionsList(
        privilege_assignments=[
            EffectivePrivilegeAssignment(
                principal=principal, privileges=[EffectivePrivilege(privilege=p) for p in privileges]
            )
        ]
    )


@pytest.fixture
def obo() -> WorkspaceClient:
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.current_user = create_autospec(CurrentUserAPI, instance=True)
    ws.current_user.me.return_value = User(user_name="alice@example.com")
    ws.tables = create_autospec(TablesAPI, instance=True)
    ws.schemas = create_autospec(SchemasAPI, instance=True)
    ws.catalogs = create_autospec(CatalogsAPI, instance=True)
    ws.tables.get.return_value = TableInfo()
    ws.schemas.get.return_value = SchemaInfo()
    ws.catalogs.get.return_value = CatalogInfo()
    ws.grants = create_autospec(GrantsAPI, instance=True)
    ws.grants.get_effective.side_effect = PermissionDenied("OBO scope excludes grants REST")
    ws.statement_execution = create_autospec(StatementExecutionAPI, instance=True)
    ws.statement_execution.execute_statement.return_value = sql_response()
    return ws


@pytest.fixture
def sp() -> WorkspaceClient:
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.jobs = create_autospec(JobsAPI, instance=True)
    ws.jobs.get.return_value = Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="runner-sp")))
    ws.current_user = create_autospec(CurrentUserAPI, instance=True)
    ws.current_user.me.return_value = User(user_name="app-sp")
    ws.service_principals = create_autospec(ServicePrincipalsAPI, instance=True)
    ws.service_principals.list.side_effect = lambda *, filter: iter(
        [ServicePrincipal(application_id=filter.split('"')[1], groups=[])]
    )
    ws.grants = create_autospec(GrantsAPI, instance=True)
    ws.grants.get_effective.side_effect = lambda kind, name, *, principal, page_token=None: effective(
        principal, [Privilege.USE_CATALOG] if kind == "catalog" else [Privilege.USE_SCHEMA]
    )
    ws.tables = create_autospec(TablesAPI, instance=True)
    ws.schemas = create_autospec(SchemasAPI, instance=True)
    ws.catalogs = create_autospec(CatalogsAPI, instance=True)
    ws.tables.get.return_value = TableInfo()
    ws.schemas.get.return_value = SchemaInfo()
    ws.catalogs.get.return_value = CatalogInfo()
    ws.statement_execution = create_autospec(StatementExecutionAPI, instance=True)
    ws.statement_execution.execute_statement.return_value = sql_response(StatementState.FAILED)
    return ws


@pytest.fixture
def service(obo: WorkspaceClient, sp: WorkspaceClient, monkeypatch: pytest.MonkeyPatch) -> ScheduleGrantService:
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    return ScheduleGrantService(obo, sp, "123", warehouse_id="warehouse-1")


@pytest.mark.parametrize("principal", ["alice@example.com", "DATA-ENG", "group-id"])
def test_manage_via_show_grants(service: ScheduleGrantService, obo: WorkspaceClient, principal: str) -> None:
    obo.current_user.me.return_value = User(
        user_name="alice@example.com", groups=[ComplexValue(display="data-eng", value="group-id")]
    )
    obo.statement_execution.execute_statement.return_value = sql_response(rows=[[principal, "MANAGE", "TABLE", FQN]])
    assert service.user_can_manage(FQN)
    obo.grants.get_effective.assert_not_called()
    assert any(
        c.kwargs["statement"] == "SHOW GRANTS ON TABLE `cat`.`sch`.`tbl`" and c.kwargs["warehouse_id"] == "warehouse-1"
        for c in obo.statement_execution.execute_statement.call_args_list
    )


@pytest.mark.parametrize("action", ["SELECT", "ALL PRIVILEGES", "MANAGE"])
def test_other_principal_or_all_privileges_never_confers_manage(
    service: ScheduleGrantService, obo: WorkspaceClient, action: str
) -> None:
    principal = "bob@example.com" if action == "MANAGE" else "alice@example.com"
    obo.statement_execution.execute_statement.return_value = sql_response(rows=[[principal, action, "TABLE", FQN]])
    assert not service.user_can_manage(FQN)


@pytest.mark.parametrize("kind", ["tables", "schemas", "catalogs"])
def test_group_owner_can_manage(service: ScheduleGrantService, obo: WorkspaceClient, kind: str) -> None:
    obo.current_user.me.return_value = User(user_name="alice@example.com", groups=[ComplexValue(display="data-eng")])
    api = getattr(obo, kind)
    info = {"tables": TableInfo, "schemas": SchemaInfo, "catalogs": CatalogInfo}[kind]
    api.get.return_value = info(owner="data-eng")
    assert service.user_can_manage(FQN)


@pytest.mark.parametrize("columns", GRANT_COLUMN_CASES)
def test_manage_on_later_sql_chunk(
    service: ScheduleGrantService, obo: WorkspaceClient, columns: tuple[str, ...]
) -> None:
    response = sql_response(rows=[["bob@example.com", "SELECT", "TABLE", FQN]], columns=columns)
    response.result.next_chunk_index = 1
    obo.statement_execution.execute_statement.return_value = response
    obo.statement_execution.get_statement_result_chunk_n.return_value = ResultData(
        data_array=[["alice@example.com", "MANAGE", "TABLE", FQN]]
    )
    assert service.user_can_manage(FQN)
    assert service.manage_holders(FQN) == [{"principal": "alice@example.com", "type": "user"}]


@pytest.mark.parametrize(
    "columns",
    [
        ("unknown", "actionType", "objectType", "objectKey"),
        ("principal", "unknown", "objectType", "objectKey"),
        ("principal", "actionType", "PRINCIPAL", "objectKey"),
        ("principal", "actionType", "ACTIONTYPE", "objectKey"),
    ],
)
def test_unknown_or_ambiguous_grant_columns_fail_closed(
    service: ScheduleGrantService, obo: WorkspaceClient, columns: tuple[str, ...]
) -> None:
    obo.statement_execution.execute_statement.return_value = sql_response(
        rows=[["alice@example.com", "MANAGE", "TABLE", FQN]], columns=columns
    )
    with pytest.raises(WarehouseUnavailableError):
        service.user_can_manage(FQN)


@pytest.mark.parametrize("failure", [PermissionDenied("no metadata"), TimeoutError("unavailable")])
def test_inspection_failure_is_unknown_not_missing(
    service: ScheduleGrantService, obo: WorkspaceClient, failure: Exception
) -> None:
    obo.statement_execution.execute_statement.side_effect = failure
    with pytest.raises(WarehouseUnavailableError):
        service.user_can_manage(FQN)
    result = service.preflight([FQN])
    assert result[0].access_unverified
    assert not result[0].can_manage


def test_unresolved_caller_is_unknown(service: ScheduleGrantService, obo: WorkspaceClient) -> None:
    obo.current_user.me.side_effect = PermissionDenied("no identity")
    with pytest.raises(WarehouseUnavailableError):
        service.user_can_manage(FQN)


@pytest.mark.parametrize("missing_principal", ["app-sp", "runner-sp"])
@pytest.mark.parametrize("kind,privilege", [("catalog", "USE_CATALOG"), ("schema", "USE_SCHEMA")])
def test_missing_source_parent_blocks_even_table_owner(
    service: ScheduleGrantService,
    obo: WorkspaceClient,
    sp: WorkspaceClient,
    missing_principal: str,
    kind: str,
    privilege: str,
) -> None:
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")

    def grants(securable: str, name: str, *, principal: str, page_token: str | None = None) -> EffectivePermissionsList:
        if securable == kind and principal == missing_principal:
            return EffectivePermissionsList(privilege_assignments=[])
        return effective(principal, [Privilege.ALL_PRIVILEGES])

    sp.grants.get_effective.side_effect = grants
    assert not service.can_schedule(FQN)
    preflight = service.preflight([FQN])[0]
    assert not preflight.can_manage
    assert not preflight.access_unverified
    detail = manage_block_detail([(FQN, service.manage_holders(FQN))])
    assert f"GRANT {privilege}" in str(detail)
    assert f"TO `{missing_principal}`" in str(detail)
    with pytest.raises(CannotManageError):
        service.grant_select_precleared(FQN)
    assert not any(
        c.kwargs["statement"].startswith("GRANT") for c in obo.statement_execution.execute_statement.call_args_list
    )


def test_parent_inspection_unknown_blocks_owner(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient
) -> None:
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")
    sp.grants.get_effective.side_effect = PermissionDenied("not visible")
    obo.statement_execution.execute_statement.return_value = sql_response(StatementState.FAILED)
    result = service.preflight([FQN, "cat.sch.other"])
    assert all(r.access_unverified and not r.can_manage for r in result)
    assert sp.grants.get_effective.call_count == 1
    sp.catalogs.get.assert_called_once_with("cat")
    obo.catalogs.get.assert_called_once_with("cat")
    with pytest.raises(WarehouseUnavailableError):
        service.grant_select_precleared(FQN)


@pytest.mark.parametrize("columns", GRANT_COLUMN_CASES)
def test_parent_sql_fallback_is_caller_scoped(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient, columns: tuple[str, ...]
) -> None:
    sp.grants.get_effective.side_effect = PermissionDenied("not visible")

    def sql(*, statement: str, **kwargs: object) -> StatementResponse:
        if statement.startswith("SHOW GRANTS"):
            principal = "runner-sp" if "`runner-sp`" in statement else "app-sp"
            action = "USE CATALOG" if "ON CATALOG" in statement else "USE SCHEMA"
            return sql_response(rows=[[principal, action, "SCHEMA", "cat.sch"]], columns=columns)
        return sql_response()

    obo.statement_execution.execute_statement.side_effect = sql
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")
    result = service.preflight([FQN, "cat.sch.other"])
    assert all(r.can_manage and not r.access_unverified for r in result)
    assert sp.grants.get_effective.call_count == 4
    assert (
        sum(
            c.kwargs["statement"].startswith("SHOW GRANTS")
            for c in obo.statement_execution.execute_statement.call_args_list
        )
        == 4
    )
    assert service.grant_select_precleared(FQN) == ["app-sp", "runner-sp"]
    assert sp.grants.get_effective.call_count == 4
    obo.grants.get_effective.assert_not_called()


def test_parent_owner_and_all_privileges_satisfy_usage(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient
) -> None:
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")
    sp.grants.get_effective.side_effect = lambda kind, name, *, principal, page_token=None: (
        EffectivePermissionsList(privilege_assignments=[])
        if principal == "runner-sp"
        else effective(principal, [Privilege.ALL_PRIVILEGES])
    )
    sp.catalogs.get.return_value = CatalogInfo(owner="runner-sp")
    sp.schemas.get.return_value = SchemaInfo(owner="runner-sp")
    result = service.preflight([FQN, "cat.sch.other"])
    assert all(r.can_manage and not r.access_unverified for r in result)
    assert sp.grants.get_effective.call_count == 4


def test_failed_effective_pagination_is_unknown(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient
) -> None:
    first = effective("app-sp", [Privilege.USE_CATALOG])
    first.next_page_token = "page-2"
    sp.grants.get_effective.side_effect = [first, PermissionDenied("page failed")]
    obo.statement_execution.execute_statement.return_value = sql_response(StatementState.FAILED)
    assert service.preflight([FQN])[0].access_unverified


def test_effective_parent_usage_uses_lowercase_rest_types_and_all_pages(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient
) -> None:
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")
    obo.statement_execution.execute_statement.return_value = sql_response(StatementState.FAILED)

    def grants(securable: str, name: str, *, principal: str, page_token: str | None = None) -> EffectivePermissionsList:
        if securable not in ("catalog", "schema"):
            raise PermissionDenied("Unsupported REST path type")
        if page_token is None:
            return EffectivePermissionsList(privilege_assignments=[], next_page_token="page-2")
        assert page_token == "page-2"
        return effective(principal, [Privilege.USE_CATALOG if securable == "catalog" else Privilege.USE_SCHEMA])

    sp.grants.get_effective.side_effect = grants
    result = service.preflight([FQN, "cat.sch.other"])
    assert all(r.can_manage and not r.access_unverified for r in result)
    assert sp.grants.get_effective.call_count == 8
    assert {
        (c.args, c.kwargs["principal"], c.kwargs["page_token"]) for c in sp.grants.get_effective.call_args_list
    } == {
        (("catalog", "cat"), "app-sp", None),
        (("catalog", "cat"), "app-sp", "page-2"),
        (("schema", "cat.sch"), "app-sp", None),
        (("schema", "cat.sch"), "app-sp", "page-2"),
        (("catalog", "cat"), "runner-sp", None),
        (("catalog", "cat"), "runner-sp", "page-2"),
        (("schema", "cat.sch"), "runner-sp", None),
        (("schema", "cat.sch"), "runner-sp", "page-2"),
    }
    assert not any(
        c.kwargs["statement"].startswith("SHOW GRANTS")
        for c in obo.statement_execution.execute_statement.call_args_list
    )


@pytest.mark.parametrize("run_as", [None, JobRunAs(user_name="someone@example.com"), JobRunAs()])
def test_missing_runner_identity_fails_closed(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient, run_as: JobRunAs | None
) -> None:
    sp.jobs.get.return_value = Job(settings=JobSettings(run_as=run_as))
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")
    with pytest.raises(WarehouseUnavailableError):
        service.grant_select_precleared(FQN)
    assert service.preflight([FQN])[0].access_unverified


def test_failed_runner_select_propagates(service: ScheduleGrantService, obo: WorkspaceClient) -> None:
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")

    def sql(*, statement: str, **kwargs: object) -> StatementResponse:
        return sql_response(StatementState.FAILED if statement.endswith("TO `runner-sp`") else StatementState.SUCCEEDED)

    obo.statement_execution.execute_statement.side_effect = sql
    with pytest.raises(StatementFailedError):
        service.grant_select_precleared(FQN)


async def test_grants_only_select_to_resolved_principals(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient
) -> None:
    assert await service.grant_select_precleared_async(FQN) == ["app-sp", "runner-sp"]
    assert [c.kwargs["statement"] for c in obo.statement_execution.execute_statement.call_args_list] == [
        "GRANT SELECT ON TABLE `cat`.`sch`.`tbl` TO `app-sp`",
        "GRANT SELECT ON TABLE `cat`.`sch`.`tbl` TO `runner-sp`",
    ]
    assert sp.jobs.get.call_count == 1


def test_same_runner_readable_shortcut(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient
) -> None:
    sp.jobs.get.return_value = Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="app-sp")))
    sp.statement_execution.execute_statement.return_value = sql_response()
    assert service.can_schedule(FQN)
    assert service.grant_select_to_schedulers(FQN) == []


def test_polling_via_public_read_probe(service: ScheduleGrantService, obo: WorkspaceClient) -> None:
    obo.statement_execution.execute_statement.return_value = StatementResponse(statement_id="statement-1")
    obo.statement_execution.get_statement.return_value = sql_response()
    assert service.user_can_read(FQN)


def test_preflight_deduplicates_and_skips_synthetic(service: ScheduleGrantService) -> None:
    result = service.preflight(["", "__sql_check__/x", "__sql_check__/x", "invalid"])
    assert [(r.fqn, r.can_manage) for r in result] == [("__sql_check__/x", True), ("invalid", False)]
    assert service.grant_select_precleared("__sql_check__/x") == []


@pytest.mark.parametrize("parent_usage_via_ownership", [False, True])
def test_preflight_reuses_parent_owner_metadata(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient, parent_usage_via_ownership: bool
) -> None:
    obo.catalogs.get.return_value = CatalogInfo(owner="alice@example.com")
    obo.schemas.get.return_value = SchemaInfo(owner="alice@example.com")
    if parent_usage_via_ownership:
        sp.grants.get_effective.side_effect = lambda kind, name, *, principal, page_token=None: effective(principal, [])
        sp.catalogs.get.return_value = CatalogInfo(owner="scheduler-group")
        sp.schemas.get.return_value = SchemaInfo(owner="scheduler-group")
        sp.service_principals.list.side_effect = lambda *, filter: iter(
            [ServicePrincipal(application_id=filter.split('"')[1], groups=[ComplexValue(display="scheduler-group")])]
        )

    result = service.preflight([FQN, "cat.sch.other"])
    assert all(r.can_manage and not r.access_unverified for r in result)
    assert sp.grants.get_effective.call_count == 4
    obo.catalogs.get.assert_called_once_with("cat")
    obo.schemas.get.assert_called_once_with("cat.sch")
    assert obo.tables.get.call_count == 2
    if parent_usage_via_ownership:
        sp.catalogs.get.assert_called_once_with("cat")
        sp.schemas.get.assert_called_once_with("cat.sch")
        assert sp.service_principals.list.call_count == 2


def test_parent_cache_separates_securables_and_principals(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient
) -> None:
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")

    def grants(securable: str, name: str, *, principal: str, page_token: str | None = None) -> EffectivePermissionsList:
        privileges = [] if name == "cat.sch" and principal == "runner-sp" else [Privilege.ALL_PRIVILEGES]
        return effective(principal, privileges)

    sp.grants.get_effective.side_effect = grants
    result = service.preflight([FQN, "cat.sch.other", "cat.other.tbl", "other.sch.tbl"])
    assert [(r.can_manage, r.access_unverified) for r in result] == [
        (False, False),
        (False, False),
        (True, False),
        (True, False),
    ]
    assert (
        result[0].manage_holders
        == result[1].manage_holders
        == [
            {
                "principal": "runner-sp",
                "type": "service_principal",
                "remediation": "GRANT USE_SCHEMA ON SCHEMA `cat`.`sch` TO `runner-sp`;",
            }
        ]
    )
    assert sp.grants.get_effective.call_count == 10


@pytest.mark.parametrize("parent_usage_via_ownership", [False, True])
@pytest.mark.parametrize("inspection_unknown", [False, True])
def test_new_request_rechecks_revoked_parent_access(
    service: ScheduleGrantService,
    obo: WorkspaceClient,
    sp: WorkspaceClient,
    parent_usage_via_ownership: bool,
    inspection_unknown: bool,
) -> None:
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")
    if parent_usage_via_ownership:
        sp.grants.get_effective.side_effect = lambda kind, name, *, principal, page_token=None: effective(principal, [])
        sp.catalogs.get.return_value = CatalogInfo(owner="scheduler-group")
        sp.schemas.get.return_value = SchemaInfo(owner="scheduler-group")
        sp.service_principals.list.side_effect = lambda *, filter: iter(
            [ServicePrincipal(application_id=filter.split('"')[1], groups=[ComplexValue(display="scheduler-group")])]
        )
    assert service.preflight([FQN])[0].can_manage

    sp.catalogs.get.return_value = CatalogInfo()
    sp.schemas.get.return_value = SchemaInfo()
    if inspection_unknown:
        sp.grants.get_effective.side_effect = PermissionDenied("not visible")
        obo.statement_execution.execute_statement.return_value = sql_response(StatementState.FAILED)
    else:
        sp.grants.get_effective.side_effect = lambda kind, name, *, principal, page_token=None: effective(principal, [])

    next_request = ScheduleGrantService(obo, sp, "123", warehouse_id="warehouse-1")
    result = next_request.preflight([FQN])[0]
    assert not result.can_manage
    assert result.access_unverified is inspection_unknown


def test_manage_holders_are_owners_and_managers(service: ScheduleGrantService, obo: WorkspaceClient) -> None:
    obo.tables.get.return_value = TableInfo(owner="data-eng")
    obo.statement_execution.execute_statement.return_value = sql_response(
        rows=[
            ["bob@example.com", "MANAGE", "TABLE", FQN],
            ["reader@example.com", "SELECT", "TABLE", FQN],
            ["37f263d6-1794-4873-8347-1f919b834fff", "MANAGE", "TABLE", FQN],
        ]
    )
    assert {h["principal"]: h["type"] for h in service.manage_holders(FQN)} == {
        "data-eng": "group",
        "bob@example.com": "user",
        "37f263d6-1794-4873-8347-1f919b834fff": "service_principal",
    }


def test_parent_ownership_proves_usage_when_grants_unavailable(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient
) -> None:
    sp.jobs.get.return_value = Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="app-sp")))
    sp.grants.get_effective.side_effect = PermissionDenied("not visible")
    sp.catalogs.get.return_value = CatalogInfo(owner="app-sp")
    sp.schemas.get.return_value = SchemaInfo(owner="app-sp")
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")
    obo.statement_execution.execute_statement.return_value = sql_response(StatementState.FAILED)
    assert service.can_schedule(FQN)


def test_group_owned_parents_satisfy_usage_without_impersonation(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient
) -> None:
    sp.grants.get_effective.side_effect = lambda kind, name, *, principal, page_token=None: effective(principal, [])
    sp.service_principals.list.side_effect = lambda *, filter: iter(
        [ServicePrincipal(application_id=filter.split('"')[1], groups=[ComplexValue(display="scheduler-group")])]
    )
    sp.catalogs.get.return_value = CatalogInfo(owner="scheduler-group")
    sp.schemas.get.return_value = SchemaInfo(owner="scheduler-group")
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")
    assert service.can_schedule(FQN)


def test_sql_fallback_preserves_group_parent_usage(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient
) -> None:
    sp.grants.get_effective.side_effect = PermissionDenied("not visible")
    sp.service_principals.list.side_effect = lambda *, filter: iter(
        [ServicePrincipal(application_id=filter.split('"')[1], groups=[ComplexValue(display="scheduler-group")])]
    )

    def sql(*, statement: str, **kwargs: object) -> StatementResponse:
        action = "USE CATALOG" if "ON CATALOG" in statement else "USE SCHEMA"
        return sql_response(rows=[["scheduler-group", action, "CATALOG", "cat"]])

    obo.statement_execution.execute_statement.side_effect = sql
    obo.tables.get.return_value = TableInfo(owner="alice@example.com")
    assert service.can_schedule(FQN)


def test_truncated_sql_grants_are_unknown(service: ScheduleGrantService, obo: WorkspaceClient) -> None:
    response = sql_response(rows=[["alice@example.com", "MANAGE", "TABLE", FQN]])
    response.manifest.truncated = True
    obo.statement_execution.execute_statement.return_value = response
    with pytest.raises(WarehouseUnavailableError):
        service.user_can_manage(FQN)


def test_partial_grants_never_authorize(service: ScheduleGrantService, obo: WorkspaceClient) -> None:
    response = sql_response(rows=[["alice@example.com", "MANAGE", "TABLE", FQN]])
    response.result.next_chunk_index = 1
    obo.statement_execution.execute_statement.return_value = response
    obo.statement_execution.get_statement_result_chunk_n.side_effect = PermissionDenied("chunk unavailable")
    with pytest.raises(WarehouseUnavailableError):
        service.user_can_manage(FQN)


def test_readable_app_cannot_bypass_missing_parent(
    service: ScheduleGrantService, obo: WorkspaceClient, sp: WorkspaceClient
) -> None:
    sp.jobs.get.return_value = Job(settings=JobSettings(run_as=JobRunAs(service_principal_name="app-sp")))
    sp.statement_execution.execute_statement.return_value = sql_response()
    sp.grants.get_effective.side_effect = lambda kind, name, *, principal, page_token=None: effective(principal, [])
    assert not service.can_schedule(FQN)
    with pytest.raises(CannotManageError):
        service.grant_select_to_schedulers(FQN)
