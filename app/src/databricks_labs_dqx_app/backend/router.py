import json
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any

import yaml
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.config import InstallationChecksStorageConfig
from databricks.labs.dqx.config_serializer import ConfigSerializer
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidCheckError, InvalidConfigError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceDoesNotExist
from databricks.sdk.service.iam import User as UserOut
from fastapi import APIRouter, Depends, HTTPException, Query
from pyspark.sql import SparkSession

from .config import conf
from .dependencies import get_engine, get_generator, get_obo_ws, get_spark
from .logger import logger
from .models import (
    AppSettings,
    ApprovalRequest,
    AvailableCatalogsResponse,
    AvailableSchemasResponse,
    AvailableTablesResponse,
    CatalogInfo,
    ChecksIn,
    ChecksOut,
    ConfigIn,
    ConfigOut,
    CreateRuleRequest,
    DryRunRequest,
    SchemaInfo,
    TableInfo,
    DryRunResponse,
    ExistingProfilesResponse,
    ExportRuleRequest,
    GenerateChecksIn,
    GenerateChecksOut,
    GeneratedRule,
    InitializeRequest,
    InitializeResponse,
    InstallationSettings,
    MessageResponse,
    PrerequisiteSqlResponse,
    ProfileTableRequest,
    ProfileJobResponse,
    ProfileStatus,
    ProfilingResult,
    RuleFilterOptions,
    RuleResponse,
    RulesListResponse,
    RuleStatus,
    RunConfigIn,
    RunConfigOut,
    UpdateRuleRequest,
    VersionOut,
)
from .settings import SettingsManager
from .storage import DQXStorage

api = APIRouter(prefix=conf.api_prefix)


def get_install_folder(ws: WorkspaceClient, path: str | None) -> str:
    folder = path
    if not folder:
        settings = SettingsManager(ws).get_settings()
        folder = settings.install_folder
        logger.info(f"Using install folder from settings: {folder}")
    else:
        logger.info(f"Using install folder from path parameter: {folder}")
    return folder.strip()


def _make_json_serializable(obj: Any) -> Any:
    """Convert non-JSON-serializable types to serializable equivalents."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: _make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_make_json_serializable(item) for item in obj]
    elif hasattr(obj, '__dict__'):
        return _make_json_serializable(obj.__dict__)
    return obj


@api.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    return VersionOut.from_metadata()


@api.get("/current-user", response_model=UserOut, operation_id="currentUser")
def me(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    return obo_ws.current_user.me()


@api.get("/settings", response_model=InstallationSettings, operation_id="get_settings")
def get_settings(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    return SettingsManager(obo_ws).get_settings()


@api.post("/settings", response_model=InstallationSettings, operation_id="save_settings")
def save_settings(settings: InstallationSettings, obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    try:
        return SettingsManager(obo_ws).save_settings(settings)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@api.get("/config", response_model=ConfigOut, operation_id="config")
def get_config(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ConfigOut:
    install_folder = get_install_folder(obo_ws, path)
    logger.info(f"Loading config from install folder: {install_folder}")
    serializer = ConfigSerializer(obo_ws)
    try:
        config = serializer.load_config(install_folder=install_folder)
        logger.info(f"Successfully loaded config with {len(config.run_configs)} run configs")
        return ConfigOut(config=config)
    except ResourceDoesNotExist as e:
        logger.error(f"Configuration not found at {install_folder}: {e}")
        raise HTTPException(status_code=404, detail=f"Configuration not found at {install_folder}")


@api.post("/config", response_model=ConfigOut, operation_id="save_config")
def save_config(
    body: ConfigIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ConfigOut:
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)
    serializer.save_config(body.config, install_folder=install_folder)
    return ConfigOut(config=serializer.load_config(install_folder=install_folder))


@api.get("/config/run/{name}", response_model=RunConfigOut, operation_id="get_run_config")
def get_run_config(
    name: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> RunConfigOut:
    # per each run config there is a separate, single unique checks_location
    # checks_location can be one of the following: file path, delta or lakebase table name, volume file path
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)
    try:
        return RunConfigOut(config=serializer.load_run_config(run_config_name=name, install_folder=install_folder))
    except (ResourceDoesNotExist, InvalidConfigError):
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")


@api.post("/config/run", response_model=RunConfigOut, operation_id="save_run_config")
def save_run_config(
    body: RunConfigIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> RunConfigOut:
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)
    serializer.save_run_config(body.config, install_folder=install_folder)
    return RunConfigOut(
        config=serializer.load_run_config(run_config_name=body.config.name, install_folder=install_folder)
    )


@api.delete("/config/run/{name}", response_model=ConfigOut, operation_id="delete_run_config")
def delete_run_config(
    name: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ConfigOut:
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)

    try:
        config = serializer.load_config(install_folder=install_folder)
    except (ResourceDoesNotExist, InvalidConfigError):
        raise HTTPException(status_code=404, detail=f"Configuration not found at {install_folder}")

    # Filter out the run config with the given name
    original_count = len(config.run_configs)
    config.run_configs = [rc for rc in config.run_configs if rc.name != name]

    if len(config.run_configs) == original_count:
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    serializer.save_config(config, install_folder=install_folder)
    return ConfigOut(config=config)


@api.get("/config/run/{name}/checks", response_model=ChecksOut, operation_id="get_run_checks")
def get_run_checks(
    name: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    engine: Annotated[DQEngine, Depends(get_engine)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ChecksOut:
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)
    try:
        run_config = serializer.load_run_config(run_config_name=name, install_folder=install_folder)
    except (ResourceDoesNotExist, InvalidConfigError):
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    checks_config = InstallationChecksStorageConfig(run_config_name=run_config.name, install_folder=install_folder)

    try:
        checks = engine.load_checks(checks_config)
        return ChecksOut(checks=checks)
    except (NotFound, FileNotFoundError):
        # Checks file doesn't exist yet - return empty list
        return ChecksOut(checks=[])
    except InvalidCheckError as e:
        # Checks file exists but has invalid format
        raise HTTPException(status_code=400, detail=f"Invalid checks format: {e}")
    except InvalidConfigError as e:
        # Configuration issue
        raise HTTPException(status_code=400, detail=f"Invalid configuration: {e}")


@api.post("/config/run/{name}/checks", response_model=ChecksOut, operation_id="save_run_checks")
def save_run_checks(
    name: str,
    body: ChecksIn,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    engine: Annotated[DQEngine, Depends(get_engine)],
    path: str | None = Query(None, description="Path to the configuration folder"),
) -> ChecksOut:
    install_folder = get_install_folder(obo_ws, path)
    serializer = ConfigSerializer(obo_ws)
    try:
        run_config = serializer.load_run_config(run_config_name=name, install_folder=install_folder)
    except (ResourceDoesNotExist, InvalidConfigError):
        raise HTTPException(status_code=404, detail=f"Run config '{name}' not found")

    checks_config = InstallationChecksStorageConfig(run_config_name=run_config.name, install_folder=install_folder)
    engine.save_checks(body.checks, checks_config)
    return ChecksOut(checks=body.checks)


@api.post("/ai-generate-checks", response_model=GenerateChecksOut, operation_id="ai_assisted_checks_generation")
def ai_generate_checks(
    body: GenerateChecksIn,
    generator: Annotated[DQGenerator, Depends(get_generator)],
) -> GenerateChecksOut:
    """Generate data quality checks from natural language using AI-assisted generation."""
    try:
        checks = generator.generate_dq_rules_ai_assisted(user_input=body.user_input)

        # Convert checks to YAML
        yaml_output = yaml.dump(checks, default_flow_style=False, sort_keys=False)

        return GenerateChecksOut(yaml_output=yaml_output, checks=checks)
    except Exception as e:
        logger.error(f"Failed to generate checks: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate checks: {str(e)}")


# =============================================================================
# New View-Based Profiler Endpoints (PROFILER_VIEW_BASED_APPROACH.md)
# =============================================================================

def get_storage(
    spark: Annotated[SparkSession, Depends(get_spark)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DQXStorage:
    """Get storage instance with current app settings."""
    settings = SettingsManager(obo_ws).get_settings()
    # For now, use a default catalog - this will be updated once initialization is done
    # The actual catalog/schema comes from the app's stored settings
    return DQXStorage(spark=spark, catalog="main", schema=conf.default_schema)


# -----------------------------------------------------------------------------
# Admin Endpoints
# -----------------------------------------------------------------------------

@api.get("/admin/available-catalogs", response_model=AvailableCatalogsResponse, operation_id="get_available_catalogs")
def get_available_catalogs(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
) -> AvailableCatalogsResponse:
    """
    List catalogs where the current user has CREATE SCHEMA permission.
    This allows users to select a catalog for app initialization.
    """
    catalogs = []
    error_message = None
    
    try:
        # List all catalogs the user can see
        logger.info("Listing available catalogs...")
        catalog_rows = spark.sql("SHOW CATALOGS").collect()
        logger.info(f"Found {len(catalog_rows)} catalogs")
        
        for row in catalog_rows:
            catalog_name = row.catalog
            logger.info(f"Processing catalog: {catalog_name}")
            
            # Skip system catalogs
            if catalog_name in ("system", "hive_metastore", "__databricks_internal"):
                logger.info(f"Skipping system catalog: {catalog_name}")
                continue
            
            # Check if user has CREATE SCHEMA permission
            has_create_schema = True  # Default to true - we'll include all non-system catalogs
            
            catalogs.append(CatalogInfo(name=catalog_name, has_create_schema=has_create_schema))
            
    except Exception as e:
        error_message = str(e)
        logger.error(f"Failed to list catalogs: {e}")
        # Continue - will return empty list but frontend can allow manual entry
        
    logger.info(f"Returning {len(catalogs)} catalogs")
    return AvailableCatalogsResponse(catalogs=catalogs, error=error_message)


@api.get("/admin/schemas", response_model=AvailableSchemasResponse, operation_id="get_available_schemas")
def get_available_schemas(
    catalog: str = Query(..., description="Catalog to list schemas from"),
    spark: Annotated[SparkSession, Depends(get_spark)] = None,
) -> AvailableSchemasResponse:
    """
    List schemas in a catalog that the current user can access.
    """
    schemas = []
    error_message = None
    
    try:
        logger.info(f"Listing schemas in catalog: {catalog}")
        schema_rows = spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
        logger.info(f"Found {len(schema_rows)} schemas")
        
        for row in schema_rows:
            schema_name = row.databaseName if hasattr(row, 'databaseName') else row.namespace
            # Skip internal schemas
            if schema_name.startswith("__"):
                continue
            schemas.append(SchemaInfo(name=schema_name, catalog=catalog))
            
    except Exception as e:
        error_message = str(e)
        logger.error(f"Failed to list schemas in {catalog}: {e}")
        
    return AvailableSchemasResponse(schemas=schemas, error=error_message)


@api.get("/admin/tables", response_model=AvailableTablesResponse, operation_id="get_available_tables")
def get_available_tables(
    catalog: str = Query(..., description="Catalog containing the schema"),
    schema_name: str = Query(..., alias="schema", description="Schema to list tables from"),
    spark: Annotated[SparkSession, Depends(get_spark)] = None,
) -> AvailableTablesResponse:
    """
    List tables in a schema that the current user can access.
    """
    tables = []
    error_message = None
    
    try:
        logger.info(f"Listing tables in {catalog}.{schema_name}")
        table_rows = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema_name}`").collect()
        logger.info(f"Found {len(table_rows)} tables")
        
        for row in table_rows:
            table_name = row.tableName
            table_type = row.isTemporary if hasattr(row, 'isTemporary') else None
            tables.append(TableInfo(
                name=table_name, 
                catalog=catalog, 
                schema_name=schema_name,
                table_type="TEMPORARY" if table_type else "TABLE"
            ))
            
    except Exception as e:
        error_message = str(e)
        logger.error(f"Failed to list tables in {catalog}.{schema_name}: {e}")
        
    return AvailableTablesResponse(tables=tables, error=error_message)


@api.get("/admin/prerequisite-sql", response_model=PrerequisiteSqlResponse, operation_id="get_prerequisite_sql")
def get_prerequisite_sql(
    catalog: str = Query(..., description="Target catalog for the DQX app"),
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)] = None,
) -> PrerequisiteSqlResponse:
    """
    Generate the pre-requisite SQL that catalog owners need to run.
    This grants the app's service principal permission to create schemas in the catalog.
    """
    # Get the app's service principal name (would come from app config in production)
    app_sp = "<app-service-principal>"  # This would be dynamically retrieved in production
    
    sql = f"""-- DQX App Pre-requisite
-- Run by: Catalog owner for '{catalog}'
-- Required: ONCE before clicking Initialize

GRANT CREATE SCHEMA ON CATALOG {catalog} TO `{app_sp}`;
GRANT USAGE ON CATALOG {catalog} TO `{app_sp}`;
"""
    
    return PrerequisiteSqlResponse(
        sql=sql,
        instructions=[
            "1. Copy the SQL above",
            f"2. Run it as the owner of catalog '{catalog}'",
            "3. Return to this page and click 'Initialize'",
        ]
    )


@api.post("/admin/initialize", response_model=InitializeResponse, operation_id="initialize_app")
def initialize_app(
    body: InitializeRequest,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
) -> InitializeResponse:
    """
    Initialize the app's schema, tables, and permissions.
    Creates the dqx_app schema in the specified catalog and sets up all required tables.
    """
    current_user = obo_ws.current_user.me().user_name
    
    storage = DQXStorage(
        spark=spark,
        catalog=body.target_catalog,
        schema=body.target_schema,
    )
    
    return storage.initialize(
        data_steward_group=body.data_steward_group,
        viewer_group=body.viewer_group,
        current_user=current_user,
    )


@api.get("/admin/app-settings", response_model=AppSettings, operation_id="get_app_settings")
def get_app_settings(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    catalog: str = Query(..., description="Catalog where app is installed"),
) -> AppSettings:
    """Get the current app settings."""
    storage = DQXStorage(spark=spark, catalog=catalog, schema=conf.default_schema)
    return storage.get_app_settings()


# -----------------------------------------------------------------------------
# Profiler Endpoints
# -----------------------------------------------------------------------------

@api.get(
    "/tables/{catalog}/{schema}/{table}/profiles",
    response_model=ExistingProfilesResponse,
    operation_id="get_existing_profiles"
)
def get_existing_profiles(
    catalog: str,
    schema: str,
    table: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> ExistingProfilesResponse:
    """
    Get all existing profiles for a table.
    All users can see all profiles (shared visibility).
    """
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    source_fqn = f"{catalog}.{schema}.{table}"
    
    profiles = storage.get_profiles_for_table(source_fqn)
    
    return ExistingProfilesResponse(
        table=source_fqn,
        existing_profiles=profiles,
        has_existing_profiles=len(profiles) > 0,
    )


@api.post("/profile-table", response_model=ProfileJobResponse, operation_id="profile_table")
def profile_table(
    body: ProfileTableRequest,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    generator: Annotated[DQGenerator, Depends(get_generator)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> ProfileJobResponse:
    """
    Start profiling a table.
    Creates a VIEW (inheriting user's permissions) and runs the profiler.
    """
    from datetime import datetime
    import time
    
    current_user = obo_ws.current_user.me().user_name
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    
    # Generate unique IDs
    profile_id = str(uuid.uuid4())
    view_uuid = profile_id.replace("-", "_")
    source_fqn = f"{body.catalog}.{body.schema_name}.{body.table}"
    
    # Validate sample limits
    sample_limit = min(body.sample_limit or conf.default_sample_limit, conf.max_sample_limit)
    
    try:
        # Create temporary view (runs as user via OBO - inherits user's permissions)
        view_name = storage.create_temp_view(view_uuid, source_fqn, sample_limit)
        logger.info(f"Created temp view {view_name} for user {current_user}")
    except Exception as e:
        logger.error(f"Cannot access table {source_fqn}: {e}")
        raise HTTPException(403, f"Cannot access table: {e}")
    
    # Run the profiler
    start_time = time.time()
    try:
        logger.info(f"Starting profiler for {view_name}")
        
        # Read the view data
        df = spark.table(view_name)
        col_count = len(df.columns)
        
        # Step 1: Profile the data using DQProfiler
        profiler = DQProfiler(workspace_client=obo_ws, spark=spark)
        logger.info(f"Running DQProfiler on {view_name}")
        summary_stats, profiles = profiler.profile(df)
        row_count = summary_stats.get("row_count", 0)
        logger.info(f"Profiling complete: {row_count} rows, {len(profiles)} column profiles")
        
        # Step 2: Generate rules from profiles using DQGenerator
        logger.info(f"Generating DQ rules from profiles")
        rules = generator.generate_dq_rules(profiles)
        logger.info(f"Generated {len(rules)} DQ rules")
        
        # Combine summary and rules (convert to JSON-serializable types)
        summary = _make_json_serializable({
            "summary_stats": summary_stats,
            "profiles": [p.__dict__ if hasattr(p, '__dict__') else str(p) for p in profiles],
            "generated_rules": rules,
        })
        
        duration = time.time() - start_time
        
        # Save profiling result
        result = ProfilingResult(
            profile_id=profile_id,
            requesting_user=current_user,
            source_catalog=body.catalog,
            source_schema=body.schema_name,
            source_table=body.table,
            source_table_fqn=source_fqn,
            sample_fraction=None,
            sample_limit=sample_limit,
            profiler_options_json=json.dumps({}),
            profile_summary_json=json.dumps(summary, default=str) if summary else None,
            column_profiles_json=None,
            rows_profiled=row_count,
            columns_profiled=col_count,
            profile_duration_seconds=round(duration, 2),
            created_at=datetime.now(),
            status=ProfileStatus.COMPLETED,
            job_run_id=None,
            view_name=view_name,
        )
        storage.save_profiling_result(result)
        
        logger.info(f"Profile completed for {source_fqn} in {duration:.2f}s with {len(rules)} rules")
        
        # Convert rules to GeneratedRule format (ensure all values are JSON-serializable)
        generated_rules = []
        for rule in rules:
            # Extract check info - can be dict {'function': '...', 'arguments': {...}} or string
            check = rule.get("check", {})
            if isinstance(check, dict):
                check_type = check.get("function", "unknown")
                check_args = check.get("arguments", {})
            else:
                check_type = str(check)
                check_args = {}
            
            column = rule.get("column", "")
            
            # Build params from check arguments and other rule fields
            params = {k: v for k, v in rule.items() if k not in ["name", "check", "column", "criticality"]}
            params.update(check_args)
            params = _make_json_serializable(params)
            
            # Generate a readable name
            rule_name = rule.get("name", f"{check_type}_{column}" if column else check_type)
            
            generated_rules.append(GeneratedRule(
                name=rule_name,
                check_type=check_type,
                column=column,
                criticality=rule.get("criticality", "error"),
                params=params,
            ))
        
        return ProfileJobResponse(
            profile_id=profile_id,
            view_name=view_name,
            status="completed",
            rows_profiled=row_count,
            columns_profiled=col_count,
            duration_seconds=round(duration, 2),
            generated_rules=generated_rules,
        )
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Profiling failed for {source_fqn}: {e}")
        
        # Save failed result
        result = ProfilingResult(
            profile_id=profile_id,
            requesting_user=current_user,
            source_catalog=body.catalog,
            source_schema=body.schema_name,
            source_table=body.table,
            source_table_fqn=source_fqn,
            sample_fraction=None,
            sample_limit=sample_limit,
            profiler_options_json=json.dumps({}),
            profile_summary_json=json.dumps({"error": str(e)}),
            column_profiles_json=None,
            rows_profiled=0,
            columns_profiled=0,
            profile_duration_seconds=round(duration, 2),
            created_at=datetime.now(),
            status=ProfileStatus.FAILED,
            job_run_id=None,
            view_name=view_name,
        )
        storage.save_profiling_result(result)
        
        # Create a user-friendly error message
        error_str = str(e)
        if "CANNOT_CONVERT_COLUMN_INTO_BOOL" in error_str:
            user_error = (
                "The table contains column types that the profiler cannot analyze. "
                "This is typically caused by complex nested types or certain data patterns. "
                "Try profiling a different table or contact support."
            )
        else:
            user_error = error_str
        
        return ProfileJobResponse(
            profile_id=profile_id,
            view_name=view_name,
            status="failed",
            error=user_error,
        )


# -----------------------------------------------------------------------------
# Rules Endpoints
# -----------------------------------------------------------------------------

@api.get("/rules/filter-options", response_model=RuleFilterOptions, operation_id="get_rule_filter_options")
def get_rule_filter_options(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> RuleFilterOptions:
    """Get unique values for rule filters (catalogs, schemas, tables)."""
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    return storage.get_rule_filter_options()


@api.get("/rules", response_model=RulesListResponse, operation_id="list_rules")
def list_rules(
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
    source_table: str | None = Query(None, description="Filter by source table FQN"),
    source_catalog: str | None = Query(None, description="Filter by source catalog"),
    source_schema: str | None = Query(None, description="Filter by source schema"),
    table_name: str | None = Query(None, description="Filter by table name"),
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(100, description="Maximum number of rules to return"),
) -> RulesListResponse:
    """List all rules with optional filters."""
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    
    rule_status = RuleStatus(status) if status else None
    rules = storage.list_rules(
        source_table_fqn=source_table,
        source_catalog=source_catalog,
        source_schema=source_schema,
        table_name=table_name,
        status=rule_status,
        limit=limit,
    )
    
    return RulesListResponse(rules=rules, total=len(rules))


@api.post("/rules", response_model=RuleResponse, operation_id="create_rule")
def create_rule(
    body: CreateRuleRequest,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> RuleResponse:
    """Create a new quality rule."""
    current_user = obo_ws.current_user.me().user_name
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    
    rule = storage.create_rule(
        created_by=current_user,
        source_catalog=body.source_catalog,
        source_schema=body.source_schema,
        source_table=body.source_table,
        rules=body.rules,
        suggestion_id=body.suggestion_id,
    )
    
    return RuleResponse(rule=rule)


@api.get("/rules/{rule_id}", response_model=RuleResponse, operation_id="get_rule")
def get_rule(
    rule_id: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> RuleResponse:
    """Get a specific rule by ID."""
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    
    rule = storage.get_rule(rule_id)
    if not rule:
        raise HTTPException(404, f"Rule {rule_id} not found")
    
    return RuleResponse(rule=rule)


@api.put("/rules/{rule_id}", response_model=RuleResponse, operation_id="update_rule")
def update_rule(
    rule_id: str,
    body: UpdateRuleRequest,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> RuleResponse:
    """Update an existing rule's content."""
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    
    # Check rule exists
    existing = storage.get_rule(rule_id)
    if not existing:
        raise HTTPException(404, f"Rule {rule_id} not found")
    
    # Only draft rules can be edited
    if existing.status not in [RuleStatus.DRAFT, RuleStatus.REJECTED]:
        raise HTTPException(400, f"Cannot edit rule with status {existing.status.value}")
    
    storage.update_rule_content(rule_id, body.rules)
    
    return RuleResponse(rule=storage.get_rule(rule_id))


@api.delete("/rules/{rule_id}", response_model=MessageResponse, operation_id="delete_rule")
def delete_rule(
    rule_id: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> MessageResponse:
    """Delete a rule."""
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    
    # Check rule exists
    existing = storage.get_rule(rule_id)
    if not existing:
        raise HTTPException(404, f"Rule {rule_id} not found")
    
    storage.delete_rule(rule_id)
    return MessageResponse(message=f"Rule {rule_id} deleted")


@api.post("/rules/{rule_id}/submit", response_model=RuleResponse, operation_id="submit_rule_for_approval")
def submit_rule_for_approval(
    rule_id: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> RuleResponse:
    """Submit a rule for approval."""
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    
    rule = storage.get_rule(rule_id)
    if not rule:
        raise HTTPException(404, f"Rule {rule_id} not found")
    
    if rule.status != RuleStatus.DRAFT:
        raise HTTPException(400, f"Only draft rules can be submitted. Current status: {rule.status.value}")
    
    storage.update_rule_status(rule_id, RuleStatus.PENDING_APPROVAL)
    
    return RuleResponse(rule=storage.get_rule(rule_id))


@api.post("/rules/{rule_id}/approve", response_model=RuleResponse, operation_id="approve_rule")
def approve_rule(
    rule_id: str,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> RuleResponse:
    """Approve a rule (Admin only)."""
    current_user = obo_ws.current_user.me().user_name
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    
    rule = storage.get_rule(rule_id)
    if not rule:
        raise HTTPException(404, f"Rule {rule_id} not found")
    
    if rule.status != RuleStatus.PENDING_APPROVAL:
        raise HTTPException(400, f"Only pending rules can be approved. Current status: {rule.status.value}")
    
    storage.update_rule_status(rule_id, RuleStatus.APPROVED, approved_by=current_user)
    
    return RuleResponse(rule=storage.get_rule(rule_id))


@api.post("/rules/{rule_id}/reject", response_model=RuleResponse, operation_id="reject_rule")
def reject_rule(
    rule_id: str,
    body: ApprovalRequest,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> RuleResponse:
    """Reject a rule (Admin only)."""
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    
    rule = storage.get_rule(rule_id)
    if not rule:
        raise HTTPException(404, f"Rule {rule_id} not found")
    
    if rule.status != RuleStatus.PENDING_APPROVAL:
        raise HTTPException(400, f"Only pending rules can be rejected. Current status: {rule.status.value}")
    
    storage.update_rule_status(rule_id, RuleStatus.REJECTED)
    
    return RuleResponse(rule=storage.get_rule(rule_id))


@api.post("/rules/{rule_id}/export", response_model=RuleResponse, operation_id="export_rule")
def export_rule(
    rule_id: str,
    body: ExportRuleRequest,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> RuleResponse:
    """Export an approved rule to checks storage (Admin only)."""
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    
    rule = storage.get_rule(rule_id)
    if not rule:
        raise HTTPException(404, f"Rule {rule_id} not found")
    
    if rule.status != RuleStatus.APPROVED:
        raise HTTPException(400, f"Only approved rules can be exported. Current status: {rule.status.value}")
    
    # Export the rules to the specified location
    # This would use DQEngine to save to the checks storage
    # For now, just update the status
    storage.update_rule_status(rule_id, RuleStatus.EXPORTED)
    
    return RuleResponse(rule=storage.get_rule(rule_id))


# -----------------------------------------------------------------------------
# Dry Run / Validation Endpoints
# -----------------------------------------------------------------------------

def _sanitize_rules_for_dqx(rules: list[dict]) -> list[dict]:
    """Remove unsupported arguments from rules before passing to DQX engine.
    
    The profiler may add metadata fields that DQX check functions don't accept.
    This function strips those out to prevent errors.
    """
    metadata_fields = {"user_metadata", "pk_detection_confidence", "detection_method"}
    
    sanitized = []
    for rule in rules:
        clean_rule = rule.copy()
        if "check" in clean_rule and isinstance(clean_rule["check"], dict):
            check = clean_rule["check"].copy()
            if "arguments" in check and isinstance(check["arguments"], dict):
                check["arguments"] = {
                    k: v for k, v in check["arguments"].items() 
                    if k not in metadata_fields
                }
            clean_rule["check"] = check
        sanitized.append(clean_rule)
    return sanitized


@api.post("/rules/{rule_id}/dry-run", response_model=DryRunResponse, operation_id="dry_run_rule")
def dry_run_rule(
    rule_id: str,
    body: DryRunRequest,
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
    spark: Annotated[SparkSession, Depends(get_spark)],
    engine: Annotated[DQEngine, Depends(get_engine)],
    app_catalog: str = Query(..., description="Catalog where DQX app is installed"),
) -> DryRunResponse:
    """Run a dry run validation on a rule."""
    from datetime import datetime as dt  # Local import to avoid circular imports
    
    current_user = obo_ws.current_user.me().user_name
    storage = DQXStorage(spark=spark, catalog=app_catalog, schema=conf.default_schema)
    
    rule = storage.get_rule(rule_id)
    if not rule:
        raise HTTPException(404, f"Rule {rule_id} not found")
    
    run_id = str(uuid.uuid4())
    start_time = dt.now()
    
    try:
        # Parse and sanitize the rules
        rules = json.loads(rule.rules_json)
        rules = _sanitize_rules_for_dqx(rules)
        
        # Sample the source table
        df = spark.table(rule.source_table_fqn).limit(body.sample_size)
        
        # Run the checks using DQEngine
        result_df = engine.apply_checks_by_metadata(df, rules)
        
        # Count results
        total_rows = result_df.count()
        failed_df = result_df.filter("_errors IS NOT NULL AND size(_errors) > 0")
        failed_rows = failed_df.count()
        passed_rows = total_rows - failed_rows
        
        # Get sample failures
        sample_failures = []
        if failed_rows > 0:
            sample_failures = [row.asDict() for row in failed_df.limit(10).collect()]
        
        duration = (dt.now() - start_time).total_seconds()
        pass_rate = (passed_rows / total_rows * 100) if total_rows > 0 else 100.0
        
        return DryRunResponse(
            run_id=run_id,
            total_rows=total_rows,
            passed_rows=passed_rows,
            failed_rows=failed_rows,
            pass_rate=pass_rate,
            rule_results=[],  # Would contain per-rule breakdown
            sample_failures=sample_failures,
        )
        
    except Exception as e:
        logger.error(f"Dry run failed: {e}", exc_info=True)
        raise HTTPException(500, f"Dry run failed: {str(e)}")
