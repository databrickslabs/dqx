"""
Delta table storage operations for the DQX App.

This module handles all Delta table operations for the view-based profiler approach,
including creating schemas, tables, and managing data in the app's storage.

Note on SQL construction: This module uses parameterized queries where possible.
For Spark SQL, we use explicit escaping for string values since Spark SQL doesn't
support traditional parameterized queries like JDBC. All user inputs are sanitized
through the _escape_sql_string method.
"""

import json
import re
import uuid
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession

from .config import (
    DQ_APP_SETTINGS_TABLE,
    DQ_PROFILING_RESULTS_TABLE,
    DQ_PROFILING_SUGGESTIONS_TABLE,
    DQ_QUALITY_RULES_TABLE,
    DQ_VALIDATION_RUNS_TABLE,
)
from .logger import logger
from .models import (
    AppSettings,
    InitializeResponse,
    ProfileStatus,
    ProfilingResult,
    QualityRule,
    RuleStatus,
    ValidationRunResult,
)


class DQXStorage:
    """Manages Delta table storage for the DQX App."""

    def __init__(self, spark: SparkSession, catalog: str, schema: str = "dqx_app"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def _fqn(self, table: str) -> str:
        """Get fully qualified table name."""
        return f"{self.catalog}.{self.schema}.{table}"

    @staticmethod
    def _escape_sql_string(value: str | None) -> str:
        """Escape a string value for safe SQL insertion.
        
        Escapes single quotes and backslashes to prevent SQL injection.
        Returns 'NULL' for None values.
        """
        if value is None:
            return "NULL"
        # Escape backslashes first, then single quotes
        escaped = str(value).replace("\\", "\\\\").replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _validate_identifier(name: str) -> str:
        """Validate and return a SQL identifier (catalog, schema, table name).
        
        Raises ValueError if the identifier contains invalid characters.
        """
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValueError(f"Invalid SQL identifier: {name}")
        return name

    @staticmethod
    def _format_timestamp(dt: datetime | None) -> str:
        """Format a datetime for SQL insertion."""
        if dt is None:
            return "NULL"
        return f"TIMESTAMP'{dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'"

    # =========================================================================
    # Schema and Table Creation
    # =========================================================================

    def create_schema(self) -> bool:
        """Create the dqx_app schema if it doesn't exist."""
        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
            logger.info(f"Created schema {self.catalog}.{self.schema}")
            return True
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise

    def create_tables(self) -> list[str]:
        """Create all required Delta tables. Returns list of created tables."""
        created = []
        
        table_ddls = {
            DQ_APP_SETTINGS_TABLE: f"""
                CREATE TABLE IF NOT EXISTS {self._fqn(DQ_APP_SETTINGS_TABLE)} (
                    config_key STRING NOT NULL,
                    config_value STRING NOT NULL,
                    updated_by STRING NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
                USING DELTA
            """,
            DQ_PROFILING_RESULTS_TABLE: f"""
                CREATE TABLE IF NOT EXISTS {self._fqn(DQ_PROFILING_RESULTS_TABLE)} (
                    profile_id STRING NOT NULL,
                    requesting_user STRING NOT NULL,
                    source_catalog STRING NOT NULL,
                    source_schema STRING NOT NULL,
                    source_table STRING NOT NULL,
                    source_table_fqn STRING NOT NULL,
                    sample_fraction DOUBLE,
                    sample_limit INT,
                    profiler_options_json STRING,
                    profile_summary_json STRING,
                    column_profiles_json STRING,
                    rows_profiled BIGINT,
                    columns_profiled INT,
                    profile_duration_seconds DOUBLE,
                    created_at TIMESTAMP NOT NULL,
                    status STRING NOT NULL,
                    job_run_id BIGINT,
                    view_name STRING
                )
                USING DELTA
                PARTITIONED BY (source_catalog)
                TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            """,
            DQ_PROFILING_SUGGESTIONS_TABLE: f"""
                CREATE TABLE IF NOT EXISTS {self._fqn(DQ_PROFILING_SUGGESTIONS_TABLE)} (
                    suggestion_id STRING NOT NULL,
                    profile_id STRING NOT NULL,
                    requesting_user STRING NOT NULL,
                    source_table_fqn STRING NOT NULL,
                    rules_json STRING NOT NULL,
                    rules_count INT NOT NULL,
                    generation_method STRING,
                    prompt_used STRING,
                    created_at TIMESTAMP NOT NULL,
                    status STRING NOT NULL
                )
                USING DELTA
            """,
            DQ_QUALITY_RULES_TABLE: f"""
                CREATE TABLE IF NOT EXISTS {self._fqn(DQ_QUALITY_RULES_TABLE)} (
                    rule_id STRING NOT NULL,
                    created_by STRING NOT NULL,
                    approved_by STRING,
                    source_catalog STRING NOT NULL,
                    source_schema STRING NOT NULL,
                    source_table STRING NOT NULL,
                    source_table_fqn STRING NOT NULL,
                    rules_json STRING NOT NULL,
                    rules_count INT NOT NULL,
                    suggestion_id STRING,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP,
                    approved_at TIMESTAMP,
                    status STRING NOT NULL,
                    exported_to STRING,
                    exported_at TIMESTAMP
                )
                USING DELTA
                PARTITIONED BY (source_catalog)
                TBLPROPERTIES (
                    'delta.enableChangeDataFeed' = 'true',
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            """,
            DQ_VALIDATION_RUNS_TABLE: f"""
                CREATE TABLE IF NOT EXISTS {self._fqn(DQ_VALIDATION_RUNS_TABLE)} (
                    run_id STRING NOT NULL,
                    rule_id STRING NOT NULL,
                    requesting_user STRING NOT NULL,
                    source_table_fqn STRING NOT NULL,
                    total_rows BIGINT,
                    passed_rows BIGINT,
                    failed_rows BIGINT,
                    rule_results_json STRING,
                    sample_failures_json STRING,
                    sample_size INT,
                    executed_at TIMESTAMP NOT NULL,
                    duration_seconds DOUBLE
                )
                USING DELTA
            """,
        }

        for table_name, ddl in table_ddls.items():
            try:
                self.spark.sql(ddl)
                created.append(table_name)
                logger.info(f"Created table {self._fqn(table_name)}")
            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {e}")
                raise

        return created

    def apply_grants(self, data_steward_group: str, viewer_group: str | None) -> list[str]:
        """Apply grants to tables for data steward and viewer groups."""
        applied = []

        steward_grants = [
            f"GRANT USAGE ON SCHEMA {self.catalog}.{self.schema} TO `{data_steward_group}`",
            f"GRANT CREATE TABLE ON SCHEMA {self.catalog}.{self.schema} TO `{data_steward_group}`",
            f"GRANT SELECT, INSERT, UPDATE ON TABLE {self._fqn(DQ_QUALITY_RULES_TABLE)} TO `{data_steward_group}`",
            f"GRANT SELECT, INSERT ON TABLE {self._fqn(DQ_PROFILING_RESULTS_TABLE)} TO `{data_steward_group}`",
            f"GRANT SELECT, INSERT ON TABLE {self._fqn(DQ_PROFILING_SUGGESTIONS_TABLE)} TO `{data_steward_group}`",
            f"GRANT SELECT, INSERT ON TABLE {self._fqn(DQ_VALIDATION_RUNS_TABLE)} TO `{data_steward_group}`",
        ]

        for grant in steward_grants:
            try:
                self.spark.sql(grant)
                applied.append(grant.split(" TO ")[0])
                logger.info(f"Applied grant: {grant}")
            except Exception as e:
                logger.error(f"Failed to apply grant: {e}")
                raise

        if viewer_group:
            viewer_grants = [
                f"GRANT USAGE ON SCHEMA {self.catalog}.{self.schema} TO `{viewer_group}`",
                f"GRANT SELECT ON TABLE {self._fqn(DQ_QUALITY_RULES_TABLE)} TO `{viewer_group}`",
                f"GRANT SELECT ON TABLE {self._fqn(DQ_PROFILING_RESULTS_TABLE)} TO `{viewer_group}`",
                f"GRANT SELECT ON TABLE {self._fqn(DQ_VALIDATION_RUNS_TABLE)} TO `{viewer_group}`",
            ]

            for grant in viewer_grants:
                try:
                    self.spark.sql(grant)
                    applied.append(grant.split(" TO ")[0])
                    logger.info(f"Applied grant: {grant}")
                except Exception as e:
                    logger.error(f"Failed to apply grant: {e}")
                    raise

        return applied

    def initialize(
        self, data_steward_group: str, viewer_group: str | None, current_user: str
    ) -> InitializeResponse:
        """Initialize the app's schema, tables, and permissions."""
        errors = []
        tables_created = []
        grants_applied = []
        schema_created = False

        try:
            schema_created = self.create_schema()
        except Exception as e:
            errors.append(f"Failed to create schema: {e}")
            errors.append("Hint: Ensure catalog owner has run the pre-requisite grants")
            return InitializeResponse(
                success=False,
                schema_created=False,
                tables_created=[],
                grants_applied=[],
                errors=errors,
            )

        try:
            tables_created = self.create_tables()
        except Exception as e:
            errors.append(f"Failed to create tables: {e}")

        try:
            grants_applied = self.apply_grants(data_steward_group, viewer_group)
        except Exception as e:
            errors.append(f"Failed to apply grants: {e}")

        # Save app settings
        try:
            self.save_app_setting("target_catalog", self.catalog, current_user)
            self.save_app_setting("target_schema", self.schema, current_user)
            self.save_app_setting("data_steward_group", data_steward_group, current_user)
            if viewer_group:
                self.save_app_setting("viewer_group", viewer_group, current_user)
            self.save_app_setting("initialized", "true", current_user)
            self.save_app_setting("initialized_at", datetime.now().isoformat(), current_user)
            self.save_app_setting("initialized_by", current_user, current_user)
        except Exception as e:
            errors.append(f"Failed to save app settings: {e}")

        return InitializeResponse(
            success=len(errors) == 0,
            schema_created=schema_created,
            tables_created=tables_created,
            grants_applied=grants_applied,
            errors=errors,
        )

    # =========================================================================
    # App Settings Operations
    # =========================================================================

    def save_app_setting(self, key: str, value: str, updated_by: str) -> None:
        """Save a single app setting."""
        now = datetime.now()
        self.spark.sql(f"""
            MERGE INTO {self._fqn(DQ_APP_SETTINGS_TABLE)} AS target
            USING (SELECT {self._escape_sql_string(key)} as config_key, 
                          {self._escape_sql_string(value)} as config_value, 
                          {self._escape_sql_string(updated_by)} as updated_by, 
                          {self._format_timestamp(now)} as updated_at) AS source
            ON target.config_key = source.config_key
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    def get_app_setting(self, key: str) -> str | None:
        """Get a single app setting."""
        try:
            result = self.spark.sql(f"""
                SELECT config_value FROM {self._fqn(DQ_APP_SETTINGS_TABLE)}
                WHERE config_key = {self._escape_sql_string(key)}
            """).first()
            return result.config_value if result else None
        except Exception:
            return None

    def get_app_settings(self) -> AppSettings:
        """Get all app settings."""
        try:
            rows = self.spark.sql(f"""
                SELECT config_key, config_value FROM {self._fqn(DQ_APP_SETTINGS_TABLE)}
            """).collect()
            
            settings_dict = {row.config_key: row.config_value for row in rows}
            
            return AppSettings(
                target_catalog=settings_dict.get("target_catalog", self.catalog),
                target_schema=settings_dict.get("target_schema", self.schema),
                data_steward_group=settings_dict.get("data_steward_group"),
                viewer_group=settings_dict.get("viewer_group"),
                initialized=settings_dict.get("initialized", "false").lower() == "true",
                initialized_at=datetime.fromisoformat(settings_dict["initialized_at"]) 
                    if settings_dict.get("initialized_at") else None,
                initialized_by=settings_dict.get("initialized_by"),
            )
        except Exception as e:
            logger.warning(f"Failed to get app settings: {e}")
            return AppSettings(
                target_catalog=self.catalog,
                target_schema=self.schema,
                initialized=False,
            )

    # =========================================================================
    # Profiling Operations
    # =========================================================================

    def create_temp_view(self, view_uuid: str, source_fqn: str, limit: int | None = None) -> str:
        """Create a temporary view for profiling. Returns the view name."""
        view_name = f"{self.catalog}.{self.schema}.temp_view_{view_uuid}"
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM {source_fqn}
            {limit_clause}
        """)
        
        logger.info(f"Created temp view {view_name}")
        return view_name

    def drop_temp_view(self, view_name: str) -> None:
        """Drop a temporary view."""
        try:
            self.spark.sql(f"DROP VIEW IF EXISTS {view_name}")
            logger.info(f"Dropped temp view {view_name}")
        except Exception as e:
            logger.warning(f"Failed to drop temp view {view_name}: {e}")

    def save_profiling_result(self, result: ProfilingResult) -> None:
        """Save a profiling result to the table."""
        self.spark.sql(f"""
            INSERT INTO {self._fqn(DQ_PROFILING_RESULTS_TABLE)}
            VALUES (
                {self._escape_sql_string(result.profile_id)},
                {self._escape_sql_string(result.requesting_user)},
                {self._escape_sql_string(result.source_catalog)},
                {self._escape_sql_string(result.source_schema)},
                {self._escape_sql_string(result.source_table)},
                {self._escape_sql_string(result.source_table_fqn)},
                {result.sample_fraction if result.sample_fraction is not None else 'NULL'},
                {result.sample_limit if result.sample_limit is not None else 'NULL'},
                {self._escape_sql_string(result.profiler_options_json)},
                {self._escape_sql_string(result.profile_summary_json)},
                {self._escape_sql_string(result.column_profiles_json)},
                {result.rows_profiled if result.rows_profiled is not None else 'NULL'},
                {result.columns_profiled if result.columns_profiled is not None else 'NULL'},
                {result.profile_duration_seconds if result.profile_duration_seconds is not None else 'NULL'},
                {self._format_timestamp(result.created_at)},
                {self._escape_sql_string(result.status.value)},
                {result.job_run_id if result.job_run_id is not None else 'NULL'},
                {self._escape_sql_string(result.view_name)}
            )
        """)

    def get_profiles_for_table(self, source_fqn: str) -> list[dict[str, Any]]:
        """Get all profiles for a specific table."""
        rows = self.spark.sql(f"""
            SELECT 
                profile_id,
                requesting_user,
                created_at,
                status,
                rows_profiled,
                profile_duration_seconds
            FROM {self._fqn(DQ_PROFILING_RESULTS_TABLE)}
            WHERE source_table_fqn = {self._escape_sql_string(source_fqn)}
            ORDER BY created_at DESC
        """).collect()

        return [
            {
                "profile_id": row.profile_id,
                "created_by": row.requesting_user,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "status": row.status,
                "rows_profiled": row.rows_profiled,
                "duration_seconds": row.profile_duration_seconds,
            }
            for row in rows
        ]

    # =========================================================================
    # Quality Rules Operations
    # =========================================================================

    def create_rule(
        self,
        created_by: str,
        source_catalog: str,
        source_schema: str,
        source_table: str,
        rules: list[dict[str, Any]],
        suggestion_id: str | None = None,
    ) -> QualityRule:
        """Create a new quality rule."""
        rule_id = str(uuid.uuid4())
        now = datetime.now()
        source_fqn = f"{source_catalog}.{source_schema}.{source_table}"
        rules_json = json.dumps(rules)

        self.spark.sql(f"""
            INSERT INTO {self._fqn(DQ_QUALITY_RULES_TABLE)}
            VALUES (
                {self._escape_sql_string(rule_id)},
                {self._escape_sql_string(created_by)},
                NULL,
                {self._escape_sql_string(source_catalog)},
                {self._escape_sql_string(source_schema)},
                {self._escape_sql_string(source_table)},
                {self._escape_sql_string(source_fqn)},
                {self._escape_sql_string(rules_json)},
                {len(rules)},
                {self._escape_sql_string(suggestion_id)},
                {self._format_timestamp(now)},
                NULL,
                NULL,
                {self._escape_sql_string(RuleStatus.DRAFT.value)},
                NULL,
                NULL
            )
        """)

        return QualityRule(
            rule_id=rule_id,
            created_by=created_by,
            approved_by=None,
            source_catalog=source_catalog,
            source_schema=source_schema,
            source_table=source_table,
            source_table_fqn=source_fqn,
            rules_json=json.dumps(rules),
            rules_count=len(rules),
            suggestion_id=suggestion_id,
            created_at=now,
            updated_at=None,
            approved_at=None,
            status=RuleStatus.DRAFT,
            exported_to=None,
            exported_at=None,
        )

    def get_rule(self, rule_id: str) -> QualityRule | None:
        """Get a single rule by ID."""
        result = self.spark.sql(f"""
            SELECT * FROM {self._fqn(DQ_QUALITY_RULES_TABLE)}
            WHERE rule_id = {self._escape_sql_string(rule_id)}
        """).first()

        if not result:
            return None

        return QualityRule(
            rule_id=result.rule_id,
            created_by=result.created_by,
            approved_by=result.approved_by,
            source_catalog=result.source_catalog,
            source_schema=result.source_schema,
            source_table=result.source_table,
            source_table_fqn=result.source_table_fqn,
            rules_json=result.rules_json,
            rules_count=result.rules_count,
            suggestion_id=result.suggestion_id,
            created_at=result.created_at,
            updated_at=result.updated_at,
            approved_at=result.approved_at,
            status=RuleStatus(result.status),
            exported_to=result.exported_to,
            exported_at=result.exported_at,
        )

    def get_rule_filter_options(self) -> "RuleFilterOptions":
        """Get unique values for rule filters."""
        from .models import RuleFilterOptions
        
        try:
            # Get distinct catalogs
            catalogs_rows = self.spark.sql(f"""
                SELECT DISTINCT source_catalog FROM {self._fqn(DQ_QUALITY_RULES_TABLE)}
                WHERE source_catalog IS NOT NULL
                ORDER BY source_catalog
            """).collect()
            catalogs = [row.source_catalog for row in catalogs_rows]
            
            # Get distinct schemas
            schemas_rows = self.spark.sql(f"""
                SELECT DISTINCT source_schema FROM {self._fqn(DQ_QUALITY_RULES_TABLE)}
                WHERE source_schema IS NOT NULL
                ORDER BY source_schema
            """).collect()
            schemas = [row.source_schema for row in schemas_rows]
            
            # Get distinct tables
            tables_rows = self.spark.sql(f"""
                SELECT DISTINCT source_table FROM {self._fqn(DQ_QUALITY_RULES_TABLE)}
                WHERE source_table IS NOT NULL
                ORDER BY source_table
            """).collect()
            tables = [row.source_table for row in tables_rows]
            
            return RuleFilterOptions(catalogs=catalogs, schemas=schemas, tables=tables)
        except Exception as e:
            logger.warning(f"Failed to get rule filter options: {e}")
            return RuleFilterOptions(catalogs=[], schemas=[], tables=[])

    def list_rules(
        self,
        source_table_fqn: str | None = None,
        source_catalog: str | None = None,
        source_schema: str | None = None,
        table_name: str | None = None,
        status: RuleStatus | None = None,
        limit: int = 100,
    ) -> list[QualityRule]:
        """List rules with optional filters."""
        where_clauses = []
        if source_table_fqn:
            where_clauses.append(f"source_table_fqn = {self._escape_sql_string(source_table_fqn)}")
        if source_catalog:
            where_clauses.append(f"source_catalog = {self._escape_sql_string(source_catalog)}")
        if source_schema:
            where_clauses.append(f"source_schema = {self._escape_sql_string(source_schema)}")
        if table_name:
            where_clauses.append(f"source_table = {self._escape_sql_string(table_name)}")
        if status:
            where_clauses.append(f"status = {self._escape_sql_string(status.value)}")

        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        # Validate limit is a positive integer to prevent injection
        safe_limit = max(1, min(int(limit), 1000))

        rows = self.spark.sql(f"""
            SELECT * FROM {self._fqn(DQ_QUALITY_RULES_TABLE)}
            {where_clause}
            ORDER BY created_at DESC
            LIMIT {safe_limit}
        """).collect()

        return [
            QualityRule(
                rule_id=row.rule_id,
                created_by=row.created_by,
                approved_by=row.approved_by,
                source_catalog=row.source_catalog,
                source_schema=row.source_schema,
                source_table=row.source_table,
                source_table_fqn=row.source_table_fqn,
                rules_json=row.rules_json,
                rules_count=row.rules_count,
                suggestion_id=row.suggestion_id,
                created_at=row.created_at,
                updated_at=row.updated_at,
                approved_at=row.approved_at,
                status=RuleStatus(row.status),
                exported_to=row.exported_to,
                exported_at=row.exported_at,
            )
            for row in rows
        ]

    def update_rule_status(
        self,
        rule_id: str,
        status: RuleStatus,
        approved_by: str | None = None,
    ) -> None:
        """Update a rule's status."""
        now = datetime.now()
        approved_at_clause = f", approved_at = {self._format_timestamp(now)}" if status == RuleStatus.APPROVED else ""
        approved_by_clause = f", approved_by = {self._escape_sql_string(approved_by)}" if approved_by else ""

        self.spark.sql(f"""
            UPDATE {self._fqn(DQ_QUALITY_RULES_TABLE)}
            SET status = {self._escape_sql_string(status.value)}, 
                updated_at = {self._format_timestamp(now)}{approved_by_clause}{approved_at_clause}
            WHERE rule_id = {self._escape_sql_string(rule_id)}
        """)

    def update_rule_content(self, rule_id: str, rules: list[dict[str, Any]]) -> None:
        """Update a rule's content."""
        now = datetime.now()
        rules_json = json.dumps(rules)

        self.spark.sql(f"""
            UPDATE {self._fqn(DQ_QUALITY_RULES_TABLE)}
            SET rules_json = {self._escape_sql_string(rules_json)}, 
                rules_count = {len(rules)}, 
                updated_at = {self._format_timestamp(now)}
            WHERE rule_id = {self._escape_sql_string(rule_id)}
        """)

    def delete_rule(self, rule_id: str) -> None:
        """Delete a rule."""
        self.spark.sql(f"""
            DELETE FROM {self._fqn(DQ_QUALITY_RULES_TABLE)}
            WHERE rule_id = {self._escape_sql_string(rule_id)}
        """)

    # =========================================================================
    # Validation Run Operations
    # =========================================================================

    def save_validation_run(self, run: ValidationRunResult) -> None:
        """Save a validation run result."""
        self.spark.sql(f"""
            INSERT INTO {self._fqn(DQ_VALIDATION_RUNS_TABLE)}
            VALUES (
                {self._escape_sql_string(run.run_id)},
                {self._escape_sql_string(run.rule_id)},
                {self._escape_sql_string(run.requesting_user)},
                {self._escape_sql_string(run.source_table_fqn)},
                {run.total_rows if run.total_rows is not None else 'NULL'},
                {run.passed_rows if run.passed_rows is not None else 'NULL'},
                {run.failed_rows if run.failed_rows is not None else 'NULL'},
                {self._escape_sql_string(run.rule_results_json)},
                {self._escape_sql_string(run.sample_failures_json)},
                {run.sample_size if run.sample_size is not None else 'NULL'},
                {self._format_timestamp(run.executed_at)},
                {run.duration_seconds if run.duration_seconds is not None else 'NULL'}
            )
        """)

    def get_validation_runs(self, rule_id: str) -> list[ValidationRunResult]:
        """Get all validation runs for a rule."""
        rows = self.spark.sql(f"""
            SELECT * FROM {self._fqn(DQ_VALIDATION_RUNS_TABLE)}
            WHERE rule_id = {self._escape_sql_string(rule_id)}
            ORDER BY executed_at DESC
        """).collect()

        return [
            ValidationRunResult(
                run_id=row.run_id,
                rule_id=row.rule_id,
                requesting_user=row.requesting_user,
                source_table_fqn=row.source_table_fqn,
                total_rows=row.total_rows,
                passed_rows=row.passed_rows,
                failed_rows=row.failed_rows,
                rule_results_json=row.rule_results_json,
                sample_failures_json=row.sample_failures_json,
                sample_size=row.sample_size,
                executed_at=row.executed_at,
                duration_seconds=row.duration_seconds,
            )
            for row in rows
        ]
