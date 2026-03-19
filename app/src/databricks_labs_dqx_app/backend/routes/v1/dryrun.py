from typing import Annotated

from databricks.labs.dqx.engine import DQEngine
from fastapi import APIRouter, Depends, HTTPException

from databricks_labs_dqx_app.backend.dependencies import get_engine
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import DryRunIn, DryRunOut

router = APIRouter()


@router.post("", response_model=DryRunOut, operation_id="dryRun")
def dry_run(
    body: DryRunIn,
    engine: Annotated[DQEngine, Depends(get_engine)],
) -> DryRunOut:
    """Run checks against a sample of the table and return validation results."""
    try:
        # Validate checks first
        validation = DQEngine.validate_checks(body.checks)
        if validation.has_errors:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid checks: {validation.errors}",
            )

        # Read a sample from the table
        spark = engine._spark  # noqa: SLF001
        df = spark.read.table(body.table_fqn).limit(body.sample_size)

        # Apply checks and split
        valid_df, invalid_df = engine.apply_checks_by_metadata_and_split(df, body.checks)

        total_rows = df.count()
        valid_rows = valid_df.count()
        invalid_rows = invalid_df.count()

        # Collect error summary — count occurrences of each error message
        error_summary: list[dict] = []
        if invalid_rows > 0:
            # Get the _errors column and aggregate
            from pyspark.sql import functions as F

            errors_df = invalid_df.select(F.explode(F.col("_errors")).alias("error"))
            summary_rows = (
                errors_df.groupBy("error").count().orderBy(F.desc("count")).limit(20).collect()
            )
            error_summary = [{"error": str(row["error"]), "count": row["count"]} for row in summary_rows]

        # Collect sample invalid rows (limited)
        sample_invalid: list[dict] = []
        if invalid_rows > 0:
            sample_rows = invalid_df.limit(10).collect()
            sample_invalid = [row.asDict(recursive=True) for row in sample_rows]

        return DryRunOut(
            total_rows=total_rows,
            valid_rows=valid_rows,
            invalid_rows=invalid_rows,
            error_summary=error_summary,
            sample_invalid=sample_invalid,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dry run failed for {body.table_fqn}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Dry run failed: {e}")
