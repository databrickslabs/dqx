"""
Simple demo showing optional LLM-based Primary Key Detection in DQX.

This demo shows the key usage patterns:
1. Regular profiling (default - no LLM)
2. LLM-based PK detection (when explicitly requested)
"""

import logging
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import ProfilerConfig
from databricks.labs.dqx.profiler.profiler import DQProfiler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demo_regular_profiling():
    """Demo: Regular profiling without LLM (default behavior)."""
    logger.info("🔧 REGULAR PROFILING (NO LLM DEPENDENCIES REQUIRED)")

    # Default configuration - no LLM features
    config = ProfilerConfig()
    logger.info(f"LLM PK Detection: {config.enable_llm_pk_detection}")  # False by default

    # This works without any LLM dependencies!
    logger.info("✅ Regular profiling works out of the box!")


def demo_llm_profiling():
    """Demo: LLM-based PK detection (when explicitly requested)."""
    logger.info("🤖 LLM-BASED PK DETECTION (WHEN EXPLICITLY REQUESTED)")

    # Method 1: Configuration
    config = ProfilerConfig(enable_llm_pk_detection=True)
    logger.info(f"LLM PK Detection: {config.enable_llm_pk_detection}")

    # Method 2: Options
    ws = WorkspaceClient()
    profiler = DQProfiler(ws)

    try:
        # This requires: pip install dspy-ai databricks_langchain
        summary_stats, dq_rules = profiler.profile_table(
            "catalog.schema.table", options={"llm": True}  # Simple LLM enablement
        )
        logger.info("✅ LLM-based profiling enabled!")

    except ImportError:
        logger.warning("⚠️  LLM dependencies not available")
        logger.info("💡 Install with: pip install dspy-ai databricks_langchain")

    # Method 3: Convenience method
    try:
        summary_stats, dq_rules = profiler.profile_table_with_llm_pk_detection("catalog.schema.table")
        logger.info("✅ Convenience method works!")

    except ImportError:
        logger.warning("⚠️  LLM dependencies not available for convenience method")


def main():
    """Main demo function."""
    logger.info("🚀 DQX OPTIONAL LLM PRIMARY KEY DETECTION DEMO")
    logger.info("=" * 50)

    # Demo 1: Regular profiling (always works)
    demo_regular_profiling()

    print()

    # Demo 2: LLM profiling (requires dependencies)
    demo_llm_profiling()

    logger.info("\n🎯 KEY TAKEAWAYS:")
    logger.info("✅ Regular DQX works without LLM dependencies")
    logger.info("✅ LLM features only activate when explicitly requested")
    logger.info("✅ Multiple ways to enable: config, options, convenience methods")
    logger.info("✅ Graceful fallback when dependencies unavailable")


if __name__ == "__main__":
    main()
