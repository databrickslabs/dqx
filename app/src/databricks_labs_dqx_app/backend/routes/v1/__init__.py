from fastapi import APIRouter

from .me import router as me_router
from .check_functions import router as check_functions_router
from .config import router as config_router
from .contract import router as contract_router
from .discovery import router as discovery_router
from .generate import router as generate_router
from .ai import router as ai_router
from .rules import router as rules_router
from .registry_rules import router as registry_rules_router
from .monitored_tables import router as monitored_tables_router
from .import_rules import router as import_rules_router
from .dryrun import router as dryrun_router
from .profiler import router as profiler_router
from .settings import router as settings_router
from .roles import router as roles_router
from .comments import router as comments_router
from .quarantine import router as quarantine_router
from .metrics import router as metrics_router
from .review_status import router as review_status_router
from .schedules import router as schedules_router
from .run_sets import router as run_sets_router
from .data_products import router as data_products_router
from .compute import router as compute_router
from .table_data import router as table_data_router
from .principals import router as principals_router
from .permissions import router as permissions_router

v1_router = APIRouter()
v1_router.include_router(me_router, tags=["meta"])
v1_router.include_router(config_router, prefix="/config", tags=["config"])
v1_router.include_router(schedules_router, prefix="/schedules", tags=["schedules"])
v1_router.include_router(roles_router, prefix="/roles", tags=["roles"])
v1_router.include_router(discovery_router, prefix="/discovery", tags=["discovery"])
v1_router.include_router(generate_router, prefix="/ai", tags=["ai"])
v1_router.include_router(ai_router, prefix="/ai", tags=["ai"])
v1_router.include_router(contract_router, prefix="/contract", tags=["contract"])
v1_router.include_router(rules_router, prefix="/rules", tags=["rules"])
v1_router.include_router(registry_rules_router, prefix="/registry-rules", tags=["registry-rules"])
v1_router.include_router(monitored_tables_router, prefix="/monitored-tables", tags=["monitored-tables"])
v1_router.include_router(import_rules_router, prefix="/rules", tags=["rules"])
v1_router.include_router(check_functions_router, prefix="/check-functions", tags=["check-functions"])
v1_router.include_router(dryrun_router, prefix="/dryrun", tags=["dryrun"])
v1_router.include_router(profiler_router, prefix="/profiler", tags=["profiler"])
v1_router.include_router(settings_router, prefix="/settings", tags=["settings"])
v1_router.include_router(comments_router, prefix="/comments", tags=["comments"])
v1_router.include_router(quarantine_router, prefix="/quarantine", tags=["quarantine"])
v1_router.include_router(metrics_router, prefix="/metrics", tags=["metrics"])
v1_router.include_router(review_status_router, prefix="/runs", tags=["review-status"])
v1_router.include_router(run_sets_router, prefix="/run-sets", tags=["run-sets"])
v1_router.include_router(data_products_router, prefix="/data-products", tags=["data-products"])
v1_router.include_router(compute_router, prefix="/compute", tags=["compute"])
v1_router.include_router(table_data_router, prefix="/table-data", tags=["table-data"])
v1_router.include_router(principals_router, prefix="/principals", tags=["principals"])
v1_router.include_router(permissions_router, prefix="/permissions", tags=["permissions"])
