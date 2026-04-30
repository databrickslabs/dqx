from fastapi import APIRouter

from .me import router as me_router
from .config import router as config_router
from .discovery import router as discovery_router
from .generate import router as generate_router
from .rules import router as rules_router
from .import_rules import router as import_rules_router
from .dryrun import router as dryrun_router
from .profiler import router as profiler_router
from .settings import router as settings_router
from .roles import router as roles_router
from .comments import router as comments_router
from .quarantine import router as quarantine_router
from .metrics import router as metrics_router
from .schedules import router as schedules_router

v1_router = APIRouter()
v1_router.include_router(me_router, tags=["meta"])
v1_router.include_router(config_router, prefix="/config", tags=["config"])
v1_router.include_router(schedules_router, prefix="/schedules", tags=["schedules"])
v1_router.include_router(roles_router, prefix="/roles", tags=["roles"])
v1_router.include_router(discovery_router, prefix="/discovery", tags=["discovery"])
v1_router.include_router(generate_router, prefix="/ai", tags=["ai"])
v1_router.include_router(rules_router, prefix="/rules", tags=["rules"])
v1_router.include_router(import_rules_router, prefix="/rules", tags=["rules"])
v1_router.include_router(dryrun_router, prefix="/dryrun", tags=["dryrun"])
v1_router.include_router(profiler_router, prefix="/profiler", tags=["profiler"])
v1_router.include_router(settings_router, prefix="/settings", tags=["settings"])
v1_router.include_router(comments_router, prefix="/comments", tags=["comments"])
v1_router.include_router(quarantine_router, prefix="/quarantine", tags=["quarantine"])
v1_router.include_router(metrics_router, prefix="/metrics", tags=["metrics"])
