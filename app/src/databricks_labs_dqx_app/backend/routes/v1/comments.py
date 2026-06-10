from __future__ import annotations

from typing import Annotated

from databricks.sdk import WorkspaceClient
from fastapi import APIRouter, Depends, HTTPException, Query

from databricks_labs_dqx_app.backend.common.authorization import UserRole
from databricks_labs_dqx_app.backend.dependencies import get_comments_service, get_obo_ws, require_role
from databricks_labs_dqx_app.backend.logger import logger
from databricks_labs_dqx_app.backend.models import AddCommentIn, CommentOut
from databricks_labs_dqx_app.backend.services.comments_service import CommentsService

router = APIRouter()

_ALL_ROLES = [UserRole.ADMIN, UserRole.RULE_APPROVER, UserRole.RULE_AUTHOR, UserRole.VIEWER]


def _comment_to_out(c) -> CommentOut:
    return CommentOut(
        comment_id=c.comment_id,
        entity_type=c.entity_type,
        entity_id=c.entity_id,
        user_email=c.user_email,
        comment=c.comment,
        created_at=c.created_at,
    )


@router.post(
    "",
    response_model=CommentOut,
    operation_id="addComment",
    dependencies=[require_role(*_ALL_ROLES)],
)
def add_comment(
    body: AddCommentIn,
    svc: Annotated[CommentsService, Depends(get_comments_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> CommentOut:
    """Add a comment to a run or rule."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        comment = svc.add_comment(body.entity_type, body.entity_id, user_email, body.comment)
        return _comment_to_out(comment)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to add comment: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add comment: {e}")


@router.get(
    "",
    response_model=list[CommentOut],
    operation_id="listComments",
    dependencies=[require_role(*_ALL_ROLES)],
)
def list_comments(
    svc: Annotated[CommentsService, Depends(get_comments_service)],
    entity_type: Annotated[str, Query(description="Entity type: 'run' or 'rule'")],
    entity_id: Annotated[str, Query(description="Entity identifier: run_id or table_fqn")],
) -> list[CommentOut]:
    """List comments for a specific run or rule."""
    try:
        comments = svc.list_comments(entity_type, entity_id)
        return [_comment_to_out(c) for c in comments]
    except Exception as e:
        logger.error("Failed to list comments: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list comments: {e}")


@router.delete(
    "/{comment_id}",
    operation_id="deleteComment",
    dependencies=[require_role(*_ALL_ROLES)],
)
def delete_comment(
    comment_id: str,
    svc: Annotated[CommentsService, Depends(get_comments_service)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> dict[str, str]:
    """Delete a comment (only the author can delete their own comments)."""
    try:
        user = obo_ws.current_user.me()
        user_email = user.user_name or "unknown"
        svc.delete_comment(comment_id, user_email)
        return {"status": "deleted", "comment_id": comment_id}
    except Exception as e:
        logger.error("Failed to delete comment %s: %s", comment_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete comment: {e}")
