import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Loader2, MessageSquare, Send, Trash2 } from "lucide-react";
import { formatDateTime } from "@/lib/format-utils";
import { toast } from "sonner";
import { useQueryClient } from "@tanstack/react-query";
import {
  useListComments,
  useAddComment,
  useDeleteComment,
  getListCommentsQueryKey,
  type CommentOut,
} from "@/lib/api-custom";

interface CommentThreadProps {
  entityType: "run" | "rule";
  entityId: string;
}

export function CommentThread({ entityType, entityId }: CommentThreadProps) {
  const queryClient = useQueryClient();
  const [newComment, setNewComment] = useState("");
  const [isOpen, setIsOpen] = useState(false);

  const { data: commentsResp, isLoading } = useListComments(entityType, entityId, {
    query: { enabled: isOpen },
  });
  const comments: CommentOut[] = commentsResp?.data ?? [];

  const addMutation = useAddComment();
  const deleteMutation = useDeleteComment();

  const handleAdd = async () => {
    if (!newComment.trim()) return;
    try {
      await addMutation.mutateAsync({
        data: { entity_type: entityType, entity_id: entityId, comment: newComment.trim() },
      });
      setNewComment("");
      queryClient.invalidateQueries({ queryKey: getListCommentsQueryKey(entityType, entityId) });
    } catch {
      toast.error("Failed to add comment");
    }
  };

  const handleDelete = async (commentId: string) => {
    try {
      await deleteMutation.mutateAsync({ commentId });
      queryClient.invalidateQueries({ queryKey: getListCommentsQueryKey(entityType, entityId) });
    } catch {
      toast.error("Failed to delete comment");
    }
  };

  return (
    <div className="space-y-3">
      <button
        onClick={() => setIsOpen(!isOpen)}
        aria-expanded={isOpen}
        className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors"
      >
        <MessageSquare className="h-4 w-4" />
        <span>
          {isOpen ? "Hide" : "Show"} comments
          {comments.length > 0 && ` (${comments.length})`}
        </span>
      </button>

      {isOpen && (
        <div className="space-y-3 pl-6 border-l-2 border-muted">
          {isLoading && (
            <div className="flex items-center gap-2 text-muted-foreground text-xs py-2">
              <Loader2 className="h-3 w-3 animate-spin" />
              Loading comments...
            </div>
          )}

          {!isLoading && comments.length === 0 && (
            <p className="text-xs text-muted-foreground py-1">No comments yet.</p>
          )}

          {comments.map((c) => (
            <div key={c.comment_id} className="group space-y-1">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2 text-xs">
                  <span className="font-medium">{c.user_email}</span>
                  <span className="text-muted-foreground">
                    {formatDateTime(c.created_at)}
                  </span>
                </div>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 w-6 p-0 opacity-0 group-hover:opacity-100 transition-opacity"
                  onClick={() => handleDelete(c.comment_id)}
                  disabled={deleteMutation.isPending}
                >
                  <Trash2 className="h-3 w-3 text-muted-foreground" />
                </Button>
              </div>
              <p className="text-sm whitespace-pre-wrap">{c.comment}</p>
            </div>
          ))}

          <div className="flex items-start gap-2">
            <Textarea
              value={newComment}
              onChange={(e) => setNewComment(e.target.value)}
              placeholder="Add a comment..."
              className="text-sm min-h-[60px] flex-1"
              onKeyDown={(e) => {
                if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) handleAdd();
              }}
            />
            <Button
              size="sm"
              onClick={handleAdd}
              disabled={!newComment.trim() || addMutation.isPending}
              className="h-9 gap-1.5"
            >
              {addMutation.isPending ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Send className="h-3.5 w-3.5" />
              )}
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
