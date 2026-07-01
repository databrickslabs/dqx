import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Loader2, MessageSquare, Send, Trash2 } from "lucide-react";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
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
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [newComment, setNewComment] = useState("");
  const [isOpen, setIsOpen] = useState(false);
  // Deletion is destructive — gate it behind a confirm dialog so an
  // accidental click on the trash icon (which appears on hover) doesn't
  // silently nuke a comment.
  const [pendingDeleteId, setPendingDeleteId] = useState<string | null>(null);

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
      toast.error(t("commentThread.failedAdd"));
    }
  };

  const handleDelete = async (commentId: string) => {
    try {
      await deleteMutation.mutateAsync({ commentId });
      queryClient.invalidateQueries({ queryKey: getListCommentsQueryKey(entityType, entityId) });
    } catch {
      toast.error(t("commentThread.failedDelete"));
    } finally {
      setPendingDeleteId(null);
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
          {isOpen ? t("commentThread.hideComments") : t("commentThread.showComments")} {t("commentThread.commentsSuffix")}
          {comments.length > 0 && ` (${comments.length})`}
        </span>
      </button>

      {isOpen && (
        <div className="space-y-3 pl-6 border-l-2 border-muted">
          {isLoading && (
            <div className="flex items-center gap-2 text-muted-foreground text-xs py-2">
              <Loader2 className="h-3 w-3 animate-spin" />
              {t("commentThread.loading")}
            </div>
          )}

          {!isLoading && comments.length === 0 && (
            <p className="text-xs text-muted-foreground py-1">{t("commentThread.empty")}</p>
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
                  onClick={() => setPendingDeleteId(c.comment_id)}
                  disabled={deleteMutation.isPending}
                  aria-label={t("commentThread.deleteAria")}
                >
                  <Trash2 className="h-3 w-3 text-muted-foreground" />
                </Button>
              </div>
              <p className="text-sm whitespace-pre-wrap">{c.comment}</p>
            </div>
          ))}

          <div className="flex items-start gap-2">
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
            <Textarea
              value={newComment}
              onChange={(e) => setNewComment(e.target.value)}
              placeholder={t("commentThread.placeholder")}
              className="text-sm min-h-[60px] flex-1"
              onKeyDown={(e) => {
                if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) handleAdd();
              }}
            />
          </div>
        </div>
      )}

      <AlertDialog
        open={pendingDeleteId !== null}
        onOpenChange={(open) => {
          if (!open) setPendingDeleteId(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("commentThread.deleteConfirmTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {t("commentThread.deleteConfirmBody")}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (pendingDeleteId) void handleDelete(pendingDeleteId);
              }}
              disabled={deleteMutation.isPending}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              {deleteMutation.isPending ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                t("common.delete")
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
