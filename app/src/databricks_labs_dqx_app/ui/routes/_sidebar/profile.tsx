import { createFileRoute, Link } from "@tanstack/react-router";
import { Suspense } from "react";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useCurrentUserSuspense } from "@/lib/api";
import selector from "@/lib/selector";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Separator } from "@/components/ui/separator";
import {
  User,
  Mail,
  Shield,
  Users,
  AlertCircle,
  CheckCircle,
  XCircle,
} from "lucide-react";

export const Route = createFileRoute("/_sidebar/profile")({
  component: () => <Profile />,
});

function ProfileContent() {
  const { data: user } = useCurrentUserSuspense(selector());

  const getInitials = () => {
    if (user.name?.given_name && user.name?.family_name) {
      return `${user.name.given_name[0]}${user.name.family_name[0]}`.toUpperCase();
    }
    if (user.display_name) {
      const parts = user.display_name.split(" ");
      if (parts.length >= 2) {
        return `${parts[0][0]}${parts[parts.length - 1][0]}`.toUpperCase();
      }
      return user.display_name.substring(0, 2).toUpperCase();
    }
    if (user.user_name) {
      return user.user_name.substring(0, 2).toUpperCase();
    }
    return "U";
  };

  return (
    <div className="space-y-4">
      <PageBreadcrumb page="Profile" />
      {/* Header Card */}
      <Card className="border-primary/20">
        <CardContent className="pt-4">
          <div className="flex flex-col md:flex-row items-center gap-4">
            <Avatar className="h-20 w-20">
              <AvatarFallback className="text-xl font-bold bg-primary/10">
                {getInitials()}
              </AvatarFallback>
            </Avatar>
            <div className="flex-1 text-center md:text-left space-y-1.5">
              <div className="flex items-center gap-2 justify-center md:justify-start">
                <h1 className="text-2xl font-bold">
                  {user.display_name || user.user_name || "User"}
                </h1>
                {user.active !== null && (
                  <Badge
                    variant={user.active ? "default" : "secondary"}
                    className="flex items-center gap-1"
                  >
                    {user.active ? (
                      <>
                        <CheckCircle className="h-3 w-3" />
                        Active
                      </>
                    ) : (
                      <>
                        <XCircle className="h-3 w-3" />
                        Inactive
                      </>
                    )}
                  </Badge>
                )}
              </div>
              {user.user_name && (
                <p className="text-muted-foreground">@{user.user_name}</p>
              )}
              {user.id && (
                <p className="text-xs text-muted-foreground font-mono">
                  ID: {user.id}
                </p>
              )}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* User Details Grid */}
      <div className="grid gap-4 md:grid-cols-2">
        {/* Personal Information */}
        <Card className="border-primary/20">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <User className="h-5 w-5" />
              Personal Information
            </CardTitle>
            <CardDescription>Basic user details</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {user.name && (
              <>
                {user.name.given_name && (
                  <div>
                    <p className="text-sm font-medium text-muted-foreground">
                      First Name
                    </p>
                    <p className="text-lg">{user.name.given_name}</p>
                  </div>
                )}
                {user.name.family_name && (
                  <>
                    <Separator />
                    <div>
                      <p className="text-sm font-medium text-muted-foreground">
                        Last Name
                      </p>
                      <p className="text-lg">{user.name.family_name}</p>
                    </div>
                  </>
                )}
              </>
            )}
            {user.external_id && (
              <>
                <Separator />
                <div>
                  <p className="text-sm font-medium text-muted-foreground">
                    External ID
                  </p>
                  <p className="text-sm font-mono">{user.external_id}</p>
                </div>
              </>
            )}
          </CardContent>
        </Card>

        {/* Contact Information */}
        <Card className="border-primary/20">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Mail className="h-5 w-5" />
              Contact Information
            </CardTitle>
            <CardDescription>Email addresses</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {user.emails && user.emails.length > 0 ? (
              user.emails.map((email, index) => (
                <div key={index}>
                  {index > 0 && <Separator className="mb-3" />}
                  <div className="flex items-center gap-2">
                    <p className="text-sm break-all">{email.value}</p>
                    {email.primary && (
                      <Badge variant="secondary" className="text-xs">
                        Primary
                      </Badge>
                    )}
                  </div>
                  {email.type && (
                    <p className="text-xs text-muted-foreground mt-1">
                      Type: {email.type}
                    </p>
                  )}
                </div>
              ))
            ) : (
              <p className="text-sm text-muted-foreground">
                No email addresses available
              </p>
            )}
          </CardContent>
        </Card>

        {/* Roles */}
        {user.roles && user.roles.length > 0 && (
          <Card className="border-primary/20">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Shield className="h-5 w-5" />
                Roles
              </CardTitle>
              <CardDescription>
                User roles and permissions ({user.roles.length})
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-2">
                {[...user.roles]
                  .sort((a, b) => (a.value || "").localeCompare(b.value || ""))
                  .map((role, index) => (
                    <Badge key={index} variant="outline">
                      {role.value}
                    </Badge>
                  ))}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Groups */}
        {user.groups && user.groups.length > 0 && (
          <Card className="border-primary/20">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Users className="h-5 w-5" />
                Groups
              </CardTitle>
              <CardDescription>
                User group memberships ({user.groups.length})
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-2">
                {[...user.groups]
                  .sort((a, b) => {
                    const aDisplay = a.display || a.value || "";
                    const bDisplay = b.display || b.value || "";
                    return aDisplay.localeCompare(bDisplay);
                  })
                  .map((group, index) => (
                    <Badge key={index} variant="outline">
                      {group.display || group.value}
                    </Badge>
                  ))}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Entitlements */}
        {user.entitlements && user.entitlements.length > 0 && (
          <Card className="border-primary/20">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Shield className="h-5 w-5" />
                Entitlements
              </CardTitle>
              <CardDescription>
                User entitlements ({user.entitlements.length})
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-2">
                {[...user.entitlements]
                  .sort((a, b) => (a.value || "").localeCompare(b.value || ""))
                  .map((entitlement, index) => (
                    <Badge key={index} variant="secondary">
                      {entitlement.value}
                    </Badge>
                  ))}
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}

function ProfileSkeleton() {
  return (
    <div className="space-y-4">
      {/* Header Card Skeleton */}
      <Card className="border-primary/20">
        <CardContent className="pt-4">
          <div className="flex flex-col md:flex-row items-center gap-4">
            <Skeleton className="h-20 w-20 rounded-full" />
            <div className="flex-1 space-y-1.5 text-center md:text-left">
              <Skeleton className="h-7 w-48 mx-auto md:mx-0" />
              <Skeleton className="h-4 w-32 mx-auto md:mx-0" />
              <Skeleton className="h-3 w-64 mx-auto md:mx-0" />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Cards Skeleton */}
      <div className="grid gap-4 md:grid-cols-2">
        {[1, 2, 3, 4].map((i) => (
          <Card key={i} className="border-primary/20">
            <CardHeader>
              <Skeleton className="h-6 w-48" />
              <Skeleton className="h-4 w-32" />
            </CardHeader>
            <CardContent className="space-y-2">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-3/4" />
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}

function Profile() {
  return (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary
          onReset={reset}
          fallbackRender={({ resetErrorBoundary }) => (
            <Card className="border-destructive/50">
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-destructive">
                  <AlertCircle className="h-5 w-5" />
                  Failed to Load Profile
                </CardTitle>
                <CardDescription>
                  There was an error loading your profile information. Make sure
                  the backend is running and you're authenticated.
                </CardDescription>
              </CardHeader>
              <CardContent className="flex gap-2">
                <Button variant="outline" onClick={resetErrorBoundary}>
                  Try Again
                </Button>
                <Button variant="outline" asChild>
                  <Link to="/">Go Home</Link>
                </Button>
              </CardContent>
            </Card>
          )}
        >
          <Suspense fallback={<ProfileSkeleton />}>
            <ProfileContent />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  );
}
