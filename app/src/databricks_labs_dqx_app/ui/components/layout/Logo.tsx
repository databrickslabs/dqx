import { Link } from "@tanstack/react-router";
import { useState } from "react";
import { useBranding } from "@/hooks/use-branding";
import { useIsDarkMode } from "@/hooks/use-is-dark-mode";
import { headerTitle, logoUrl, pickHeaderLogo } from "@/lib/branding/header";

interface LogoProps {
  to?: string;
  className?: string;
  showText?: boolean;
  /** Setup Wizard passes false: it always shows the plain DQX logo. */
  branded?: boolean;
}

interface LogoContentProps {
  className: string;
  showText: boolean;
  /** Company name and logo overrides; null renders the plain DQX identity. */
  company: string | null;
  companyLogoSrc: string | null;
  onCompanyLogoError: () => void;
}

function LogoContent({ className, showText, company, companyLogoSrc, onCompanyLogoError }: LogoContentProps) {
  return (
    <div className={`flex min-w-0 items-center gap-2 ${className}`}>
      {companyLogoSrc ? (
        <img
          src={companyLogoSrc}
          // The name is already read out as text next to the logo, so the image is decorative then.
          alt={showText ? "" : (company ?? "")}
          className="h-6 w-auto max-w-40 shrink-0 object-contain"
          onError={onCompanyLogoError}
        />
      ) : (
        <img src="/dqx-logo.svg" alt="DQX Studio logo" className="h-6 w-6 shrink-0" />
      )}
      {showText && (
        <span className="flex min-w-0 items-center font-semibold text-lg">
          <span className="shrink-0">{__APP_NAME__}</span>
          {company && (
            <>
              <span className="mx-2 shrink-0 opacity-60" aria-hidden="true">
                |
              </span>
              <span className="truncate max-w-[16rem]" title={company}>
                {company}
              </span>
            </>
          )}
        </span>
      )}
    </div>
  );
}

/** Reads branding (hooks live here so the unbranded Logo needs no query client). */
function BrandedLogoContent({ className, showText }: { className: string; showText: boolean }) {
  const branding = useBranding();
  const isDark = useIsDarkMode();
  const [failedSrc, setFailedSrc] = useState<string | null>(null);
  const picked = branding ? pickHeaderLogo(branding, isDark) : null;
  const src = picked ? logoUrl(picked.slot, picked.hash) : null;
  return (
    <LogoContent
      className={className}
      showText={showText}
      company={headerTitle(branding?.companyName).company}
      companyLogoSrc={src !== null && failedSrc !== src ? src : null}
      onCompanyLogoError={() => setFailedSrc(src)}
    />
  );
}

function Logo({ to = "/home", className = "", showText = true, branded = true }: LogoProps) {
  const content = branded ? (
    <BrandedLogoContent className={className} showText={showText} />
  ) : (
    <LogoContent className={className} showText={showText} company={null} companyLogoSrc={null} onCompanyLogoError={() => undefined} />
  );

  if (to) {
    return (
      <Link to={to} className="min-w-0 hover:opacity-80 transition-opacity">
        {content}
      </Link>
    );
  }

  return content;
}

export default Logo;
