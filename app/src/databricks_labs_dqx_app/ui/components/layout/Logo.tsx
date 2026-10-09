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
    <div className={`flex items-center gap-2 ${className}`}>
      {companyLogoSrc ? (
        <img
          src={companyLogoSrc}
          alt={company ?? ""}
          className="h-6 w-auto max-w-40 object-contain"
          onError={onCompanyLogoError}
        />
      ) : (
        <img src="/dqx-logo.svg" alt="DQX Studio logo" className="h-6 w-6" />
      )}
      {showText && (
        <span className="font-semibold text-lg">
          {__APP_NAME__}
          {company && (
            <>
              <span className="mx-2 opacity-60" aria-hidden="true">
                |
              </span>
              <span>{company}</span>
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
      <Link to={to} className="hover:opacity-80 transition-opacity">
        {content}
      </Link>
    );
  }

  return content;
}

export default Logo;
