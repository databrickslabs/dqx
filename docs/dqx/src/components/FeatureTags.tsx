import React, { CSSProperties, ReactNode } from 'react';
import Link from '@docusaurus/Link';

/**
 * Inline documentation tags for feature lifecycle stage and version information.
 *
 * These components standardize the annotations proposed in issue #445 so authors
 * do not hand-write badge markup. Use them inline next to a page/section heading
 * or in the body text:
 *
 *   import { FeatureLifecycleStage, AvailableSinceVersion, DeprecatedInVersion }
 *     from '@site/src/components/FeatureTags';
 *
 *   # DQX MCP Server <FeatureLifecycleStage stage="beta" />
 *
 *   <AvailableSinceVersion version="0.15.0" />
 *   <DeprecatedInVersion version="0.16.0" replacement="the new_check function" />
 *
 * Version strings are literal props (an annotation is a historical fact tied to a
 * specific release), so they are never inferred at build time.
 */

// GitHub release notes live at the tag; link version tags there so readers can see
// what shipped in that release.
const RELEASE_NOTES_BASE = 'https://github.com/databrickslabs/dqx/releases/tag/v';

// The four lifecycle stages, each mapped to a DQX badge color-modifier class
// (defined in src/css/custom.css) and the anchor of its explanation on the
// Feature lifecycle reference page. All DQX badges also carry the shared
// `dqx-badge` geometry class so they render identically in size.
type Stage = 'experimental' | 'beta' | 'ga' | 'deprecated';

const STAGE_META: Record<Stage, { label: string; badgeClass: string; anchor: string }> = {
  experimental: { label: 'Experimental', badgeClass: 'dqx-badge--experimental', anchor: 'experimental' },
  beta: { label: 'Beta', badgeClass: 'dqx-badge--beta', anchor: 'beta' },
  ga: { label: 'GA', badgeClass: 'dqx-badge--ga', anchor: 'ga' },
  deprecated: { label: 'Deprecated', badgeClass: 'dqx-badge--deprecated', anchor: 'deprecated' },
};

// Sizing sits in the `dqx-badge` CSS class; these style objects only handle how
// a badge is positioned relative to heading text. When `heading` is true the
// badge shrinks to sit inline next to a large title; otherwise it renders at its
// normal (CSS-defined) size.
const HEADING_BADGE_STYLE: CSSProperties = {
  verticalAlign: 'text-top',
  marginLeft: '0.4rem',
  fontSize: '0.19em',
};

const INLINE_BADGE_STYLE: CSSProperties = {};

/**
 * Renders a lifecycle status badge (Experimental, Beta, Deprecated) linked to
 * the matching section of the Feature lifecycle reference page.
 *
 * @param stage    the lifecycle stage to display.
 * @param heading  when true (default), uses heading-friendly sizing so the badge
 *                 can sit inline next to a page or section title; set false for a
 *                 normal-size badge in body text.
 */
export function FeatureLifecycleStage({
  stage,
  heading = true,
}: {
  stage: Stage;
  heading?: boolean;
}): JSX.Element {
  const meta = STAGE_META[stage];
  if (!meta) {
    throw new Error(
      `FeatureLifecycleStage: unknown stage "${stage}". Use one of: ${Object.keys(STAGE_META).join(', ')}.`,
    );
  }
  return (
    <Link
      to={`/docs/reference/feature_lifecycle#${meta.anchor}`}
      title={`Feature lifecycle: ${meta.label}`}
      style={{ textDecoration: 'none' }}
    >
      <span
        className={`dqx-badge ${meta.badgeClass}`}
        style={heading ? HEADING_BADGE_STYLE : INLINE_BADGE_STYLE}
      >
        {meta.label}
      </span>
    </Link>
  );
}

/**
 * Renders an "Available since vX.Y.Z" tag linked to that release's notes.
 *
 * Uses a neutral badge color (distinct from every lifecycle-stage color) so version
 * tags read as metadata rather than a status.
 *
 * @param version  the release the feature first shipped in, e.g. "0.15.0".
 * @param heading  when true (default), uses heading-friendly sizing so the tag can sit
 *                 inline next to a page or section title; set false for body text.
 */
export function AvailableSinceVersion({
  version,
  heading = true,
}: {
  version: string;
  heading?: boolean;
}): JSX.Element {
  return (
    <Link
      to={`${RELEASE_NOTES_BASE}${version}`}
      title={`Available since DQX v${version}`}
      style={{ textDecoration: 'none' }}
    >
      <span
        className='dqx-badge dqx-badge--version'
        style={heading ? HEADING_BADGE_STYLE : INLINE_BADGE_STYLE}
      >
        Available since v{version}
      </span>
    </Link>
  );
}

/**
 * Renders a "Deprecated in vX.Y.Z" tag linked to that release's notes, optionally
 * naming the recommended replacement.
 *
 * @param version      the release the feature was deprecated in, e.g. "0.16.0".
 * @param replacement  optional human-readable replacement to point readers to.
 * @param heading      when true (default), uses heading-friendly sizing so the tag can
 *                     sit inline next to a page or section title; set false for body text.
 */
export function DeprecatedInVersion({
  version,
  replacement,
  heading = true,
}: {
  version: string;
  replacement?: string;
  heading?: boolean;
}): JSX.Element {
  const title = replacement
    ? `Deprecated in DQX v${version}; use ${replacement}`
    : `Deprecated in DQX v${version}`;
  return (
    <Link
      to={`${RELEASE_NOTES_BASE}${version}`}
      title={title}
      style={{ textDecoration: 'none' }}
    >
      <span
        className='dqx-badge dqx-badge--version'
        style={heading ? HEADING_BADGE_STYLE : INLINE_BADGE_STYLE}
      >
        Deprecated in v{version}
      </span>
    </Link>
  );
}

/**
 * Row container that places one or more feature tags on their own line, directly
 * under a heading. Put it immediately after the `#` heading (which stays clean, so
 * no frontmatter `title:` workaround is needed) and pass the tags as children with
 * `heading={false}` so they render at their normal size:
 *
 *   # My Feature
 *
 *   <FeatureTags>
 *     <FeatureLifecycleStage stage="beta" heading={false} />
 *     <AvailableSinceVersion version="0.14.0" heading={false} />
 *   </FeatureTags>
 */
export function FeatureTags({ children }: { children: ReactNode }): JSX.Element {
  return (
    <div
      style={{
        display: 'flex',
        flexWrap: 'wrap',
        alignItems: 'center',
        gap: '0.4rem',
        // Pull up slightly toward the heading and add breathing room before the body.
        marginTop: '-0.5rem',
        marginBottom: '1.5rem',
      }}
    >
      {children}
    </div>
  );
}
