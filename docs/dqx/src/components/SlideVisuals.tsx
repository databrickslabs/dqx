import React, { useEffect, useRef, useState } from 'react';
import { Highlight, themes } from 'prism-react-renderer';
import type { Token, RenderProps } from 'prism-react-renderer';

export type VisualKey =
  | 'title' | 'known-unknowns' | 'dqx-rules' | 'trucks-dqm'
  | 'gap' | 'row-level' | 'features' | 'isolation-forest'
  | 'baseline-groups' | 'correlation' | 'shap' | 'summary' | 'coming-soon';

// ── Data ────────────────────────────────────────────────────────────

const BANANA_VARIANTS: Array<{ filter: string; scale?: number; title?: string }> = [
  { filter: 'none' },
  { filter: 'sepia(0.08) hue-rotate(-4deg)' },
  { filter: 'sepia(0.12) saturate(0.92)' },
  { filter: 'hue-rotate(72deg) saturate(1.15)', title: 'Unripe' },
  { filter: 'sepia(0.05) hue-rotate(3deg)' },
  { filter: 'saturate(0.88)' },
  { filter: 'sepia(0.9) brightness(0.4) contrast(1.15)', title: 'Overripe' },
  { filter: 'hue-rotate(-5deg)' },
  { filter: 'sepia(0.1) saturate(0.95)' },
  { filter: 'brightness(0.98) contrast(1.02)' },
  { filter: 'sepia(0.14) hue-rotate(4deg)' },
  { filter: 'saturate(1.08) hue-rotate(-2deg)' },
];

const TRUCK_CONFIGS: Array<{ bananaCount: number; label: string; isAnomaly: boolean; isLate?: boolean }> = [
  { bananaCount: 4, label: 'Full', isAnomaly: false },
  { bananaCount: 3, label: 'Full', isAnomaly: false },
  { bananaCount: 4, label: 'Full', isAnomaly: false },
  { bananaCount: 0, label: 'Empty', isAnomaly: true },
  { bananaCount: 3, label: 'Full', isAnomaly: false },
  { bananaCount: 4, label: 'Full', isAnomaly: false },
  { bananaCount: 3, label: 'Full', isAnomaly: false },
  { bananaCount: 4, label: 'Full', isAnomaly: false },
  { bananaCount: 3, label: 'Late', isAnomaly: true, isLate: true },
  { bananaCount: 3, label: 'Full', isAnomaly: false },
  { bananaCount: 4, label: 'Full', isAnomaly: false },
  { bananaCount: 1, label: 'Low', isAnomaly: true },
  { bananaCount: 3, label: 'Full', isAnomaly: false },
  { bananaCount: 6, label: 'Overfull', isAnomaly: true },
  { bananaCount: 4, label: 'Full', isAnomaly: false },
  { bananaCount: 3, label: 'Full', isAnomaly: false },
  { bananaCount: 4, label: 'Full', isAnomaly: false },
  { bananaCount: 3, label: 'Full', isAnomaly: false },
  { bananaCount: 4, label: 'Full', isAnomaly: false },
  { bananaCount: 3, label: 'Full', isAnomaly: false },
];

const BANANA_FEATURES = ['size', 'colour', 'shape', 'flavour', 'smell'] as const;

// Baseline conditioning: one arriving size, judged three times. 18cm is unremarkable across the whole
// crate -- it sits inside the combined range -- and clearly wrong for a lady finger. That is the whole
// argument for `baseline_by` in one number.
const ARRIVING_SIZE_CM = 18;
const BANANA_VARIETIES: Array<{ name: string; emoji: string; scale: number; low: number; high: number }> = [
  { name: 'Plantain', emoji: '🍌', scale: 1.3, low: 22, high: 30 },
  { name: 'Cavendish', emoji: '🍌', scale: 1.0, low: 16, high: 22 },
  { name: 'Lady finger', emoji: '🍌', scale: 0.72, low: 10, high: 14 },
];

// Correlation break, as a packing line. One crate at one ripeness -- colour deliberately does NOT vary,
// because it is the most salient channel on screen and varying it makes the eye nominate the wrong fruit,
// which then makes the gate's verdict look arbitrary. What varies visibly is LENGTH, one of the two numbers
// the check reads. WEIGHT is invisible, which is the whole reason a check is needed.
const CRATE_RIPENESS = [
  'sepia(.08) saturate(1.04)',
  'sepia(.13) saturate(1.02)',
  'sepia(.05) saturate(1.06)',
  'sepia(.1) saturate(1.03)',
];
const CRATE_NORMAL: Array<{ len: number; wt: number }> = [
  { len: 15, wt: 83 },
  { len: 16, wt: 90 },
  { len: 17, wt: 97 },
  { len: 18, wt: 104 },
  { len: 20, wt: 117 },
  { len: 21, wt: 124 },
  { len: 22, wt: 131 },
  { len: 23, wt: 138 },
];
// 19cm sits mid-pack, so nothing about it looks wrong. 19cm of banana weighs ~110g; this one is 38g.
const CRATE_ODD = { len: 19, wt: 38, bad: true };

// Longer fruit is drawn larger, so the visible variation is a variable the check actually reads.
const fruitSize = (len: number) => `${(1.3 + ((len - 15) / 8) * 0.8).toFixed(2)}rem`;
const expectedWeight = (len: number) => Math.round(len * 5.8);

// Belt geometry, in percent of track width per second.
const LINE_GATE_X = 50;
const LINE_SPEED = 4.6;      // a banana crosses the gate about every 4.3s -- slow enough to read
const LINE_SPACING = 20;
const LINE_COUNT = 8;
const LINE_HOLD_MS = 4200;   // the line halts this long on a failure

const DQX_RULES_CODE = `from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.check_funcs import is_not_null, is_in_range, is_in_list

# One banana: check size, colour, flavour
rules = [
    DQRowRule(check_func=is_not_null, column="banana"),
    DQRowRule(check_func=is_in_range, column="size_cm",
        check_func_kwargs={"min_limit": 15, "max_limit": 25}),
    DQRowRule(check_func=is_in_list, column="colour",
        check_func_kwargs={"allowed": ["yellow", "green"]}),
    DQRowRule(check_func=is_in_list, column="flavour",
        check_func_kwargs={"allowed": ["sweet", "ripe"]}),
]`;

const FEATURE_VECTOR_ROWS = [
  { feature: 'Size (Length)', value: '15 cm' },
  { feature: 'Spots (Count)', value: '2' },
  { feature: 'Bend (Angle)', value: '30°' },
  { feature: 'Ripeness (1=green, 7=brown)', value: '5' },
];

const SHAP_EXAMPLES = [
  { contributors: [{ label: 'too brown', value: 0.6 }, { label: 'wrong size', value: 0.3 }] },
  { contributors: [{ label: 'spots', value: 0.55 }, { label: 'bend', value: 0.35 }] },
  { contributors: [{ label: 'size', value: 0.7 }, { label: 'ripeness', value: 0.2 }] },
  { contributors: [{ label: 'colour', value: 0.5 }, { label: 'size', value: 0.4 }] },
];

const SUMMARY_BULLETS: Array<{ title: string; detail?: string; code?: string }> = [
  {
    title: 'Two calls to get started',
    detail: 'Train on good data, then add one check. No config files, no tuning — defaults work for most tables.',
    code: 'anomaly_engine.train(df, model_name=..., registry_table=...)\n\n# Then add to your existing DQ checks:\nDQDatasetRule(check_func=has_no_row_anomalies, ...)',
  },
  {
    title: 'No ML expertise needed',
    detail: 'DQX auto-discovers which columns to use, engineers features, and even finds a grouping to judge each row against when one makes sense. You just point it at a table.',
  },
  {
    title: 'Explainable results',
    detail: 'Every flagged row comes with a breakdown of why — which columns drove the score. So you can act on it, not just stare at a number.',
  },
  {
    title: 'Works with batch and streaming',
    detail: 'Train on historical data, then score batch tables or streaming DataFrames. Same API, same checks.',
  },
  {
    title: 'Knows when to retrain',
    detail: 'Enable drift detection and DQX warns you when scoring data looks different from training data. No guessing when models go stale.',
    code: 'has_no_row_anomalies(..., drift_threshold=3.0)',
  },
  {
    title: 'Complements Databricks DQM',
    detail: 'DQM watches table health (freshness, row counts). DQX checks the rows inside. Use both for full coverage.',
  },
];

const BANANA_FACTS = [
  "Over 100 billion bananas are eaten worldwide every year.",
  "Bananas are the world's most popular fruit — and the fourth most valuable food crop after rice, wheat, and maize.",
  "The banana plant isn't a tree — it's the world's largest herbaceous flowering plant.",
  "India produces more bananas than any other country — nearly 30 million tonnes a year.",
  "Bananas share about 60% of their DNA with humans.",
  "The Cavendish banana we eat today replaced the Gros Michel, which was wiped out by a fungus in the 1950s.",
  "A cluster of bananas is called a hand. A single banana is called a finger.",
  "Bananas float in water because they're less dense than water.",
  "Wild bananas are full of large, hard seeds — the ones we eat are sterile clones.",
  "Ecuador is the world's largest banana exporter, shipping over 6 million tonnes a year.",
];

const PIP_INSTALL_CMD = "pip install 'databricks-labs-dqx[anomaly]'";

// ── Components ──────────────────────────────────────────────────────

function PipInstallTyping() {
  const [animKey, setAnimKey] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setAnimKey(k => k + 1), 30_000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="bv-terminal" aria-hidden>
      <div className="bv-terminal__line">
        <span className="bv-terminal__prompt">$</span>
        <span>&nbsp;</span>
        <div className="bv-terminal__cmd">
          <span className="bv-terminal__ghost">{PIP_INSTALL_CMD}</span>
          <span key={animKey} className="bv-terminal__typed">{PIP_INSTALL_CMD}</span>
        </div>
        <span className="bv-terminal__cursor" />
      </div>
    </div>
  );
}

function TitleCarousel() {
  return (
    <div className="bv-title">
      <div className="bv-title__carousel-wrap">
        <div className="bv-title__carousel-track">
          {[0, 1].map(copy => (
            <div key={copy} className="bv-title__carousel-set">
              {BANANA_VARIANTS.map((v, i) => (
                <span
                  key={i}
                  className="bv-title__banana"
                  style={{ filter: v.filter, transform: v.scale ? `scale(${v.scale})` : undefined }}
                  title={v.title}
                >🍌</span>
              ))}
            </div>
          ))}
        </div>
      </div>
      <div className="bv-title__install">
        <PipInstallTyping />
      </div>
    </div>
  );
}

function BananaPop({ delay = 0 }: { delay?: number }) {
  return (
    <span className="bv-pop-in" style={{ animationDelay: `${delay}s`, fontSize: '1.75rem' }}>🍌</span>
  );
}

function KnownUnknowns() {
  const [ship, setShip] = useState(false);
  const [drop, setDrop] = useState(false);
  useEffect(() => { const t = setTimeout(() => setShip(true), 2000); return () => clearTimeout(t); }, []);
  useEffect(() => { if (!ship) return; const t = setTimeout(() => setDrop(true), 1200); return () => clearTimeout(t); }, [ship]);

  return (
    <div className="bv-known" aria-hidden>
      <div className="bv-known__grid">
        <div className="bv-known__cell bv-known__cell--green">
          <strong>Known knowns</strong>
          <div className="bv-known__icons">
            <BananaPop delay={0} />
            <BananaPop delay={0.2} />
            <BananaPop delay={0.4} />
          </div>
          <small>Rules: count, size in range</small>
        </div>
        <div className="bv-known__cell bv-known__cell--amber">
          <strong>Known unknowns</strong>
          <div className="bv-known__icons">
            <BananaPop delay={0} />
            <span className="bv-pop-in" style={{ animationDelay: '0.25s', fontSize: '1.5rem' }}>?</span>
          </div>
          <small>Size varies — define range?</small>
        </div>
        <div className="bv-known__cell bv-known__cell--stone">
          <strong>Unknown knowns</strong>
          <BananaPop delay={0.1} />
          <small>Blind spots</small>
        </div>
        <div className="bv-known__cell bv-known__cell--red">
          <strong>Unknown unknowns</strong>
          <div className="bv-known__ufo-area">
            {ship && (
              <div className="bv-known__ufo-container">
                <span className="bv-alien-ship">🛸</span>
                {drop && <span className="bv-alien-banana">🍌</span>}
              </div>
            )}
          </div>
          <small>Surprise! Odd banana → anomaly detection</small>
        </div>
      </div>
      <p className="bv-known__footnote">It&apos;s what you don&apos;t know you don&apos;t know that gets you.</p>
    </div>
  );
}

const DQX_LINE_STAGGER_S = 0.78;

function HighlightedLine({ line, getTokenProps }: { line: Token[]; getTokenProps: RenderProps['getTokenProps'] }) {
  return (
    <>
      {line.map((token, key) => (
        <span key={key} {...getTokenProps({ token })} />
      ))}
    </>
  );
}

function DqxCodeBlock() {
  return (
    <Highlight theme={themes.github} code={DQX_RULES_CODE} language="python">
      {({ tokens, getTokenProps }) => (
        <div className="bv-code-block">
          <div className="bv-code-block__dimmed">
            {tokens.map((line, i) => (
              <div key={i} className="bv-code-block__line">
                <HighlightedLine line={line} getTokenProps={getTokenProps} />
              </div>
            ))}
          </div>
          <div className="bv-code-block__typed-overlay">
            {tokens.map((line, i) => (
              <div key={i} className="bv-code-block__line">
                <span className="bv-code-line-reveal" style={{ animationDelay: `${i * DQX_LINE_STAGGER_S}s` }}>
                  <HighlightedLine line={line} getTokenProps={getTokenProps} />
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </Highlight>
  );
}

function DqxRules() {
  const [idx, setIdx] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setIdx(i => (i + 1) % BANANA_FEATURES.length), 2800);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="bv-dqx" aria-hidden>
      <span className="bv-dqx__banana">🍌</span>
      <div className="bv-dqx__label-area">
        <span key={idx} className="bv-label-pop">{BANANA_FEATURES[idx]}</span>
      </div>
      <pre className="bv-dqx__code-wrap">
        <DqxCodeBlock />
      </pre>
    </div>
  );
}

function TruckCard({ bananaCount, label, isAnomaly, isLate }: typeof TRUCK_CONFIGS[0]) {
  return (
    <div className={`bv-truck ${isAnomaly ? 'bv-truck--bad' : ''}`} title={isAnomaly ? `Anomaly: ${label}` : undefined}>
      <span className="bv-truck__icon">🚛</span>
      <div className="bv-truck__bananas">
        {Array.from({ length: bananaCount }, (_, i) => <span key={i}>🍌</span>)}
      </div>
      <span className={`bv-truck__label ${isAnomaly ? 'bv-truck__label--bad' : ''}`}>
        {isLate && '⏱ '}{label}
      </span>
    </div>
  );
}

function TrucksDqm() {
  return (
    <div className="bv-trucks" aria-hidden>
      <div className="bv-trucks__track">
        {[0, 1].map(copy => (
          <div key={copy} className="bv-trucks__set">
            {TRUCK_CONFIGS.map((config, i) => (
              <TruckCard key={`${copy}-${i}`} {...config} />
            ))}
          </div>
        ))}
      </div>
      <p className="bv-trucks__caption">DQM checks the trucks</p>
      <div className="bv-trucks__setup">
        <p className="bv-trucks__setup-label">How to set up DQM</p>
        <img
          src="/dqx/img/dqm-setup.png"
          alt="Data quality monitoring setup: anomaly detection toggle and description"
          className="bv-trucks__setup-img"
        />
      </div>
    </div>
  );
}

function Gap() {
  return (
    <div className="bv-gap" aria-hidden>
      <div className="bv-gap__box">
        <span className="bv-gap__crate">📦</span>
        <span className="bv-gap__q">?</span>
      </div>
      <p className="bv-gap__text">Delivery ✓&nbsp;&nbsp;Contents ???</p>
    </div>
  );
}

function RowLevelCycle() {
  const [centerIdx, setCenterIdx] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setCenterIdx(i => (i + 1) % 3), 2000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="bv-row" aria-hidden>
      <div className="bv-row__flow">
        <div className="bv-row__step">
          <div className="bv-row__train-bananas">
            <span style={{ fontSize: '1.5rem', transform: 'scale(0.9)' }}>🍌</span>
            <span style={{ fontSize: '1.8rem' }}>🍌</span>
            <span style={{ fontSize: '1.5rem', transform: 'scale(0.75)' }} title="Unusual">🍌</span>
            <span style={{ fontSize: '1.8rem' }}>🍌</span>
            <span style={{ fontSize: '1.5rem', transform: 'scale(1.1)' }} title="Unusual">🍌</span>
            <span style={{ fontSize: '1.3rem' }}>📚</span>
          </div>
          <small className="bv-row__label">Train ML Model</small>
        </div>
        <span className="bv-row__arrow">→</span>
        <div className="bv-row__step">
          <span style={{ fontSize: '2.5rem' }}>🧠</span>
          <small className="bv-row__label">ML Model</small>
        </div>
        <span className="bv-row__arrow">→</span>
        <div className="bv-row__step">
          <div className="bv-row__check-area">
            <div className="bv-row__check-bananas">
              {[0, 1, 2].map(i => (
                <span key={i} style={{
                  fontSize: '1.8rem',
                  opacity: i === centerIdx ? 1 : 0.35,
                  transform: i === centerIdx ? 'scale(1.1)' : 'scale(0.9)',
                  transition: 'all 0.5s ease-in-out',
                }}>🍌</span>
              ))}
            </div>
            <div className="bv-magnifier-orbit">
              <span className="bv-magnifier-orbit__glass">🔍</span>
            </div>
          </div>
          <small className="bv-row__label">Check Banana</small>
        </div>
      </div>
    </div>
  );
}

function FeaturesSlide() {
  return (
    <div className="bv-features" aria-hidden>
      <h4 className="bv-features__heading">Feature Engineering</h4>
      <div className="bv-features__top">
        <div className="bv-features__banana-labels">
          <span className="bv-features__banana-big">🍌</span>
          <span className="bv-features__tag" style={{ top: -2, left: 4 }}>color</span>
          <span className="bv-features__tag" style={{ top: '50%', left: -36 }}>size</span>
          <span className="bv-features__tag" style={{ top: -2, right: 4 }}>spots</span>
          <span className="bv-features__tag" style={{ bottom: 0, right: 8 }}>bend</span>
        </div>
        <span className="bv-features__arrow">→</span>
        <table className="bv-features__table">
          <thead>
            <tr><th>Feature</th><th>Value</th></tr>
          </thead>
          <tbody>
            {FEATURE_VECTOR_ROWS.map((r, i) => (
              <tr key={i}><td>{r.feature}</td><td className="bv-features__val">{r.value}</td></tr>
            ))}
          </tbody>
        </table>
      </div>
      <span className="bv-features__down-arrow">↓</span>
      <h4 className="bv-features__subheading">Unsupervised: Isolation Forest (Feature Splitting)</h4>
      <div className="bv-features__tree">
        <div className="bv-tree-node bv-tree-node--root">Start (All Data)</div>
        <span className="bv-tree-arrow">↓</span>
        <div className="bv-tree-node bv-tree-node--root">Spots (Count) &gt; 3?</div>
        <div className="bv-tree-branches">
          <div className="bv-tree-branch">
            <small>Yes (many spots)</small>
            <span className="bv-tree-arrow">↓</span>
            <div className="bv-tree-node bv-tree-node--anomaly">Anomaly (Short Path)</div>
          </div>
          <div className="bv-tree-branch">
            <small>No (e.g. 2 spots)</small>
            <span className="bv-tree-arrow">↓</span>
            <div className="bv-tree-node bv-tree-node--root">Size (Length) &lt; 12 cm?</div>
            <div className="bv-tree-branches bv-tree-branches--inner">
              <div className="bv-tree-branch">
                <small>Yes (too small)</small>
                <span className="bv-tree-arrow">↓</span>
                <div className="bv-tree-node bv-tree-node--anomaly">Anomaly</div>
              </div>
              <div className="bv-tree-branch">
                <small>No (e.g. 15 cm)</small>
                <span className="bv-tree-arrow">↓</span>
                <div className="bv-tree-node bv-tree-node--normal">Normal (Long Path)</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function IsolationForest() {
  return (
    <div className="bv-iso" aria-hidden>
      <div className="bv-iso__pair">
        <div className="bv-iso__card bv-iso__card--anomaly">
          <span style={{ fontSize: '3rem' }}>🍌</span>
          <strong>Easy to isolate → odd</strong>
        </div>
        <span className="bv-iso__vs">vs</span>
        <div className="bv-iso__card bv-iso__card--normal">
          <div className="bv-iso__crowd">
            <span>🍌</span><span>🍌</span><span>🍌</span><span>🍌</span>
          </div>
          <strong>Hard to isolate → normal</strong>
        </div>
      </div>
    </div>
  );
}

function ShapCarousel() {
  const n = SHAP_EXAMPLES.length;
  const [center, setCenter] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setCenter(i => (i + 1) % n), 3000);
    return () => clearInterval(id);
  }, [n]);

  return (
    <div className="bv-shap" aria-hidden>
      <p className="bv-shap__subtitle">Why was this banana flagged? Top contributing features</p>
      <div className="bv-shap__cards">
        {[-1, 0, 1].map(off => {
          const i = (center + off + n) % n;
          const isCenter = off === 0;
          const ex = SHAP_EXAMPLES[i];
          return (
            <div key={off} className={`bv-shap__card ${isCenter ? 'bv-shap__card--center' : 'bv-shap__card--side'}`}>
              <span className={`bv-shap__banana ${isCenter ? 'bv-shap__banana--ring' : ''}`}>🍌</span>
              <small className="bv-shap__flagged">Flagged</small>
              {ex.contributors.map((c, j) => (
                <div key={j} className="bv-shap__bar-group">
                  <div className="bv-shap__bar-label">
                    <span>{c.label}</span>
                    <span className="bv-shap__bar-value">{c.value}</span>
                  </div>
                  <div className="bv-shap__bar-bg">
                    <div className="bv-shap__bar-fill" style={{ width: `${Math.round(c.value * 100)}%` }} />
                  </div>
                </div>
              ))}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function SummarySlide() {
  return (
    <ul className="bv-summary" aria-hidden>
      {SUMMARY_BULLETS.map((b, i) => (
        <li key={i} className="bv-summary__bullet" style={{ animationDelay: `${i * 0.55}s` }}>
          <strong>{i + 1}. {b.title}</strong>
          {b.detail && <span className="bv-summary__detail">{b.detail}</span>}
          {b.code && (
            <pre className="bv-summary__code"><code>{b.code}</code></pre>
          )}
        </li>
      ))}
    </ul>
  );
}

function ComingSoonSlide() {
  const [fi, setFi] = useState(0);
  const [animKey, setAnimKey] = useState(0);

  useEffect(() => {
    const id = setInterval(() => setFi(i => (i + 1) % BANANA_FACTS.length), 4000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const id = setInterval(() => setAnimKey(k => k + 1), 30_000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="bv-coming">
      <div className="bv-coming__cta">
        <span style={{ fontSize: '2.5rem' }}>🍌</span>
        <strong className="bv-coming__try">Try it now!</strong>
      </div>
      <div className="bv-terminal" aria-hidden>
        <div className="bv-terminal__line">
          <span className="bv-terminal__prompt">$</span>
          <span>&nbsp;</span>
          <div className="bv-terminal__cmd">
            <span className="bv-terminal__ghost">{PIP_INSTALL_CMD}</span>
            <span key={animKey} className="bv-terminal__typed">{PIP_INSTALL_CMD}</span>
          </div>
          <span className="bv-terminal__cursor" />
        </div>
      </div>
      <div key={fi} className="bv-coming__fact bv-fact-enter">
        🍌 {BANANA_FACTS[fi]}
      </div>
    </div>
  );
}

// ── Export ───────────────────────────────────────────────────────────

function BaselineGroups() {
  const [activeIdx, setActiveIdx] = useState(BANANA_VARIETIES.length - 1);
  useEffect(() => {
    const id = setInterval(() => setActiveIdx(i => (i + 1) % BANANA_VARIETIES.length), 2200);
    return () => clearInterval(id);
  }, []);

  const combinedLow = Math.min(...BANANA_VARIETIES.map(v => v.low));
  const combinedHigh = Math.max(...BANANA_VARIETIES.map(v => v.high));

  return (
    <div className="bv-baseline" aria-hidden>
      <p className="bv-baseline__intro">
        A <strong>{ARRIVING_SIZE_CM}cm</strong> banana arrives. Is it odd?
      </p>
      <div className="bv-baseline__groups">
        {BANANA_VARIETIES.map((variety, i) => {
          const isOdd = ARRIVING_SIZE_CM < variety.low || ARRIVING_SIZE_CM > variety.high;
          const isActive = i === activeIdx;
          return (
            <div
              key={variety.name}
              className={`bv-baseline__card ${isActive ? 'bv-baseline__card--active' : ''} ${
                isOdd ? 'bv-baseline__card--odd' : 'bv-baseline__card--normal'
              }`}
            >
              <span className="bv-baseline__emoji" style={{ fontSize: `${1.6 * variety.scale}rem` }}>
                {variety.emoji}
              </span>
              <strong className="bv-baseline__name">{variety.name}</strong>
              <small className="bv-baseline__range">
                usually {variety.low}–{variety.high}cm
              </small>
              <span className="bv-baseline__verdict">{isOdd ? 'odd here' : 'normal here'}</span>
            </div>
          );
        })}
      </div>
      <p className="bv-baseline__whole">
        Across the whole crate: {combinedLow}–{combinedHigh}cm — so {ARRIVING_SIZE_CM}cm looks fine, and a
        model comparing against the whole crate never flags it.
      </p>
    </div>
  );
}

type LineItem = { len: number; wt: number; bad?: boolean; filter: string };
type Rider = { item: LineItem; x: number; lift: number };

function CorrelationBreak() {
  const trackRef = useRef<HTMLDivElement>(null);
  const elsRef = useRef<Array<HTMLSpanElement | null>>([]);
  const [reading, setReading] = useState<LineItem | null>(null);

  useEffect(() => {
    const seqRef = { n: 0 };
    const pick = (): LineItem => {
      seqRef.n += 1;
      const base = seqRef.n % 4 === 0 ? CRATE_ODD : CRATE_NORMAL[seqRef.n % CRATE_NORMAL.length];
      return { ...base, filter: CRATE_RIPENESS[seqRef.n % CRATE_RIPENESS.length] };
    };

    const riders: Rider[] = Array.from({ length: LINE_COUNT }, (_, i) => ({
      item: pick(),
      x: 105 - i * LINE_SPACING,
      lift: 0,
    }));

    // A percentage in translateX resolves against the ELEMENT's width, not the track's, so positions are
    // converted to pixels here. The trailing -50% is kept because centring the glyph on its position is the
    // one place percent-of-self is what we want.
    const place = (r: Rider, el: HTMLSpanElement | null, trackWidth: number) => {
      if (!el) return;
      el.style.transform =
        `translateX(${((r.x / 100) * trackWidth).toFixed(1)}px) translateX(-50%) translateY(${-r.lift.toFixed(1)}px)`;
      el.style.filter = r.item.filter;
      el.style.fontSize = fruitSize(r.item.len);
    };

    let raf = 0;
    let last = performance.now();
    let holdUntil = 0;
    let culprit: number | null = null;

    const frame = (now: number) => {
      const dt = Math.min((now - last) / 1000, 0.05);
      last = now;
      const width = trackRef.current?.clientWidth || 1;

      if (now < holdUntil) {
        // Hollow, so it drifts up off the belt rather than dropping. That is the only visible tell.
        if (culprit !== null) {
          riders[culprit].lift = Math.min(riders[culprit].lift + 16 * dt, 26);
          place(riders[culprit], elsRef.current[culprit], width);
        }
        raf = requestAnimationFrame(frame);
        return;
      }

      if (culprit !== null) {
        const r = riders[culprit];
        r.x = 105;
        r.lift = 0;
        r.item = pick();
        culprit = null;
        setReading(null);
      }

      riders.forEach((r, i) => {
        const was = r.x;
        r.x -= LINE_SPEED * dt;
        if (was > LINE_GATE_X && r.x <= LINE_GATE_X) {
          setReading(r.item);
          if (r.item.bad) {
            culprit = i;
            holdUntil = now + LINE_HOLD_MS;
          }
        }
        if (r.x < -8) {
          r.x = 105;
          r.item = pick();
        }
        place(r, elsRef.current[i], width);
      });

      raf = requestAnimationFrame(frame);
    };

    raf = requestAnimationFrame(frame);
    return () => cancelAnimationFrame(raf);
  }, []);

  const fail = !!reading?.bad;

  return (
    <div className={`bv-line ${fail ? 'bv-line--hold' : ''}`} aria-hidden>
      <span className={`bv-line__badge ${fail ? 'bv-line__badge--fail' : ''}`}>
        {reading
          ? <>{reading.len}cm · {reading.wt}g &nbsp;<b>pair {fail ? '✗' : '✓'}</b></>
          : 'waiting…'}
      </span>

      <span className={`bv-line__gantry ${fail ? 'bv-line__gantry--fail' : ''}`}>
        <span className="bv-line__scan" />
      </span>
      <span className={`bv-line__chute ${fail ? 'bv-line__chute--on' : ''}`} />

      <div className="bv-line__track" ref={trackRef}>
        {Array.from({ length: LINE_COUNT }, (_, i) => (
          <span
            key={i}
            className="bv-line__fruit"
            ref={el => { elsRef.current[i] = el; }}
          >🍌</span>
        ))}
      </div>

      <span className="bv-line__belt">
        <span className="bv-line__slats" />
        <span className="bv-line__lip bv-line__lip--top" />
        <span className="bv-line__lip bv-line__lip--bottom" />
      </span>
      <span className="bv-line__rollers">
        {Array.from({ length: 9 }, (_, i) => <span key={i} className="bv-line__roller" />)}
      </span>
      <span className="bv-line__legs">
        {['0%', '33%', '66%', '100%'].map(left => <span key={left} className="bv-line__leg" style={{ left }} />)}
      </span>
      <span className="bv-line__floor" />

      <p className="bv-line__caption">
        {fail ? (
          <>
            <strong>19 cm, 38 g.</strong> Both mid-range, so every one-column rule says pass — but a banana
            that long weighs about {expectedWeight(CRATE_ODD.len)} g. It is hollow, and only the pair gives it away.
          </>
        ) : (
          <>Two numbers read off each banana. Both ordinary, and consistent with each other. <strong>Pass.</strong></>
        )}
      </p>
    </div>
  );
}

export default function SlideVisuals({ visual }: { visual: VisualKey }) {
  switch (visual) {
    case 'title': return <TitleCarousel />;
    case 'known-unknowns': return <KnownUnknowns />;
    case 'dqx-rules': return <DqxRules />;
    case 'trucks-dqm': return <TrucksDqm />;
    case 'gap': return <Gap />;
    case 'row-level': return <RowLevelCycle />;
    case 'features': return <FeaturesSlide />;
    case 'isolation-forest': return <IsolationForest />;
    case 'baseline-groups': return <BaselineGroups />;
    case 'correlation': return <CorrelationBreak />;
    case 'shap': return <ShapCarousel />;
    case 'summary': return <SummarySlide />;
    case 'coming-soon': return <ComingSoonSlide />;
    default: return null;
  }
}
