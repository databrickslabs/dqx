import React, { useEffect, useId, useState } from 'react';
import type { KeyboardEvent, ReactNode } from 'react';

const slides = [
  [
    'The odd banana',
    'A little fruit. A useful way to understand row anomaly detection.',
  ],
  [
    'DQM + DQX: better together',
    'DQM watches the delivery. DQX checks the bananas inside.',
  ],
  [
    'Some problems already have a rule',
    'A missing weight or an impossible length has a clear check. Keep those checks.',
  ],
  [
    'Learning from historical data',
    'Train on representative data, then check new rows against what the model learned.',
  ],
  [
    'Two profiles, different strengths',
    'Choose the pattern you care about. A timestamp alone does not choose the detector.',
  ],
  [
    'Looks ordinary. Weighs differently.',
    'Length and weight each fit the observed range. Their combination tells another story.',
  ],
  [
    'Normal for which bunch?',
    'The same length can be ordinary for one variety and unusual for another.',
  ],
  [
    'Normal for when?',
    'A busy packing day can be ordinary when it follows a learned weekly pattern.',
  ],
  [
    'How much needs a second look?',
    'The threshold changes sensitivity. Higher values flag fewer rows and can miss useful findings.',
  ],
  [
    'A clue to inspect',
    'Contributions point to the columns behind a score. You decide whether the data are wrong.',
  ],
  [
    'Three outcomes worth knowing',
    'A quiet warning column does not always mean the model could judge the row.',
  ],
  [
    'Even a good baseline needs attention',
    'New suppliers, new processes and changing patterns can make old comparisons less useful.',
  ],
  ['Try DQX anomaly detection', 'Give your next batch of data a closer look.'],
] as const;

const notes = [
  'One banana represents one input row. The pictures and numbers illustrate concepts, not measured detector results.',
  'Databricks Data Quality Monitoring (DQM) includes table-health anomaly monitoring for freshness and completeness. DQX combines explicit quality rules with checks for unusual rows. This picture highlights complementary layers, not an exhaustive product comparison or a guarantee of complete coverage.',
  'Use required-field, range and membership rules for known requirements. Statistical rarity alone does not establish a defect.',
  'Choose representative training history. Exclude identifiers and labels from model features. Keep a separate validation slice to evaluate the results.',
  'tabular is the default general-purpose profile. correlation is the relationship-sensitive profile. Both score rows independently. Neither is a sequence forecaster.',
  'Illustration: historical lengths span 15–24 cm and weights span 80–143 g. The new reading is 22 cm and 94 g, inside both ranges but away from their usual relationship. This is not a model execution or a claim that tabular can never detect a relationship departure.',
  'baseline_by adds learned group-level context. It does not automatically create a different relationship model per group. An unseen group can remain unscored.',
  'baseline_over_time uses a time-dependent expectation fitted during training. It requires enough representative history. The same timestamp must not also be a model feature. A continuing pattern and a newly changed regime are different cases.',
  'The displayed scores are invented teaching values. Threshold 95 refers to fitted severity calibration. It is not a 95% chance of error or a guarantee that 5% of future rows will flag. Validate settings on separate data.',
  'AI output includes narrative, business_impact, action, group_size, group_avg_severity, top_drivers and evidence_scope. One explanation is shared by rows with the same top contributing features. This illustration uses 12 rows with mean contributions of 65%, 30% and 5%, and mean severity 99.2. Shares describe anomaly evidence, not percentages of the score or error probabilities. redact_columns=["weight"] removes weight evidence from the prompt. The remaining shares become about 86% length and 14% ripeness, and evidence_scope becomes limited. Redaction can also change which rows share an explanation. This illustration holds the group fixed to show the percentage calculation. Scores and row contribution results stay unchanged. Redaction is not table access control. AI wording varies, suggested impact needs review, and serving calls add cost. No model or AI calls run here.',
  'For scored rows, use is_anomaly and the check reporting columns. An unseen baseline has a null score and is_new_baseline=true. Filtered rows can also have null scores. Report coverage separately from alerts.',
  'baseline_by and baseline_over_time provide context learned during training. They do not retrain automatically. Investigate an alert surge before choosing a new reviewed training window. Keep the old model for comparison and rollback.',
  'Use the current release guide for installation, permissions and complete API examples. For the first investigation, disable AI explicitly if you do not want serving calls. This deck never executes DQX.',
] as const;

function Fruit({
  className = '',
  label,
}: {
  className?: string;
  label?: string;
}): ReactNode {
  return (
    <span
      className={`bd-fruit ${className}`}
      role={label ? 'img' : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
    >
      🍌
    </span>
  );
}

function Caption({ children }: { children: ReactNode }): ReactNode {
  return <p className="bd-caption">{children}</p>;
}

const INSTALL_COMMAND = "pip install 'databricks-labs-dqx[anomaly]'";
const BANANA_BURST = [
  [0, -38, -25],
  [27, -27, 50],
  [38, 0, 90],
  [27, 27, 135],
  [0, 38, 180],
  [-27, 27, -135],
  [-38, 0, -90],
  [-27, -27, -50],
] as const;

function BananaBurst({
  navigation = false,
  reverse = false,
}: {
  navigation?: boolean;
  reverse?: boolean;
}): ReactNode {
  return (
    <span
      className={`bd-copy-burst ${navigation ? 'bd-nav-burst' : ''} ${reverse ? 'bd-burst-reverse' : ''}`}
      aria-hidden="true"
    >
      {BANANA_BURST.map(([x, y, rotation], i) => (
        <span
          key={i}
          style={{
            transform: `translate(${x}px,${y}px) rotate(${rotation}deg)`,
            animationDelay: `${i * 12}ms`,
          }}
        >
          🍌
        </span>
      ))}
    </span>
  );
}

function BananaNavButton({
  className,
  label,
  disabled = false,
  reverse = false,
  onClick,
  children,
}: {
  className: string;
  label: string;
  disabled?: boolean;
  reverse?: boolean;
  onClick: () => void;
  children: ReactNode;
}): ReactNode {
  const [burst, setBurst] = useState(0);
  const reduced = useReducedMotion();
  useEffect(() => {
    if (burst === 0) return;
    const timer = window.setTimeout(() => setBurst(0), 850);
    return () => window.clearTimeout(timer);
  }, [burst]);
  return (
    <button
      type="button"
      className={className}
      aria-label={label}
      disabled={disabled}
      onClick={() => {
        setBurst((current) => current + 1);
        onClick();
      }}
    >
      {children}
      {burst > 0 && !reduced && (
        <BananaBurst key={burst} navigation reverse={reverse} />
      )}
    </button>
  );
}

function copyFromUserGesture(text: string): boolean {
  // Synchronous clipboard support also works in embedded documentation viewers.
  const field = document.createElement('textarea');
  const previousFocus = document.activeElement;
  field.value = text;
  field.readOnly = true;
  field.tabIndex = -1;
  field.style.position = 'fixed';
  field.style.left = '-9999px';
  document.body.appendChild(field);
  try {
    field.select();
    return document.execCommand('copy');
  } catch {
    return false;
  } finally {
    field.remove();
    if (previousFocus instanceof HTMLElement)
      previousFocus.focus({ preventScroll: true });
  }
}

function useReducedMotion(): boolean {
  const [reduced, setReduced] = useState(false);
  useEffect(() => {
    const preference = window.matchMedia('(prefers-reduced-motion: reduce)');
    const update = (): void => setReduced(preference.matches);
    update();
    preference.addEventListener('change', update);
    return () => preference.removeEventListener('change', update);
  }, []);
  return reduced;
}

function MotionToggle({
  paused,
  reduced,
  onToggle,
}: {
  paused: boolean;
  reduced: boolean;
  onToggle: () => void;
}): ReactNode {
  const label = reduced
    ? 'Animation disabled by reduced-motion setting'
    : paused
      ? 'Play animation'
      : 'Pause animation';
  return (
    <button
      type="button"
      className="bd-motion-control"
      aria-label={label}
      title={label}
      aria-pressed={paused || reduced}
      disabled={reduced}
      onClick={onToggle}
    >
      <svg
        width="17"
        height="17"
        viewBox="0 0 20 20"
        fill="currentColor"
        aria-hidden="true"
      >
        {paused || reduced ? (
          <path d="M6 3 17 10 6 17Z" />
        ) : (
          <>
            <rect x="5" y="3" width="3" height="14" rx="1" />
            <rect x="12" y="3" width="3" height="14" rx="1" />
          </>
        )}
      </svg>
    </button>
  );
}

const BANANA_CAPTIONS = [
  {
    text: 'No bananas were harmed. A few were flagged for review.',
    source: '',
  },
  {
    text: 'A banana plant is a giant herb, not a tree.',
    source: 'https://www.kew.org/plants/cavendish-banana',
  },
  {
    text: 'Botanically speaking, bananas are berries.',
    source: 'https://www.kew.org/plants/cavendish-banana',
  },
  {
    text: 'Cavendish bananas have three sets of chromosomes.',
    source: 'https://www.kew.org/read-and-watch/madagascan-banana',
  },
] as const;

function BananaCaption({ paused }: { paused: boolean }): ReactNode {
  const [caption, setCaption] = useState(0);
  useEffect(() => {
    if (paused) return;
    const timer = window.setInterval(
      () => setCaption((current) => (current + 1) % BANANA_CAPTIONS.length),
      6000,
    );
    return () => window.clearInterval(timer);
  }, [paused]);
  const current = BANANA_CAPTIONS[caption];
  return (
    <p className="bd-bananas-fine">
      {current.source ? (
        <a
          href={current.source}
          target="_blank"
          rel="noreferrer"
          title="Banana fact source: Kew"
        >
          {current.text}
        </a>
      ) : (
        current.text
      )}
    </p>
  );
}

function InstallCommand({ paused = false }: { paused?: boolean }): ReactNode {
  const [burst, setBurst] = useState(0);
  useEffect(() => {
    if (burst === 0) return;
    const timer = window.setTimeout(() => setBurst(0), 850);
    return () => window.clearTimeout(timer);
  }, [burst]);
  const [typed, setTyped] = useState(0);
  useEffect(() => {
    if (paused) {
      setTyped(INSTALL_COMMAND.length);
      return;
    }
    let count = 0;
    setTyped(0);
    const timer = window.setInterval(() => {
      count += 1;
      setTyped(count);
      if (count >= INSTALL_COMMAND.length) window.clearInterval(timer);
    }, 45);
    return () => window.clearInterval(timer);
  }, [paused]);
  const [copyState, setCopyState] = useState<'idle' | 'copied' | 'error'>(
    'idle',
  );
  const copy = async (): Promise<void> => {
    try {
      if (!copyFromUserGesture(INSTALL_COMMAND)) {
        await navigator.clipboard.writeText(INSTALL_COMMAND);
      }
      setCopyState('copied');
      setBurst((current) => current + 1);
    } catch {
      setCopyState('error');
    }
  };
  return (
    <div className="bd-install-wrap">
      <div className="bd-install-bar">
        <code>
          <span className="bd-terminal-prompt" aria-hidden="true">
            ${' '}
          </span>
          <span className="bd-command">
            <span className="bd-command-reserve" aria-hidden="true">
              {INSTALL_COMMAND}
            </span>
            <span className="bd-command-typing" aria-hidden="true">
              {INSTALL_COMMAND.slice(0, typed)}
              <span
                className={`bd-terminal-cursor ${paused ? 'bd-cursor-still' : ''}`}
              />
            </span>
            <span className="bd-sr-only">{INSTALL_COMMAND}</span>
          </span>
        </code>
        <button
          type="button"
          className="bd-copy"
          onClick={() => {
            void copy();
          }}
          aria-label={
            copyState === 'copied'
              ? 'Installation command copied'
              : 'Copy installation command'
          }
          title="Copy command"
        >
          <svg
            viewBox="0 0 24 24"
            width="19"
            height="19"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.7"
            aria-hidden="true"
          >
            {copyState === 'copied' ? (
              <path d="m5 12 4 4L19 6" />
            ) : (
              <>
                <rect x="8" y="8" width="12" height="12" rx="2" />
                <path d="M15 5V4a1 1 0 0 0-1-1H4a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h1" />
              </>
            )}
          </svg>
        </button>
      </div>
      {burst > 0 && !paused && <BananaBurst key={burst} />}
      <span
        className={copyState === 'error' ? 'bd-copy-error' : 'bd-sr-only'}
        role="status"
      >
        {copyState === 'copied'
          ? 'Copied to clipboard.'
          : copyState === 'error'
            ? 'Copy unavailable. Select the command and copy it manually.'
            : ''}
      </span>
    </div>
  );
}

function PairChart({
  reveal,
  mini = false,
}: {
  reveal: boolean;
  mini?: boolean;
}): ReactNode {
  const points = [
    [15, 80],
    [16, 90],
    [17, 93],
    [18, 104],
    [19, 109],
    [20, 114],
    [21, 125],
    [22, 128],
    [23, 137],
    [24, 143],
  ];
  const x = (n: number) => 55 + (n - 14) * 30;
  const y = (n: number) => 265 - (n - 60) * 2.15;
  return (
    <svg
      className="bd-chart"
      viewBox="0 0 420 310"
      role="img"
      aria-label={
        reveal
          ? 'Historical length and weight rise together. New row: length 22 cm, weight 94 g, below that relationship but inside each range.'
          : 'Historical banana length and weight rise together.'
      }
    >
      {[80, 110, 140].map((v) => (
        <g key={v}>
          <line x1="48" y1={y(v)} x2="375" y2={y(v)} className="bd-grid" />
          <text x="38" y={y(v) + 5} textAnchor="end">
            {v}
          </text>
        </g>
      ))}
      <line x1="48" y1="265" x2="375" y2="265" className="bd-axis" />
      {[15, 18, 21, 24].map((v) => (
        <text key={v} x={x(v)} y="286" textAnchor="middle">
          {v}
        </text>
      ))}
      {points.map(([a, b]) => (
        <circle
          key={a}
          cx={x(a)}
          cy={y(b)}
          r={mini ? 7 : 8}
          className="bd-history"
        />
      ))}
      {reveal && (
        <>
          <line
            x1={x(22)}
            x2={x(22)}
            y1={y(128)}
            y2={y(94)}
            className="bd-departure"
          />
          <circle cx={x(22)} cy={y(94)} r="11" className="bd-alert-point" />
          <text
            x={x(22) - 17}
            y={y(94) + 28}
            textAnchor="end"
            className="bd-alert-label"
          >
            New row #42
          </text>
        </>
      )}
      <text x="57" y="23">
        Weight (g)
      </text>
      <text x="370" y="308" textAnchor="end">
        Length (cm)
      </text>
    </svg>
  );
}

const EXPLANATION_EXAMPLE = {
  groupSize: 12,
  meanSeverity: 99.2,
  contributions: [
    { column: 'weight', label: 'Weight', share: 65 },
    { column: 'length', label: 'Length', share: 30 },
    { column: 'ripeness', label: 'Ripeness', share: 5 },
  ],
} as const;

export function ExplanationExample({
  redacted,
  onToggle,
}: {
  redacted: boolean;
  onToggle: () => void;
}): ReactNode {
  const visible = EXPLANATION_EXAMPLE.contributions.filter(
    (item) => !redacted || item.column !== 'weight',
  );
  const total = visible.reduce((sum, item) => sum + item.share, 0);
  const drivers = visible
    .slice(0, 2)
    .map(
      (item) => `${item.column} (${Math.round((item.share / total) * 100)}%)`,
    )
    .join(' and ');
  return (
    <>
      <div className="bd-explanation">
        <div>
          <span className="bd-small-label">
            {EXPLANATION_EXAMPLE.groupSize} example flagged rows
          </span>
          <h3>Average contributions</h3>
          {EXPLANATION_EXAMPLE.contributions.map(({ column, label, share }) => (
            <div className="bd-contribution" key={column}>
              <div>
                <span>{label}</span>
                <strong>{share}%</strong>
              </div>
              <div className="bd-bar-track">
                <div className="bd-bar" style={{ width: `${share}%` }} />
              </div>
            </div>
          ))}
          <p className="bd-example-severity">
            Average severity{' '}
            <strong>{EXPLANATION_EXAMPLE.meanSeverity} / 100</strong>
            <small>A severity percentile, not a probability of error.</small>
          </p>
        </div>
        <aside>
          <h3>
            <span aria-hidden="true">🔎</span> Illustrative AI explanation
          </h3>
          <div className="bd-ai-output" aria-live="polite" aria-atomic="true">
            <p>
              Across {EXPLANATION_EXAMPLE.groupSize} rows, the{' '}
              {redacted ? 'disclosed' : 'leading'} contributions are {drivers}.
            </p>
            {redacted && (
              <p className="bd-evidence-scope">
                Most of the contributing evidence could not be disclosed, so
                this explanation covers a minority of what the model used.
              </p>
            )}
            <dl>
              <div>
                <dt>Potential impact</dt>
                <dd>
                  {redacted
                    ? 'If these rows are wrong, reports using them could be affected.'
                    : 'If weight is wrong, shipment-weight totals could be affected.'}
                </dd>
              </div>
              <div>
                <dt>Suggested action</dt>
                <dd>
                  {redacted
                    ? 'Compare these rows with source records. This explanation has limited evidence.'
                    : 'Compare weight and length with the source measurements.'}
                </dd>
              </div>
            </dl>
          </div>
          <button
            className="bd-action"
            type="button"
            aria-pressed={redacted}
            onClick={onToggle}
          >
            {redacted ? 'Remove redaction' : 'Redact weight from AI'}
          </button>
        </aside>
      </div>
      <Caption>
        Redaction limits what AI sees. Your scores and row contributions stay
        unchanged.
      </Caption>
    </>
  );
}

export function BananaSlide({ index }: { index: number }): ReactNode {
  const [paused, setPaused] = useState(false);
  const reduced = useReducedMotion();
  const stopped = paused || reduced;
  const controlId = useId();
  const [revealed, setRevealed] = useState(false);
  const [variety, setVariety] = useState('Cavendish');
  const [timeContext, setTimeContext] = useState(false);
  const [threshold, setThreshold] = useState('95');
  const [redacted, setRedacted] = useState(false);
  switch (index) {
    case 0:
      return (
        <div className={`bd-cover ${stopped ? 'bd-cover-paused' : ''}`}>
          <div className="bd-animation-box">
            <div
              className={`bd-conveyor ${stopped ? 'bd-conveyor-paused' : ''}`}
              role="img"
              aria-label="A moving line of bananas passes explicit rules. Green banana number 42 also passes the rules, but gets an additional red anomaly review sticker."
            >
              <div className="bd-conveyor-track" aria-hidden="true">
                {[0, 1].map((copy) => (
                  <div className="bd-conveyor-bunch" key={copy}>
                    {[0, 1, 2, 3, 4, 5].map((item) => (
                      <div className="bd-conveyor-item" key={item}>
                        <Fruit className={item === 1 ? 'bd-green' : ''} />
                        <span className="bd-ok-sticker">
                          <small>RULES</small>✓ OK
                        </span>
                        {item === 1 && (
                          <span className="bd-anomaly-sticker">
                            <small>ANOMALY</small>#42 · REVIEW
                          </span>
                        )}
                      </div>
                    ))}
                  </div>
                ))}
              </div>
              <div className="bd-conveyor-belt" aria-hidden="true" />
            </div>
            <MotionToggle
              paused={paused}
              reduced={reduced}
              onToggle={() => setPaused(!paused)}
            />
          </div>
          <div className="bd-cover-copy">
            <div>
              <span className="bd-handwritten">
                Rules passed. One deserves a second look.
              </span>
              <p>Find unusual rows, even when simple rules miss them.</p>
            </div>
          </div>
          <InstallCommand paused={stopped} />
          <span className="bd-readtime">
            13 short slides · Explore at your own pace
          </span>
        </div>
      );
    case 1:
      return (
        <>
          <div className="bd-delivery">
            <div className="bd-delivery-panel">
              <div className="bd-delivery-art">
                <span className="bd-scene-emoji" aria-hidden="true">
                  🚚
                </span>
              </div>
              <h3>Databricks DQM</h3>
              <p>
                Is the table fresh?
                <br />
                Did the expected data arrive?
              </p>
            </div>
            <div className="bd-divider" />
            <div className="bd-delivery-panel">
              <div className="bd-delivery-art bd-fruit-line">
                <Fruit />
                <Fruit />
                <Fruit className="bd-green" />
              </div>
              <h3>DQX</h3>
              <p>
                Does this row meet our rules?
                <br />
                Does it look unusual?
              </p>
            </div>
          </div>
          <Caption>
            A healthy delivery can still contain an odd banana. Use both layers.
          </Caption>
        </>
      );
    case 2:
      return (
        <div className="bd-rule-scene">
          <Fruit label="A banana being checked" />
          <div className="bd-checklist">
            <div>
              <span>01</span>
              <p>
                Weight is present<code>is_not_null</code>
              </p>
            </div>
            <div>
              <span>02</span>
              <p>
                Length is within an agreed range<code>is_in_range</code>
              </p>
            </div>
            <div>
              <span>03</span>
              <p>
                Variety is on the approved list<code>is_in_list</code>
              </p>
            </div>
          </div>
          <Caption>Known requirement? A rule is a good first tool.</Caption>
        </div>
      );
    case 3:
      return (
        <>
          <div className="bd-learning">
            <section>
              <div className="bd-bunch">
                <Fruit />
                <Fruit />
                <Fruit />
              </div>
              <h3>Historical training data</h3>
              <p>
                The varieties and conditions
                <br />
                you expect to see.
              </p>
            </section>
            <span className="bd-step-number">
              1<br />
              <small>Train</small>
            </span>
            <section className="bd-model">
              <span aria-hidden="true">🧠</span>
              <h3>A saved model</h3>
              <p>
                Its learned comparison
                <br />
                stays fixed until retraining.
              </p>
            </section>
            <span className="bd-step-number">
              2<br />
              <small>Check</small>
            </span>
            <section>
              <Fruit />
              <h3>New rows</h3>
              <p>
                A score and a decision
                <br />
                for each supported row.
              </p>
            </section>
          </div>
          <Caption>
            Keep a separate data slice to see whether the findings are useful.
          </Caption>
        </>
      );
    case 4:
      return (
        <div className="bd-profile-pair">
          <section>
            <code>profile="tabular"</code>
            <div className="bd-profile-fruit">
              <Fruit />
              <Fruit />
              <Fruit className="bd-green" />
            </div>
            <h3>Unusual rows</h3>
            <p>General-purpose detection of unusual values and combinations.</p>
            <span className="bd-small-label">Default starting point</span>
          </section>
          <section>
            <code>profile="correlation"</code>
            <PairChart reveal mini />
            <h3>Unusual relationships</h3>
            <p>
              Numeric fields that usually agree depart from their learned
              relationship.
            </p>
            <span className="bd-small-label">No timestamp required</span>
          </section>
        </div>
      );
    case 5:
      return (
        <>
          <div className="bd-relationship">
            <PairChart reveal={revealed} />
            <div className="bd-reading">
              <Fruit />
              <dl>
                <div>
                  <dt>Length</dt>
                  <dd>
                    22 <small>cm</small>
                  </dd>
                </div>
                <div>
                  <dt>Weight</dt>
                  <dd>
                    94 <small>g</small>
                  </dd>
                </div>
              </dl>
              <button
                type="button"
                className="bd-action"
                aria-pressed={revealed}
                onClick={() => setRevealed(!revealed)}
              >
                {revealed ? 'Hide the new row' : 'Compare with the bunch'}
              </button>
              <p className="bd-feedback" aria-live="polite">
                {revealed
                  ? 'The pair stands out. Check the measurement before deciding why.'
                  : 'Both values fit the historical ranges. What about the pair?'}
              </p>
            </div>
          </div>
          <Caption>
            Illustrative measurements. A relationship departure is a clue, not a
            diagnosis.
          </Caption>
        </>
      );
    case 6:
      return (
        <>
          <div className="bd-group-scene">
            <div>
              <Fruit label="An 18 centimetre banana" />
              <strong className="bd-measure">18 cm</strong>
              <span>The same arriving row</span>
            </div>
            <div>
              <label htmlFor={`${controlId}-variety`}>
                Compare with its variety
              </label>
              <select
                id={`${controlId}-variety`}
                value={variety}
                onChange={(e) => setVariety(e.target.value)}
              >
                <option>Cavendish</option>
                <option>Lady finger</option>
                <option>New variety</option>
              </select>
              <div className="bd-group-result" aria-live="polite">
                <h3>
                  {variety === 'Cavendish'
                    ? 'Fits this bunch'
                    : variety === 'Lady finger'
                      ? 'Worth a closer look'
                      : 'No learned baseline yet'}
                </h3>
                <p>
                  {variety === 'Cavendish'
                    ? 'Illustrative usual range: 16–22 cm'
                    : variety === 'Lady finger'
                      ? 'Illustrative usual range: 10–14 cm'
                      : 'The model cannot judge an unseen group.'}
                </p>
              </div>
              <code>baseline_by=["variety"]</code>
            </div>
          </div>
          <Caption>
            Group context uses history for that group. It does not refresh the
            baseline automatically.
          </Caption>
        </>
      );
    case 7:
      return (
        <>
          <div className="bd-time-scene">
            <div
              className="bd-week"
              role="img"
              aria-label="Illustrative packing volume rises on Friday. The learned Friday expectation explains the higher volume."
            >
              {[32, 45, 38, 50, 95, 28, 22].map((v, i) => (
                <div key={i} className={i === 4 ? 'bd-friday' : ''}>
                  <div
                    className="bd-volume"
                    style={{ height: `${v * 1.6}px` }}
                  />
                  <span>
                    {['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][i]}
                  </span>
                </div>
              ))}
            </div>
            <div className="bd-time-copy">
              <h3>Friday is a big packing day</h3>
              <p aria-live="polite">
                {timeContext
                  ? 'A learned weekly pattern gives Friday its own expected level.'
                  : 'A high value needs context before you call it a problem.'}
              </p>
              <button
                className="bd-action"
                type="button"
                aria-pressed={timeContext}
                onClick={() => setTimeContext(!timeContext)}
              >
                {timeContext ? 'Hide time context' : 'Add time context'}
              </button>
            </div>
          </div>
          <code className="bd-code-line">baseline_over_time="packed_at"</code>
          <Caption>
            The model needs enough history to learn the pattern. Future patterns
            can change.
          </Caption>
        </>
      );
    case 8: {
      const scores = [42, 65, 80, 91, 95.5, 98.2, 99.4, 99.8];
      const count = scores.filter((v) => v >= Number(threshold)).length;
      return (
        <>
          <div className="bd-threshold-control">
            <label htmlFor={`${controlId}-threshold`}>threshold</label>
            <select
              id={`${controlId}-threshold`}
              value={threshold}
              onChange={(e) => setThreshold(e.target.value)}
            >
              {['95', '99', '99.5', '99.9'].map((v) => (
                <option key={v}>{v}</option>
              ))}
            </select>
            <span aria-live="polite">
              <strong>{count}</strong> of 8 example rows flagged
            </span>
          </div>
          <div className="bd-score-row">
            {scores.map((v) => (
              <div
                key={v}
                className={
                  v >= Number(threshold)
                    ? 'bd-score bd-score-flagged'
                    : 'bd-score'
                }
              >
                <Fruit />
                <strong>{v}</strong>
                <span>
                  {v >= Number(threshold) ? 'Review' : 'Below cutoff'}
                </span>
              </div>
            ))}
          </div>
          <Caption>
            Illustrative severity scores. 95 does not mean a 95% chance of error
            or exactly 5% future alerts.
          </Caption>
        </>
      );
    }
    case 9:
      return (
        <ExplanationExample
          redacted={redacted}
          onToggle={() => setRedacted(!redacted)}
        />
      );
    case 10:
      return (
        <div className="bd-outcomes">
          <section>
            <span className="bd-status">!</span>
            <h3>Flagged</h3>
            <p>The row crossed the chosen boundary.</p>
            <strong>Investigate</strong>
          </section>
          <section>
            <span className="bd-status">✓</span>
            <h3>Not flagged</h3>
            <p>The model scored it below the boundary.</p>
            <strong>Keep other DQ checks</strong>
          </section>
          <section>
            <span className="bd-status">?</span>
            <h3>Not scored</h3>
            <p>For example, its group is new to the model.</p>
            <strong>Check coverage</strong>
          </section>
        </div>
      );
    case 11:
      return (
        <>
          <div className="bd-maintain">
            <div>
              <span aria-hidden="true">📦</span>
              <h3>Yesterday’s supplier</h3>
              <p>The history your model learned.</p>
            </div>
            <div>
              <Fruit className="bd-green" />
              <h3>Today’s change</h3>
              <p>New normal, or a real issue?</p>
            </div>
          </div>
          <div className="bd-maintenance-note">
            <strong>You make that call.</strong>
            <span>
              Review the change before retraining on an approved data window.
            </span>
          </div>
          <Caption>
            Neither baseline option retrains automatically. A surge in alerts
            needs investigation.
          </Caption>
        </>
      );
    case 12:
      return (
        <div className="bd-finish bd-end-card">
          <MotionToggle
            paused={paused}
            reduced={reduced}
            onToggle={() => setPaused(!paused)}
          />
          <Fruit />
          <InstallCommand paused={stopped} />
          <BananaCaption paused={stopped} />
        </div>
      );
    default:
      return null;
  }
}

export default function BananaDeck(): ReactNode {
  const [index, setIndex] = useState(0);
  const [showNotes, setShowNotes] = useState(false);
  const id = useId();
  const go = (next: number): void => {
    setIndex(Math.max(0, Math.min(slides.length - 1, next)));
  };
  const keyDown = (event: KeyboardEvent<HTMLElement>): void => {
    if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey)
      return;
    const target = event.target;
    if (
      target instanceof HTMLElement &&
      target.closest('button,select,input,textarea,a,[contenteditable="true"]')
    )
      return;
    if (event.key === 'ArrowRight') {
      event.preventDefault();
      go(index + 1);
    }
    if (event.key === 'ArrowLeft') {
      event.preventDefault();
      go(index - 1);
    }
    if (event.key === 'Home') {
      event.preventDefault();
      go(0);
    }
    if (event.key === 'End') {
      event.preventDefault();
      go(slides.length - 1);
    }
  };
  return (
    <section
      className="banana-story"
      aria-label="The odd banana: an interactive guide to DQX"
      tabIndex={0}
      onKeyDown={keyDown}
    >
      <div className="bd-topline">
        <span>THE ODD BANANA</span>
        <span aria-live="polite" aria-atomic="true">
          {String(index + 1).padStart(2, '0')} / {slides.length}
        </span>
      </div>
      <article
        key={index}
        className={`bd-slide bd-slide-${index}`}
        aria-labelledby={`${id}-title`}
      >
        <header>
          <h2 id={`${id}-title`}>{slides[index][0]}</h2>
          <p className="bd-lead">{slides[index][1]}</p>
        </header>
        <div className="bd-visual" key={index}>
          <BananaSlide index={index} />
        </div>
      </article>
      <footer className="bd-navigation">
        <BananaNavButton
          className="bd-back"
          reverse
          onClick={() => go(index - 1)}
          disabled={index === 0}
          label="Previous slide"
        >
          Back
        </BananaNavButton>
        <label className="bd-jump">
          Slide
          <select
            aria-label="Choose slide"
            value={index}
            onChange={(e) => go(Number(e.target.value))}
          >
            {slides.map(([title], i) => (
              <option value={i} key={title}>
                {i + 1}. {title}
              </option>
            ))}
          </select>
        </label>
        <BananaNavButton
          className="bd-next"
          onClick={() => go(index === slides.length - 1 ? 0 : index + 1)}
          label={index === slides.length - 1 ? 'Restart deck' : 'Next slide'}
        >
          {index === slides.length - 1 ? 'Start again' : 'Next'}{' '}
          <span aria-hidden="true">↗</span>
        </BananaNavButton>
      </footer>
      <div
        className="bd-progress"
        role="progressbar"
        aria-label="Slide progress"
        aria-valuemin={1}
        aria-valuemax={slides.length}
        aria-valuenow={index + 1}
      >
        <span style={{ width: `${((index + 1) / slides.length) * 100}%` }} />
      </div>
      <div className="bd-detail">
        <button
          type="button"
          aria-expanded={showNotes}
          aria-controls={`${id}-notes`}
          onClick={() => setShowNotes(!showNotes)}
        >
          {showNotes ? '− Hide' : '＋ Read'} the DQX detail
        </button>
        <span>Focus this deck to use ← / →</span>
      </div>
      <p className="bd-notes" id={`${id}-notes`} hidden={!showNotes}>
        {notes[index]}
        {index === 1 && (
          <>
            {' '}
            <a
              href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-quality-monitoring/anomaly-detection/"
              target="_blank"
              rel="noreferrer"
            >
              Databricks DQM documentation
            </a>
          </>
        )}
      </p>
    </section>
  );
}
