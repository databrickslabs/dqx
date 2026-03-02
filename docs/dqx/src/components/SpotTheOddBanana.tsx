import React, { useState, useEffect } from 'react';

type VisualKey =
  | 'title'
  | 'known-unknowns'
  | 'dqx-rules'
  | 'trucks-dqm'
  | 'gap'
  | 'row-level'
  | 'features'
  | 'isolation-forest'
  | 'shap'
  | 'summary'
  | 'coming-soon';

interface Slide {
  id: number;
  title: string;
  body: string;
  visual: VisualKey;
}

const SLIDES: Slide[] = [
  { id: 1, title: 'Spot the odd banana.', body: 'Anomaly detection in data quality — in a few minutes.', visual: 'title' },
  { id: 2, title: 'Known unknowns vs unknown unknowns.', body: 'Rules handle what we know (count, size range). We know we don\'t know everything (e.g. acceptable size). The surprises — what we don\'t know we don\'t know — are where anomaly detection helps.', visual: 'known-unknowns' },
  { id: 3, title: 'DQX handles rules really well.', body: 'One banana: we check size, colour, flavour. Define rules in code — clear, testable, versioned.', visual: 'dqx-rules' },
  { id: 4, title: 'DQM built-in anomaly detection', body: 'Did trucks come? Empty? Enough crates? Table-level = freshness, row counts.', visual: 'trucks-dqm' },
  { id: 5, title: 'But who checks the bananas?', body: 'DQM doesn\'t look inside the crate. Table-level vs row-level — we have a gap.', visual: 'gap' },
  { id: 6, title: 'Introducing DQX ML-based outlier detection', body: 'Checks every banana at scale! We learn "normal" from good data and flag rows that don\'t fit. No labels = unsupervised. "Is this banana unusual?"', visual: 'row-level' },
  { id: 7, title: 'Feature engineering & Isolation Forest', body: 'Banana → features (color, size, spots, bend). Isolation Forest finds anomalies by how quickly they can be isolated through random feature splits. Unusual items require fewer splits.', visual: 'features' },
  { id: 8, title: 'How do we find the odd banana? Isolation Forest.', body: '"How many random splits to isolate this point?" Anomaly = few splits (alone). Normal = many splits (in the crowd).', visual: 'isolation-forest' },
  { id: 9, title: 'Why was this banana flagged? SHAP tells you.', body: 'Score alone doesn\'t explain. SHAP shows which features drove it (e.g. too brown, wrong size). "Here\'s why."', visual: 'shap' },
  { id: 10, title: 'DQX anomaly detection', body: 'Key capabilities that make it easy to adopt. Try DQX anomaly detection.', visual: 'summary' },
  { id: 11, title: 'Coming soon!', body: 'More banana-powered data quality. That\'s all, folks!', visual: 'coming-soon' },
];

const VISUAL_EMOJI: Record<VisualKey, string> = {
  'title': '🍌',
  'known-unknowns': '❓',
  'dqx-rules': '✅',
  'trucks-dqm': '🚛',
  'gap': '📦',
  'row-level': '🔍',
  'features': '📐',
  'isolation-forest': '🌲',
  'shap': '📊',
  'summary': '🎯',
  'coming-soon': '🍌',
};

export default function SpotTheOddBanana() {
  const [slideIndex, setSlideIndex] = useState(0);
  const totalSlides = SLIDES.length;
  const slide = SLIDES[slideIndex];
  const isLast = slideIndex === totalSlides - 1;
  const isFirst = slideIndex === 0;

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'ArrowRight' || e.key === ' ') {
        e.preventDefault();
        setSlideIndex((s) => Math.min(s + 1, totalSlides - 1));
      } else if (e.key === 'ArrowLeft') {
        e.preventDefault();
        setSlideIndex((s) => Math.max(s - 1, 0));
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [totalSlides]);

  const goNext = () => setSlideIndex((s) => Math.min(s + 1, totalSlides - 1));
  const goPrev = () => setSlideIndex((s) => Math.max(s - 1, 0));

  return (
    <div className="banana-deck margin-vert--lg">
      <div
        className="banana-deck__card"
        role="region"
        aria-label={`Slide ${slideIndex + 1} of ${totalSlides}: ${slide.title}`}
      >
        <div className="banana-deck__slide">
          {slide.visual !== 'coming-soon' && (
            <>
              <h3 className="banana-deck__title">{slide.title}</h3>
              <p className="banana-deck__body">{slide.body}</p>
            </>
          )}
          <div className="banana-deck__visual" aria-hidden>
            {slide.visual === 'coming-soon' ? (
              <p className="banana-deck__body">{slide.body}</p>
            ) : (
              <span className="banana-deck__emoji">{VISUAL_EMOJI[slide.visual]}</span>
            )}
          </div>
        </div>
      </div>

      <footer className="banana-deck__footer">
        <div className="banana-deck__nav">
          <button
            type="button"
            onClick={goPrev}
            disabled={isFirst}
            className="banana-deck__btn banana-deck__btn--prev"
            aria-label="Previous slide"
          >
            Back
          </button>
          <button
            type="button"
            onClick={goNext}
            disabled={isLast}
            className="banana-deck__btn banana-deck__btn--next"
            aria-label="Next slide"
          >
            Next
          </button>
        </div>
        <div className="banana-deck__progress" role="tablist" aria-label="Slide progress">
          <span className="banana-deck__counter">
            {slideIndex + 1} / {totalSlides}
          </span>
          <div className="banana-deck__dots">
            {SLIDES.map((_, i) => (
              <button
                key={i}
                type="button"
                onClick={() => setSlideIndex(i)}
                className={`banana-deck__dot ${i === slideIndex ? 'banana-deck__dot--current' : ''}`}
                aria-label={`Go to slide ${i + 1}`}
                aria-current={i === slideIndex ? true : undefined}
              />
            ))}
          </div>
        </div>
      </footer>
      <p className="banana-deck__hint">Use ← → arrow keys or Space to move between slides.</p>
    </div>
  );
}
