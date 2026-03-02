import React, { useState, useEffect, useRef } from 'react';
import SlideVisuals, { type VisualKey } from './SlideVisuals';

interface DeckProps {
  children: React.ReactNode;
}

interface SlideProps {
  title: string;
  visual?: VisualKey;
  children?: React.ReactNode;
}

export function Slide(_props: SlideProps) {
  return null;
}

export default function Deck({ children }: DeckProps) {
  const slideEls = React.Children.toArray(children).filter(
    (c): c is React.ReactElement<SlideProps> =>
      React.isValidElement(c) && typeof (c as React.ReactElement<SlideProps>).props?.title === 'string'
  );

  const [slideIndex, setSlideIndex] = useState(0);
  const totalSlides = slideEls.length;
  const current = slideEls[slideIndex];
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

  const slideRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = slideRef.current;
    if (!el) return;

    let raf = 0;
    let cancelled = false;

    const stopAutoScroll = () => { cancelled = true; cancelAnimationFrame(raf); };
    el.addEventListener('wheel', stopAutoScroll, { passive: true, once: true });
    el.addEventListener('touchstart', stopAutoScroll, { passive: true, once: true });

    const timer = setTimeout(() => {
      const overflow = el.scrollHeight - el.clientHeight;
      if (overflow <= 0 || cancelled) return;

      const duration = Math.max(4000, overflow * 14);
      const start = performance.now();
      const ease = (t: number) => t < 0.5 ? 2 * t * t : -1 + (4 - 2 * t) * t;

      const step = (now: number) => {
        if (cancelled) return;
        const t = Math.min((now - start) / duration, 1);
        el.scrollTop = overflow * ease(t);
        if (t < 1) raf = requestAnimationFrame(step);
      };
      raf = requestAnimationFrame(step);
    }, 1500);

    return () => {
      clearTimeout(timer);
      cancelAnimationFrame(raf);
      el.removeEventListener('wheel', stopAutoScroll);
      el.removeEventListener('touchstart', stopAutoScroll);
    };
  }, [slideIndex]);

  if (totalSlides === 0) return null;

  const { title, visual, children: slideChildren } = (current?.props || {}) as SlideProps;

  return (
    <div className="banana-deck margin-vert--lg">
      <div
        className="banana-deck__card"
        role="region"
        aria-label={`Slide ${slideIndex + 1} of ${totalSlides}: ${title}`}
      >
        <div key={slideIndex} ref={slideRef} className="banana-deck__slide banana-deck__slide-enter">
          {title && <h3 className="banana-deck__title">{title}</h3>}
          {slideChildren && <div className="banana-deck__body">{slideChildren}</div>}
          {visual && <SlideVisuals visual={visual} />}
        </div>
      </div>

      <footer className="banana-deck__footer">
        <div className="banana-deck__nav">
          <button type="button" onClick={goPrev} disabled={isFirst} className="banana-deck__btn banana-deck__btn--prev" aria-label="Previous slide">Back</button>
          <button type="button" onClick={goNext} disabled={isLast} className="banana-deck__btn banana-deck__btn--next" aria-label="Next slide">Next</button>
        </div>
        <div className="banana-deck__progress" role="tablist" aria-label="Slide progress">
          <span className="banana-deck__counter">{slideIndex + 1} / {totalSlides}</span>
          <div className="banana-deck__dots">
            {slideEls.map((_, i) => (
              <button
                key={i} type="button" onClick={() => setSlideIndex(i)}
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
