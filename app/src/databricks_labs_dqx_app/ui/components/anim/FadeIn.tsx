import { motion } from "motion/react";
import { ReactNode, useRef } from "react";

interface FadeInProps {
  children: ReactNode;
  delay?: number;
  duration?: number;
  className?: string;
}

export function FadeIn({
  children,
  delay = 0,
  duration = 0.5,
  className,
}: FadeInProps) {
  const ref = useRef<HTMLDivElement>(null);
  return (
    <motion.div
      ref={ref}
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration, delay, ease: "easeOut" }}
      onAnimationComplete={() => {
        // Once the entrance settles, strip the lingering transform/will-change
        // motion leaves behind. Otherwise the element stays on a GPU-composited
        // layer that rasterizes its children — SVG icons in particular render
        // soft/"blurry" (text is re-rasterized crisp, SVGs are not). Clearing
        // it drops the layer so the content is pixel-crisp at rest (B2-111).
        const el = ref.current;
        if (el) {
          el.style.transform = "none";
          el.style.willChange = "auto";
        }
      }}
      className={className}
    >
      {children}
    </motion.div>
  );
}
