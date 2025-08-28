import { motion } from "framer-motion";
import { useEffect, useState } from "react";

export default function FadeInOnce({
  children,
  duration = 0.1,
}: {
  children: React.ReactNode;
  duration?: number;
}) {
  const [animated, setAnimated] = useState(false);

  useEffect(() => {
    setAnimated(true);
  }, []);

  return (
    <motion.div
      initial={!animated ? { opacity: 0, y: 10 } : false}
      animate={!animated ? { opacity: 1, y: 0 } : false}
      transition={{ duration, ease: "easeIn" }}
    >
      {children}
    </motion.div>
  );
}
