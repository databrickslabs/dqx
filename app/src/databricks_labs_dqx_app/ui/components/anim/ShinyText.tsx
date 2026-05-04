import { cn } from "@/lib/utils";

interface ShinyTextProps {
  text: string;
  disabled?: boolean;
  speed?: number;
  className?: string;
}

export function ShinyText({
  text,
  disabled = false,
  speed = 5,
  className,
}: ShinyTextProps) {
  const animationDuration = `${speed}s`;

  return (
    <div
      className={cn(
        "text-foreground bg-clip-text inline-block",
        !disabled &&
          "animate-shine bg-[linear-gradient(110deg,#000000,45%,#555555,55%,#000000)] dark:bg-[linear-gradient(110deg,#ffffff,45%,#aaaaaa,55%,#ffffff)] bg-[length:200%_100%] text-transparent",
        className,
      )}
      style={{
        animationDuration: animationDuration,
      }}
    >
      {text}
    </div>
  );
}
