import { ModeToggle } from "@/components/layout/mode-toggle";
import Logo from "@/components/layout/Logo";
import { ReactNode } from "react";

interface NavbarProps {
  leftContent?: ReactNode;
  rightContent?: ReactNode;
}

function Navbar({ leftContent, rightContent }: NavbarProps) {
  return (
    <header className="z-50 bg-background/80 backdrop-blur-sm border-b">
      <div className="h-16 flex items-center justify-between px-4">
        {leftContent || <Logo />}
        <div className="flex-1" />
        <div className="flex items-center gap-2">
          {rightContent || <ModeToggle />}
        </div>
      </div>
    </header>
  );
}

export default Navbar;
