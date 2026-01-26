import { ModeToggle } from "@/components/apx/mode-toggle";
import Logo from "@/components/apx/Logo";
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
        {rightContent || <ModeToggle />}
      </div>
    </header>
  );
}

export default Navbar;
