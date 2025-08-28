import logoUrl from "/logo.svg";

export default function Logo({ className }: { className?: string }) {
  return <img src={logoUrl} alt="Logo" className={className} />;
}
