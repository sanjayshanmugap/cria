import type { SelectHTMLAttributes } from "react";

import { cn } from "@/lib/utils";

export function Select({
  className,
  children,
  ...props
}: SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <select
      className={cn(
        "min-h-11 w-full rounded-xl border border-[hsl(var(--input))] bg-white px-3 py-2 text-sm text-[hsl(var(--foreground))] transition-colors duration-200 ease-out hover:border-[hsl(var(--muted-foreground)/0.45)] disabled:cursor-not-allowed disabled:bg-[hsl(var(--muted))] disabled:opacity-70",
        className,
      )}
      {...props}
    >
      {children}
    </select>
  );
}
