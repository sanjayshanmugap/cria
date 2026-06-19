import type { TextareaHTMLAttributes } from "react";

import { cn } from "@/lib/utils";

export function Textarea({
  className,
  ...props
}: TextareaHTMLAttributes<HTMLTextAreaElement>) {
  return (
    <textarea
      className={cn(
        "min-h-40 w-full resize-y rounded-2xl border border-[hsl(var(--input))] bg-white px-4 py-3 text-base leading-7 text-[hsl(var(--foreground))] shadow-inner transition-colors duration-200 ease-out placeholder:text-[hsl(var(--muted-foreground))] hover:border-[hsl(var(--muted-foreground)/0.45)] disabled:cursor-not-allowed disabled:bg-[hsl(var(--muted))] disabled:opacity-70",
        className,
      )}
      {...props}
    />
  );
}
