import { useEffect, useRef, useState } from "react";
import { ChevronDown } from "lucide-react";
import { cn } from "../lib/utils";

const menuBtnClass =
  "inline-flex items-center justify-between gap-2 min-w-[8.5rem] rounded-lg border border-zinc-700 bg-zinc-800 px-3 py-1.5 text-left text-sm text-zinc-300 hover:border-zinc-600 hover:bg-zinc-800/90 focus:outline-none focus:border-sky-600";

/**
 * Custom listbox-style control (matches Flights sort/year pickers) — avoids native select OS chrome.
 */
export function MenuSelect<T extends string>({
  value,
  options,
  onChange,
  ariaLabel,
  className,
}: {
  value: T;
  options: { value: T; label: string }[];
  onChange: (v: T) => void;
  ariaLabel?: string;
  className?: string;
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  const current = options.find((o) => o.value === value)?.label ?? value;

  return (
    <div ref={rootRef} className={cn("relative", className)}>
      <button
        type="button"
        className={menuBtnClass}
        aria-label={ariaLabel}
        aria-expanded={open}
        aria-haspopup="listbox"
        onClick={() => setOpen((o) => !o)}
      >
        <span className="truncate">{current}</span>
        <ChevronDown className={cn("h-4 w-4 shrink-0 text-zinc-500 transition-transform", open && "rotate-180")} />
      </button>
      {open ? (
        <ul
          className="absolute left-0 top-full z-50 mt-1 max-h-60 min-w-full overflow-y-auto rounded-lg border border-zinc-700 bg-zinc-900 py-1 shadow-xl"
          role="listbox"
        >
          {options.map((o) => (
            <li key={String(o.value)} role="none">
              <button
                type="button"
                role="option"
                aria-selected={o.value === value}
                className={cn(
                  "w-full px-3 py-2 text-left text-sm text-zinc-300 hover:bg-zinc-800",
                  o.value === value && "bg-zinc-800/80 text-sky-400"
                )}
                onClick={() => {
                  onChange(o.value);
                  setOpen(false);
                }}
              >
                {o.label}
              </button>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
