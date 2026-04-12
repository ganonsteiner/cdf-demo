import { useEffect, useRef } from "react";
import { MessageSquare, X } from "lucide-react";
import { cn, CARD_SURFACE_B } from "../lib/utils";
import { useStore } from "../lib/store";
import QueryInterface from "./QueryInterface";

/** Clicks on these targets keep the floating chat open (page stays usable behind the panel). */
const INTERACTIVE_OUTSIDE_CHAT =
  [
    "a[href]",
    "button",
    "input",
    "textarea",
    "select",
    "option",
    "label",
    "canvas",
    '[role="button"]',
    '[role="link"]',
    '[role="tab"]',
    '[role="menuitem"]',
    '[role="option"]',
    '[role="checkbox"]',
    '[role="switch"]',
    '[role="combobox"]',
    '[role="listbox"]',
    "[contenteditable=true]",
  ].join(",");

function isInteractiveOutsideChatTarget(el: Element): boolean {
  return el.closest(INTERACTIVE_OUTSIDE_CHAT) !== null;
}

interface Props {
  visible: boolean;
  apiKeyMissing: boolean;
  /** Gray out Fleet in chat; store should hold a tail on these tabs (see App). */
  fleetOptionDisabled: boolean;
}

/**
 * Fixed FAB + chat-only popup for every tab except AI Assistant.
 * Open state lives in Zustand so it survives tab changes.
 *
 * No full-screen overlay: the rest of the app stays clickable. A capture listener closes the
 * chat on “empty” clicks; real controls (links, buttons, inputs, graph canvas, etc.) keep it open.
 * Navigating to the full AI Assistant tab still clears floating chat (see App.tsx).
 */
export default function FloatingChatDock({
  visible,
  apiKeyMissing,
  fleetOptionDisabled,
}: Props) {
  const floatingChatOpen = useStore((s) => s.floatingChatOpen);
  const setFloatingChatOpen = useStore((s) => s.setFloatingChatOpen);
  const toggleFloatingChat = useStore((s) => s.toggleFloatingChat);
  const panelRef = useRef<HTMLDivElement>(null);
  const fabRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!floatingChatOpen) return;

    const onPointerDownCapture = (e: PointerEvent) => {
      if (e.button !== 0) return;
      const t = e.target;
      if (!(t instanceof Node)) return;
      if (panelRef.current?.contains(t)) return;
      if (fabRef.current?.contains(t)) return;
      if (t instanceof Element && isInteractiveOutsideChatTarget(t)) return;
      setFloatingChatOpen(false);
    };

    document.addEventListener("pointerdown", onPointerDownCapture, true);
    return () => document.removeEventListener("pointerdown", onPointerDownCapture, true);
  }, [floatingChatOpen, setFloatingChatOpen]);

  if (!visible) return null;

  return (
    <>
      {floatingChatOpen && (
        <div
          ref={panelRef}
          className={cn(
            "fixed right-4 bottom-20 w-[min(24rem,90vw)] rounded-xl shadow-2xl overflow-hidden z-40 flex flex-col",
            CARD_SURFACE_B
          )}
          style={{ height: "min(58vh, 480px)" }}
          role="dialog"
          aria-modal="false"
          aria-label="Chat"
        >
          <div className="flex items-center justify-between px-3 py-2 border-b border-zinc-800 bg-zinc-900/95 backdrop-blur-sm">
            <div className="flex items-center gap-1.5 min-w-0">
              <MessageSquare className="w-4 h-4 text-sky-400 shrink-0" aria-hidden />
              <span className="text-xs font-medium text-zinc-200 shrink-0">AI Assistant</span>
            </div>
            <button
              type="button"
              onClick={() => setFloatingChatOpen(false)}
              className="text-zinc-600 hover:text-zinc-300 transition-colors shrink-0 p-1 rounded-md hover:bg-zinc-800/80"
              aria-label="Close chat"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
          <div className="flex-1 min-h-0 overflow-hidden">
            <QueryInterface
              apiKeyMissing={apiKeyMissing}
              showTraversalSidebar={false}
              showSuggestedQuestions={false}
              fleetOptionDisabled={fleetOptionDisabled}
              layout="embedded"
            />
          </div>
        </div>
      )}

      <button
        ref={fabRef}
        type="button"
        onClick={() => toggleFloatingChat()}
        className="fixed bottom-6 right-6 w-12 h-12 bg-sky-600 hover:bg-sky-500 rounded-full
          shadow-lg flex items-center justify-center transition-colors z-50"
        title={floatingChatOpen ? "Close chat" : "Open chat"}
      >
        {floatingChatOpen ? (
          <X className="w-5 h-5 text-white" />
        ) : (
          <MessageSquare className="w-5 h-5 text-white" />
        )}
      </button>
    </>
  );
}
