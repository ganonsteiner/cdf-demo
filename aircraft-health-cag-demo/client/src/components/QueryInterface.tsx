import { useRef, useEffect, useState } from "react";
import { api } from "../lib/api";
import { Send, AlertCircle } from "lucide-react";
import ReactMarkdown from "react-markdown";
import {
  cn,
  CARD_SURFACE_B,
  CARD_SURFACE_C,
  MAIN_TAB_CONTENT_FRAME,
  TAB_PAGE_TOP_INSET,
} from "../lib/utils";
import type { AgentEvent, GraphData } from "../lib/types";
import { traversalActivityCounts } from "../lib/traversalGraphIds";
import { useStore, TAILS, type TailNumber } from "../lib/store";
import GraphTraversalPanel from "./GraphTraversalPanel";

interface Props {
  apiKeyMissing: boolean;
  /** When false (e.g. Knowledge Graph popup), chat only — no traversal list. Default true for AI Assistant tab. */
  showTraversalSidebar?: boolean;
  /** When false (floating chat), hide suggested-question chips. */
  showSuggestedQuestions?: boolean;
  /** When true (floating chat on single-aircraft tabs), Fleet is disabled and grayed out. */
  fleetOptionDisabled?: boolean;
  /** Full tab page (selector strip + two cards) vs compact floating dock. */
  layout?: "page" | "embedded";
}

// Context-aware suggestions based on selected aircraft
const FLEET_SUGGESTIONS = [
  "What is the airworthiness status of the whole fleet?",
  "Which aircraft needs attention most urgently?",
  "Are there any similar symptoms across the fleet?",
  "What do fleet policies say about oil change overdue?",
];

const TAIL_SUGGESTIONS: Record<string, string[]> = {
  N4798E: [
    "Is N4798E airworthy?",
    "What maintenance is due soon on N4798E?",
    "When was the last oil change on N4798E?",
    "Show me the open squawks on N4798E",
  ],
  N2251K: [
    "Is N2251K authorized for a ferry flight?",
    "What is the oil change status on N2251K?",
    "What does the ferry flight policy say?",
    "Show N2251K's maintenance history",
  ],
  N8834Q: [
    "What are the symptoms on N8834Q?",
    "How serious is the CHT elevation on N8834Q?",
    "Does N8834Q's CHT issue resemble N1156P's pre-failure symptoms?",
    "What A&P inspection is required for N8834Q?",
  ],
  N1156P: [
    "What caused N1156P's engine failure?",
    "How does N1156P's flight log timeline compare to N8834Q's recent flights?",
    "What pre-failure symptoms did N1156P exhibit?",
    "What post-accident findings were documented on N1156P?",
  ],
};

function AircraftSelector({
  value,
  onChange,
  fleetDisabled = false,
  variant = "bar",
  dense = false,
}: {
  value: TailNumber | null;
  onChange: (t: TailNumber | null) => void;
  fleetDisabled?: boolean;
  /** Inline matches Flights / page shell; bar is for embedded floating chat. */
  variant?: "inline" | "bar";
  /** Tighter padding and chips for the floating chat bar. */
  dense?: boolean;
}) {
  const barPad = dense ? "px-2 py-1.5" : "px-3 py-2";
  const chipCls = cn(
    "rounded-full font-medium transition-colors border",
    dense ? "px-2 py-px text-[11px] leading-tight" : "px-2.5 py-0.5 text-xs"
  );

  return (
    <div
      className={cn(
        "flex items-center gap-2",
        variant === "bar" && cn("border-b border-zinc-800/60", barPad)
      )}
    >
      <div className={cn("flex flex-wrap", dense ? "gap-0.5" : "gap-1")}>
        <button
          type="button"
          disabled={fleetDisabled}
          onClick={() => {
            if (!fleetDisabled) onChange(null);
          }}
          title={fleetDisabled ? "Fleet scope not available on this page" : undefined}
          className={cn(
            chipCls,
            fleetDisabled &&
              "opacity-45 cursor-not-allowed text-zinc-600 border-zinc-800 bg-zinc-900/80 pointer-events-none",
            !fleetDisabled &&
              value === null &&
              "bg-sky-600 text-white border-sky-500",
            !fleetDisabled &&
              value !== null &&
              "bg-zinc-800 text-zinc-400 border-zinc-700 hover:border-zinc-500"
          )}
        >
          Fleet
        </button>
        {TAILS.map((t) => (
          <button
            key={t}
            type="button"
            onClick={() => onChange(t)}
            className={cn(
              chipCls,
              value === t
                ? "bg-sky-600 text-white border-sky-500"
                : "bg-zinc-800 text-zinc-400 border-zinc-700 hover:border-zinc-500"
            )}
          >
            {t}
          </button>
        ))}
      </div>
    </div>
  );
}

function formatTraversalActivityLine(events: AgentEvent[], graphData: GraphData | null): string {
  const { toolCount, stepCount, graphNodeCount } = traversalActivityCounts(events, graphData);
  const toolLabel = `${toolCount} tool${toolCount === 1 ? "" : "s"}`;
  const nodeOrStepLabel =
    graphNodeCount !== null
      ? `${graphNodeCount} node${graphNodeCount === 1 ? "" : "s"}`
      : `${stepCount} step${stepCount === 1 ? "" : "s"}`;
  return `${toolLabel} · ${nodeOrStepLabel}`;
}

export default function QueryInterface({
  apiKeyMissing,
  showTraversalSidebar = true,
  showSuggestedQuestions = true,
  fleetOptionDisabled = false,
  layout = "page",
}: Props) {
  const {
    selectedAircraft,
    setSelectedAircraft,
    chatMessages,
    addChatMessage,
    updateChatMessage,
    isQuerying,
    setIsQuerying,
    clearTraversalEvents,
    appendTraversalEvent,
    traversalEvents,
    startReplay,
    isReplaying,
    replayNodes,
    graphDataSnapshot,
  } = useStore();
  const setGraphDataSnapshot = useStore((s) => s.setGraphDataSnapshot);

  useEffect(() => {
    api.graph().then(setGraphDataSnapshot).catch(() => {});
  }, [setGraphDataSnapshot]);

  const [input, setInput] = useState("");
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [chatMessages]);

  const lastAssistantMsg = [...chatMessages].reverse().find((m) => m.role === "assistant");
  const displayEvents = isQuerying
    ? traversalEvents
    : isReplaying
    ? replayNodes
    : lastAssistantMsg?.traversalEvents ?? [];

  const suggestions = selectedAircraft
    ? (TAIL_SUGGESTIONS[selectedAircraft] ?? FLEET_SUGGESTIONS)
    : FLEET_SUGGESTIONS;

  const compact = layout === "embedded";

  const sendQuery = async (question: string) => {
    if (!question.trim() || isQuerying || apiKeyMissing) return;

    const userMsgId = crypto.randomUUID();
    addChatMessage({
      id: userMsgId,
      role: "user",
      content: question,
      timestamp: new Date(),
    });
    setInput("");
    setIsQuerying(true);
    clearTraversalEvents();

    const assistantMsgId = crypto.randomUUID();
    addChatMessage({
      id: assistantMsgId,
      role: "assistant",
      content: "",
      timestamp: new Date(),
      traversalEvents: [],
    });

    const sessionEvents: AgentEvent[] = [];

    try {
      const res = await fetch("/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, aircraft: selectedAircraft }),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        const errMsg =
          typeof err.detail === "object" ? err.detail.error : (err.detail || res.statusText);
        updateChatMessage(assistantMsgId, { content: `**Error:** ${errMsg}` });
        return;
      }

      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";

        for (const line of lines) {
          if (!line.startsWith("data:")) continue;
          const jsonStr = line.slice(5).trim();
          if (!jsonStr) continue;
          try {
            const event: AgentEvent = JSON.parse(jsonStr);
            sessionEvents.push(event);
            appendTraversalEvent(event);

            if (event.type === "final") {
              updateChatMessage(assistantMsgId, {
                content: event.content || "",
                traversalEvents: [...sessionEvents],
              });
            } else if (event.type === "error") {
              updateChatMessage(assistantMsgId, {
                content: `**Error:** ${event.message}`,
                traversalEvents: [...sessionEvents],
              });
            } else if (event.type === "done") {
              break;
            }
          } catch {
            // malformed JSON — skip
          }
        }
      }
    } catch (e) {
      updateChatMessage(assistantMsgId, {
        content: `**Connection error:** ${e instanceof Error ? e.message : String(e)}`,
      });
    } finally {
      setIsQuerying(false);
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    sendQuery(input);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key !== "Enter" || e.shiftKey) return;
    if (isQuerying) {
      e.preventDefault();
      return;
    }
    e.preventDefault();
    sendQuery(input);
  };

  const canReplay =
    !isQuerying && !isReplaying && (lastAssistantMsg?.traversalEvents?.length ?? 0) > 0;

  const placeholderText = apiKeyMissing
    ? "API key required..."
    : selectedAircraft
    ? `Ask about ${selectedAircraft}...`
    : "Ask about the fleet...";

  const chatBody = (
    <>
      {/* Suggested questions — full AI Assistant tab only, when empty */}
      {showSuggestedQuestions && chatMessages.length === 0 && (
        <div className="shrink-0 h-36 flex flex-col min-h-0 p-4 pt-3 pb-3 border-b border-zinc-800/60">
          <p className="text-xs text-zinc-500 mb-2 font-semibold uppercase tracking-widest shrink-0">
            Suggested questions for {selectedAircraft ? `${selectedAircraft}` : "Fleet"}
          </p>
          <div className="flex flex-wrap gap-2 content-start overflow-y-auto min-h-0 flex-1">
            {suggestions.map((q) => (
              <button
                key={q}
                onClick={() => sendQuery(q)}
                disabled={apiKeyMissing || isQuerying}
                className={cn(
                  "text-xs px-2.5 py-1.5 rounded-lg text-zinc-300 hover:border-zinc-600 transition-colors disabled:opacity-40 disabled:cursor-not-allowed text-left",
                  CARD_SURFACE_C
                )}
              >
                {q}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Messages */}
      <div
        className={cn("flex-1 overflow-y-auto", compact ? "p-2.5 space-y-2" : "p-4 space-y-4")}
        style={{ minHeight: 0 }}
      >
        {chatMessages.length === 0 && (
          <div
            className={cn(
              "flex flex-col items-center justify-center h-full text-center text-zinc-600",
              compact ? "py-4 text-xs" : "py-8"
            )}
          >
            <p className={cn(compact ? "text-xs" : "text-sm")}>
              {selectedAircraft ? `Ask about ${selectedAircraft}` : "Ask about the Desert Sky fleet"}
            </p>
            <p className={cn(compact ? "text-xs mt-0.5" : "text-xs mt-1")}>
              The agent will traverse the knowledge graph for context
            </p>
          </div>
        )}

        {chatMessages.map((msg) => (
          <div key={msg.id} className={cn("flex", msg.role === "user" ? "justify-end" : "justify-start")}>
            <div
              className={cn(
                "max-w-[85%]",
                compact ? "text-xs" : "text-sm",
                msg.role === "user"
                  ? cn(
                      "bg-sky-600 text-white rounded-tr-sm",
                      compact ? "rounded-xl px-3 py-2" : "rounded-2xl px-4 py-3"
                    )
                  : cn(
                      "bg-zinc-800 text-zinc-100 border border-zinc-700/70 rounded-tl-sm",
                      compact ? "rounded-xl px-3 py-2" : "rounded-2xl px-4 py-3"
                    )
              )}
            >
              {msg.role === "user" ? (
                <p>{msg.content}</p>
              ) : msg.content ? (
                <div
                  className={cn(
                    "prose prose-invert max-w-none",
                    compact
                      ? cn(
                          "text-xs prose-headings:text-xs prose-headings:font-medium prose-headings:leading-snug",
                          "prose-headings:mt-0 prose-headings:mb-0",
                          "prose-p:text-xs prose-p:!my-0 prose-p:leading-snug",
                          "prose-li:text-xs prose-li:my-0 prose-li:py-0 prose-li:leading-snug",
                          "prose-ul:my-0 prose-ul:mt-1 prose-ul:!mb-0 prose-ol:my-0 prose-ol:mt-1 prose-ol:!mb-0",
                          "prose-code:text-[11px]",
                          "prose-hr:!my-1 prose-hr:!border-zinc-600",
                          "prose-blockquote:my-1 prose-blockquote:py-0",
                          "[&>*:first-child]:!mt-0 [&>*:last-child]:!mb-0",
                          "[&_hr]:!my-1 [&_p+hr]:!mt-1 [&_hr+p]:!mt-1 [&_ul+hr]:!mt-1 [&_ol+hr]:!mt-1 [&_hr+ul]:!mt-1 [&_hr+ol]:!mt-1"
                        )
                      : "prose-sm"
                  )}
                >
                  <ReactMarkdown>{msg.content}</ReactMarkdown>
                </div>
              ) : (
                <StreamingIndicator compact={compact} />
              )}
              {msg.role === "assistant" && msg.traversalEvents && msg.traversalEvents.length > 0 && (
                <p
                  className={cn(
                    "text-zinc-500 border-t border-zinc-700/80",
                    compact ? "text-[11px] mt-1.5 pt-1.5" : "text-xs mt-2 pt-2"
                  )}
                >
                  {formatTraversalActivityLine(msg.traversalEvents, graphDataSnapshot)}
                </p>
              )}
            </div>
          </div>
        ))}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className={cn("border-t border-zinc-800/60 shrink-0", compact ? "p-2" : "p-3")}>
        {apiKeyMissing && (
          <div
            className={cn(
              "flex items-center gap-2 text-xs text-yellow-500",
              compact ? "mb-1.5" : "mb-2"
            )}
          >
            <AlertCircle className="w-3.5 h-3.5 shrink-0" />
            Add ANTHROPIC_API_KEY to .env in the project root to enable queries
          </div>
        )}
        <form
          onSubmit={handleSubmit}
          className={cn("flex", compact ? "items-center gap-1.5" : "items-end gap-2")}
        >
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={placeholderText}
            disabled={apiKeyMissing}
            rows={1}
            className={cn(
              "flex-1 m-0 resize-none bg-zinc-800 border border-zinc-700 text-zinc-100 placeholder-zinc-600 focus:outline-none focus:border-sky-600 focus:ring-1 focus:ring-sky-600 disabled:opacity-50 disabled:cursor-not-allowed box-border max-h-[7.5rem]",
              compact
                ? "rounded-lg px-2.5 text-xs leading-4 min-h-9 py-[calc((2.25rem-1rem-2px)/2)]"
                : "rounded-xl px-3 text-sm leading-5 min-h-11 py-[calc((2.75rem-1.25rem-2px)/2)]"
            )}
          />
          <button
            type="submit"
            disabled={!input.trim() || isQuerying || apiKeyMissing}
            className={cn(
              "shrink-0 flex items-center justify-center bg-sky-600 hover:bg-sky-500 text-white transition-colors disabled:opacity-40 disabled:cursor-not-allowed",
              compact ? "h-9 w-9 rounded-lg" : "h-11 w-11 rounded-xl"
            )}
            aria-label="Send message"
          >
            <Send className={cn(compact ? "w-4 h-4" : "w-5 h-5")} aria-hidden />
          </button>
        </form>
      </div>
    </>
  );

  if (layout === "embedded") {
    return (
      <div
        className={cn(
          "h-full gap-0 overflow-hidden flex flex-col",
          showTraversalSidebar && "md:flex-row"
        )}
      >
        <div
          className={cn(
            "flex-1 flex flex-col min-w-0 min-h-0 overflow-hidden",
            showTraversalSidebar && "md:border-r md:border-zinc-800"
          )}
        >
          <AircraftSelector
            value={selectedAircraft}
            onChange={setSelectedAircraft}
            fleetDisabled={fleetOptionDisabled}
            variant="bar"
            dense
          />
          {chatBody}
        </div>

        {showTraversalSidebar && (
          <div className="w-full md:w-80 xl:w-96 shrink-0 flex flex-col min-h-0 overflow-hidden border-t md:border-t-0 border-zinc-800">
            <GraphTraversalPanel
              events={displayEvents}
              isStreaming={isQuerying}
              canReplay={canReplay}
              onReplay={() => {
                if (lastAssistantMsg?.traversalEvents) {
                  startReplay(lastAssistantMsg.traversalEvents);
                }
              }}
              isReplaying={isReplaying}
            />
          </div>
        )}
      </div>
    );
  }

  return (
    <div
      className={cn(
        "flex flex-1 min-h-0 flex-col overflow-hidden pb-6",
        MAIN_TAB_CONTENT_FRAME,
        TAB_PAGE_TOP_INSET
      )}
    >
      <div className="shrink-0 mb-3">
        <AircraftSelector
          value={selectedAircraft}
          onChange={setSelectedAircraft}
          fleetDisabled={fleetOptionDisabled}
          variant="inline"
        />
      </div>

      <div
        className={cn(
          "flex-1 min-h-0 flex flex-col gap-4 overflow-hidden",
          showTraversalSidebar && "md:flex-row"
        )}
      >
        <div className={cn("flex-1 min-w-0 min-h-0 flex flex-col rounded-xl overflow-hidden", CARD_SURFACE_B)}>
          {chatBody}
        </div>

        {showTraversalSidebar && (
          <div className="w-full md:w-80 xl:w-96 shrink-0 flex flex-col min-h-[280px] md:min-h-0 min-w-0 overflow-hidden rounded-xl">
            <GraphTraversalPanel
              events={displayEvents}
              isStreaming={isQuerying}
              canReplay={canReplay}
              onReplay={() => {
                if (lastAssistantMsg?.traversalEvents) {
                  startReplay(lastAssistantMsg.traversalEvents);
                }
              }}
              isReplaying={isReplaying}
            />
          </div>
        )}
      </div>
    </div>
  );
}

function StreamingIndicator({ compact = false }: { compact?: boolean }) {
  return (
    <div className="flex items-center gap-0.5 py-0.5" aria-label="Assistant is typing">
      {[0, 150, 300].map((delay) => (
        <span
          key={delay}
          className={cn(
            "bg-zinc-400 rounded-full animate-bounce",
            compact ? "w-1 h-1" : "w-1.5 h-1.5"
          )}
          style={{ animationDelay: `${delay}ms` }}
        />
      ))}
    </div>
  );
}
