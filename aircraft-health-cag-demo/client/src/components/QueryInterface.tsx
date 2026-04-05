import { useRef, useEffect, useState } from "react";
import { Send, AlertCircle } from "lucide-react";
import ReactMarkdown from "react-markdown";
import { cn } from "../lib/utils";
import type { AgentEvent } from "../lib/types";
import { useStore, TAILS, type TailNumber } from "../lib/store";
import GraphTraversalPanel from "./GraphTraversalPanel";

interface Props {
  apiKeyMissing: boolean;
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

function AircraftSelector({ value, onChange }: { value: TailNumber | null; onChange: (t: TailNumber | null) => void }) {
  return (
    <div className="flex items-center gap-2 px-3 py-2 border-b border-zinc-800 bg-zinc-900/50">
      <span className="text-xs text-zinc-500 shrink-0">Aircraft:</span>
      <div className="flex gap-1 flex-wrap">
        <button
          onClick={() => onChange(null)}
          className={cn(
            "px-2.5 py-0.5 rounded-full text-xs font-medium transition-colors border",
            value === null
              ? "bg-sky-600 text-white border-sky-500"
              : "bg-zinc-800 text-zinc-400 border-zinc-700 hover:border-zinc-500"
          )}
        >
          Fleet
        </button>
        {TAILS.map((t) => (
          <button
            key={t}
            onClick={() => onChange(t)}
            className={cn(
              "px-2.5 py-0.5 rounded-full text-xs font-medium transition-colors border",
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

export default function QueryInterface({ apiKeyMissing }: Props) {
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
  } = useStore();

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
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendQuery(input);
    }
  };

  const canReplay =
    !isQuerying && !isReplaying && (lastAssistantMsg?.traversalEvents?.length ?? 0) > 0;

  const placeholderText = apiKeyMissing
    ? "API key required..."
    : selectedAircraft
    ? `Ask about ${selectedAircraft}...`
    : "Ask about the fleet...";

  return (
    <div className="h-full flex flex-col lg:flex-row gap-0 overflow-hidden">
      {/* Chat panel */}
      <div className="flex-1 flex flex-col min-w-0 bg-zinc-900 border-r border-zinc-800 overflow-hidden">
        {/* Aircraft selector */}
        <AircraftSelector value={selectedAircraft} onChange={setSelectedAircraft} />

        {/* Suggested questions — only when empty */}
        {chatMessages.length === 0 && (
          <div className="shrink-0 h-36 flex flex-col min-h-0 p-4 pt-3 pb-3 border-b border-zinc-800">
            <p className="text-xs text-zinc-500 mb-2 font-medium uppercase tracking-wide shrink-0">
              Suggested questions {selectedAircraft ? `— ${selectedAircraft}` : "— Fleet"}
            </p>
            <div className="flex flex-wrap gap-2 content-start overflow-y-auto min-h-0 flex-1">
              {suggestions.map((q) => (
                <button
                  key={q}
                  onClick={() => sendQuery(q)}
                  disabled={apiKeyMissing || isQuerying}
                  className="text-xs px-2.5 py-1.5 rounded-lg bg-zinc-800 border border-zinc-700 text-zinc-300
                    hover:bg-zinc-700 hover:border-zinc-600 transition-colors disabled:opacity-40 disabled:cursor-not-allowed text-left"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Messages */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4" style={{ minHeight: 0 }}>
          {chatMessages.length === 0 && (
            <div className="flex flex-col items-center justify-center h-full text-center text-zinc-600 py-8">
              <p className="text-sm">
                {selectedAircraft ? `Ask about ${selectedAircraft}` : "Ask about the Desert Sky fleet"}
              </p>
              <p className="text-xs mt-1">The agent traverses the CDF knowledge graph to answer</p>
            </div>
          )}

          {chatMessages.map((msg) => (
            <div key={msg.id} className={cn("flex", msg.role === "user" ? "justify-end" : "justify-start")}>
              <div
                className={cn(
                  "max-w-[85%] rounded-2xl px-4 py-3 text-sm",
                  msg.role === "user"
                    ? "bg-sky-600 text-white rounded-tr-sm"
                    : "bg-zinc-800 text-zinc-100 rounded-tl-sm border border-zinc-700"
                )}
              >
                {msg.role === "user" ? (
                  <p>{msg.content}</p>
                ) : msg.content ? (
                  <div className="prose prose-sm prose-invert max-w-none">
                    <ReactMarkdown>{msg.content}</ReactMarkdown>
                  </div>
                ) : (
                  <StreamingIndicator />
                )}
                {msg.role === "assistant" && msg.traversalEvents && msg.traversalEvents.length > 0 && (
                  <p className="text-xs text-zinc-600 mt-2 border-t border-zinc-700 pt-2">
                    {msg.traversalEvents.filter((e) => e.type === "traversal").length} nodes traversed ·{" "}
                    {msg.traversalEvents.filter((e) => e.type === "tool_call").length} tool calls
                  </p>
                )}
              </div>
            </div>
          ))}
          <div ref={messagesEndRef} />
        </div>

        {/* Input */}
        <div className="p-3 border-t border-zinc-800">
          {apiKeyMissing && (
            <div className="flex items-center gap-2 text-xs text-yellow-500 mb-2">
              <AlertCircle className="w-3.5 h-3.5" />
              Add ANTHROPIC_API_KEY to backend/.env to enable queries
            </div>
          )}
          <form onSubmit={handleSubmit} className="flex gap-2 items-end">
            <textarea
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={placeholderText}
              disabled={apiKeyMissing || isQuerying}
              rows={1}
              className="flex-1 resize-none bg-zinc-800 border border-zinc-700 rounded-xl px-3 py-2.5 text-sm
                text-zinc-100 placeholder-zinc-600 focus:outline-none focus:border-sky-600 focus:ring-1 focus:ring-sky-600
                disabled:opacity-50 disabled:cursor-not-allowed"
              style={{ minHeight: 44, maxHeight: 120 }}
            />
            <button
              type="submit"
              disabled={!input.trim() || isQuerying || apiKeyMissing}
              className="p-2.5 bg-sky-600 hover:bg-sky-500 rounded-xl text-white transition-colors
                disabled:opacity-40 disabled:cursor-not-allowed shrink-0"
            >
              <Send className="w-4 h-4" />
            </button>
          </form>
        </div>
      </div>

      {/* Traversal panel */}
      <div className="lg:w-80 xl:w-96 shrink-0 flex flex-col overflow-hidden">
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
    </div>
  );
}

function StreamingIndicator() {
  return (
    <div className="flex items-center gap-1 py-1">
      {[0, 150, 300].map((delay) => (
        <span
          key={delay}
          className="w-1.5 h-1.5 bg-sky-400 rounded-full animate-bounce"
          style={{ animationDelay: `${delay}ms` }}
        />
      ))}
    </div>
  );
}
