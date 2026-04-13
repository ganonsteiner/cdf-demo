/** Global Zustand store for the Desert Sky Aviation fleet demo. */

import { create } from "zustand";
import type { AgentEvent, ChatMessage, GraphData } from "./types";

export const TAILS = ["N4798E", "N2251K", "N8834Q", "N1156P"] as const;
export type TailNumber = typeof TAILS[number];

interface AppState {
  selectedAircraft: TailNumber | null;
  setSelectedAircraft: (tail: TailNumber | null) => void;

  chatMessages: ChatMessage[];
  addChatMessage: (msg: ChatMessage) => void;
  updateChatMessage: (id: string, updates: Partial<ChatMessage>) => void;

  isQuerying: boolean;
  setIsQuerying: (v: boolean) => void;

  traversalEvents: AgentEvent[];
  appendTraversalEvent: (e: AgentEvent) => void;
  clearTraversalEvents: () => void;

  replayNodes: AgentEvent[];
  isReplaying: boolean;
  startReplay: (nodes: AgentEvent[]) => Promise<void>;

  floatingChatOpen: boolean;
  setFloatingChatOpen: (v: boolean) => void;
  toggleFloatingChat: () => void;

  graphDataSnapshot: GraphData | null;
  setGraphDataSnapshot: (d: GraphData | null) => void;
}

export const useStore = create<AppState>((set) => ({
  selectedAircraft: "N4798E",

  setSelectedAircraft: (tail) => set({ selectedAircraft: tail }),

  chatMessages: [],
  addChatMessage: (msg) =>
    set((s) => ({ chatMessages: [...s.chatMessages, msg] })),
  updateChatMessage: (id, updates) =>
    set((s) => ({
      chatMessages: s.chatMessages.map((m) =>
        m.id === id ? { ...m, ...updates } : m
      ),
    })),

  isQuerying: false,
  setIsQuerying: (v) => set({ isQuerying: v }),

  traversalEvents: [],
  appendTraversalEvent: (e) =>
    set((s) => ({ traversalEvents: [...s.traversalEvents, e] })),
  clearTraversalEvents: () => set({ traversalEvents: [] }),

  replayNodes: [],
  isReplaying: false,
  startReplay: async (nodes: AgentEvent[]) => {
    set({ replayNodes: [], isReplaying: true });
    for (const node of nodes) {
      await new Promise<void>((res) => setTimeout(res, 150));
      set((s) => ({ replayNodes: [...s.replayNodes, node] }));
    }
    set({ isReplaying: false });
  },

  floatingChatOpen: false,
  setFloatingChatOpen: (v) => set({ floatingChatOpen: v }),
  toggleFloatingChat: () =>
    set((s) => ({ floatingChatOpen: !s.floatingChatOpen })),

  graphDataSnapshot: null,
  setGraphDataSnapshot: (d) => set({ graphDataSnapshot: d }),
}));
