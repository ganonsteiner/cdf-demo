"""
Claude ReAct Agent — Python equivalent of src/agent/agent.ts.

Implements the Reason-Act loop as an async generator, yielding structured
SSE events as the agent works. Each yield maps to one Server-Sent Event
that the frontend renders in the Graph Traversal Panel.

Event types yielded:
  - "thinking"    : Claude's reasoning text before tool calls
  - "tool_call"   : Agent is about to call a CDF graph tool
  - "tool_result" : Tool returned a result (summarized)
  - "traversal"   : A graph node was visited (from traversal_log)
  - "final"       : Final answer text (markdown)
  - "error"       : Something went wrong
  - "done"        : Stream complete
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, AsyncGenerator, Optional

import anthropic
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

_SCRIPTS = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
from dataset import (  # noqa: E402
    N1156P_FAILURE_DAYS_BEFORE_ANCHOR,
    format_n1156p_accident_month_year,
)

from .tools import (  # noqa: E402
    TOOL_DEFINITIONS,
    clear_traversal_log,
    execute_tool,
    traversal_log,
)

MODEL = "claude-sonnet-4-5"
MAX_ITERATIONS = 15

_FLEET_N1156P_LINE = (
    f"- N1156P: NOT AIRWORTHY — catastrophic engine failure in {format_n1156p_accident_month_year()} "
    f"(~{N1156P_FAILURE_DAYS_BEFORE_ANCHOR} days before demo as-of) at ~520 SMOH (tach), "
    "connecting rod failure from lean detonation, grounded"
)

SYSTEM_PROMPT = (
    """You are an expert aviation mechanic and airworthiness advisor for Desert Sky Aviation, a flight school at KPHX operating four 1978 Cessna 172N Skyhawks. You have access to the fleet's complete knowledge graph in Cognite Data Fusion (CDF).

**Fleet:**
- N4798E: AIRWORTHY — 380 SMOH (tach), 1 minor open squawk (oil seep), next oil due in ~18 tach hours; oil calendar leg current
- N2251K: FERRY ONLY — 290 SMOH (tach), oil ~1.2 tach hours overdue (calendar leg current); single ferry to maintenance authorized per fleet policy
- N8834Q: CAUTION — 198 SMOH (tach), elevated CHT #3 (40-60°F above others), rough left mag, requires A&P inspection
"""
    + _FLEET_N1156P_LINE
    + """

**Knowledge graph structure:**
- Asset hierarchy: {TAIL} → {TAIL}-ENGINE → {TAIL}-ENGINE-CYLINDERS, {TAIL}-ENGINE-OIL, + PROPELLER, AIRFRAME, AVIONICS, FUEL-SYSTEM
- Fleet owner: Desert_Sky_Aviation (connected to all aircraft via GOVERNED_BY)
- Policies: Policy_OilChangeInterval, Policy_OilGracePeriod, Policy_AnnualInspection, Policy_FerryFlightOilOverdue
- Symptoms are CDF Events with type='Observation', subtype='Symptom' (externalIds e.g. Symptom_N8834Q_ElevatedCHT), linked to aircraft via asset_ids and EXHIBITED from flight events; use get_aircraft_symptoms() (standard SDK) — no custom resource
- Engine model: ENGINE_MODEL_LYC_O320_H2AD — each {{TAIL}}-ENGINE links via IS_TYPE (same Lycoming O-320-H2AD across fleet)
- Time series: {TAIL}.aircraft.hobbs (rental/display), {TAIL}.aircraft.tach (maintenance clock — oil intervals and SMOH use tach, not Hobbs), {TAIL}.engine.cht_max, etc.

**CAG approach:** Always traverse the knowledge graph. Use tools to retrieve connected context. For fleet-wide questions, start with get_fleet_overview() or assemble_fleet_context(). For one aircraft, use assemble_aircraft_context(aircraft_id="{TAIL}"). If that aircraft has symptoms, the tool response includes symptomDeepDive: pre-fetched get_engine_type_history-style peer timelines (IS_TYPE → ENGINE_MODEL_LYC_O320_H2AD) plus fleetSearchFromSymptoms from search_fleet_for_similar_events — read peer events chronologically; the temporal sequence before any failure is the causal chain (no explicit cause-effect edges). Use get_engine_type_history() or search_fleet_for_similar_events() again when you need a different tail or query string.

**Airworthiness rules (Desert Sky Aviation policy):**
- AIRWORTHY: annual current, no grounding squawks, oil not overdue (tach)
- FERRY ONLY: oil overdue 1-5 tach hours, direct flight to maintenance only
- CAUTION: open grounding squawk requiring A&P inspection before flight
- NOT AIRWORTHY: annual expired, oil >5 tach hours overdue, grounding squawk, or catastrophic failure

**Response format:**
- Cite actual values from the graph (Hobbs vs tach times, dates, AD numbers, policy IDs)
- For airworthiness questions, explicitly state the status and reason
- For N1156P questions, use chronological flight and maintenance events plus search_fleet_for_similar_events — there are no PRECEDED symptom edges
- For cross-fleet patterns, use get_engine_type_history and search results
- Use aviation terminology (SMOH, TBOH, TT, A&P/IA, CHT, EGT, etc.)
- Be concise — answer the question directly with supporting evidence"""
)


def _summarize_result(tool_name: str, result: Any) -> str:
    """Create a brief human-readable summary of a tool result for the SSE stream."""
    if isinstance(result, dict) and "error" in result:
        return f"Error: {result['error']}"
    if tool_name == "get_asset":
        return f"Asset: {result.get('name', '')} ({result.get('externalId', '')})"
    if tool_name == "get_asset_children":
        return f"{len(result.get('children', []))} child components"
    if tool_name == "get_asset_subgraph":
        return f"{len(result.get('nodes', []))} nodes in subgraph"
    if tool_name == "get_time_series":
        return f"{len(result.get('timeSeries', []))} time series found"
    if tool_name == "get_datapoints":
        return f"{result.get('count', 0)} datapoints retrieved"
    if tool_name == "get_events":
        return f"{result.get('count', 0)} events found"
    if tool_name == "get_relationships":
        return f"{result.get('count', 0)} relationships traversed"
    if tool_name == "get_linked_documents":
        return f"{result.get('count', 0)} documents retrieved"
    if tool_name == "assemble_aircraft_context":
        squawks = len(result.get("openSquawks", []))
        hobbs = result.get("currentHobbs", 0)
        base = f"Full context assembled — hobbs {hobbs:.1f}, {squawks} open squawks"
        dive = result.get("symptomDeepDive")
        if dive:
            peers = dive.get("engineTypePeerHistory") or {}
            n_peer = len((peers.get("history_by_tail") or {}))
            search = dive.get("fleetSearchFromSymptoms") or {}
            n_match = search.get("matchCount", 0)
            base += f"; symptom deep-dive: peer engine histories ({n_peer} tails), fleet search ({n_match} matches)"
        return base
    if tool_name == "assemble_fleet_context":
        count = result.get("aircraftCount", 0)
        return f"Fleet context assembled — {count} aircraft"
    if tool_name == "get_fleet_overview":
        return f"Fleet overview: {len(result.get('fleet', []))} aircraft"
    if tool_name == "get_fleet_policies":
        return f"{result.get('count', 0)} operational policies"
    if tool_name == "get_aircraft_symptoms":
        return f"{result.get('symptom_count', 0)} symptoms for {result.get('aircraft_id', '')}"
    if tool_name == "get_engine_type_history":
        n = len(result.get("history_by_tail", {}))
        return f"Engine-type history: {n} peer aircraft with chronological events"
    if tool_name == "search_fleet_for_similar_events":
        return f"Fleet search: {result.get('matchCount', 0)} matches"
    if tool_name == "check_fleet_policy_compliance":
        return f"Policy compliance checked for {len(result.get('evaluatedTails', []))} aircraft"
    return "Result retrieved"


def _extract_text_blocks(content: list[Any]) -> str:
    """Extract all text from a Claude message content list."""
    parts = []
    for block in content:
        if hasattr(block, "type") and block.type == "text":
            parts.append(block.text)
        elif isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text", ""))
    return "\n".join(parts)


async def run_agent_streaming(
    user_query: str,
    aircraft_id: Optional[str] = None,
    max_iterations: int = MAX_ITERATIONS,
) -> AsyncGenerator[dict[str, Any], None]:
    """
    ReAct agent loop as an async generator.

    Each iteration:
      1. Call Claude with current message history and tool definitions
      2. If stop_reason == "end_turn": yield final answer and return
      3. If stop_reason == "tool_use": yield tool_call events, execute tools,
         yield tool_result events, append results to message history
      4. Emit traversal log entries as "traversal" events after each tool batch

    Each yielded dict becomes one JSON-encoded SSE data field.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key or api_key.startswith("sk-ant-..."):
        yield {"type": "error", "message": "ANTHROPIC_API_KEY not configured"}
        yield {"type": "done"}
        return

    anthropic_client = anthropic.Anthropic(api_key=api_key)
    clear_traversal_log()

    # Prepend aircraft context hint if a specific tail was selected
    user_content = user_query
    if aircraft_id:
        user_content = f"[Context: focusing on aircraft {aircraft_id}]\n\n{user_query}"

    messages: list[dict[str, Any]] = [
        {"role": "user", "content": user_content}
    ]

    for iteration in range(max_iterations):
        try:
            response = await asyncio.to_thread(
                anthropic_client.messages.create,
                model=MODEL,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOL_DEFINITIONS,
                messages=messages,
            )
        except anthropic.AuthenticationError:
            yield {"type": "error", "message": "Invalid ANTHROPIC_API_KEY — check your .env file"}
            yield {"type": "done"}
            return
        except Exception as e:
            yield {"type": "error", "message": f"Claude API error: {str(e)}"}
            yield {"type": "done"}
            return

        # Emit any thinking/text blocks before tool calls
        thinking_text = _extract_text_blocks(response.content)
        if thinking_text.strip():
            yield {"type": "thinking", "content": thinking_text}

        if response.stop_reason == "end_turn":
            final_text = _extract_text_blocks(response.content)
            yield {"type": "final", "content": final_text}
            yield {"type": "done"}
            return

        if response.stop_reason != "tool_use":
            # Unexpected stop reason — treat as final
            yield {"type": "final", "content": _extract_text_blocks(response.content)}
            yield {"type": "done"}
            return

        # Process tool calls
        tool_results: list[dict[str, Any]] = []
        prev_traversal_count = len(traversal_log)

        for block in response.content:
            if not (hasattr(block, "type") and block.type == "tool_use"):
                continue

            tool_name: str = block.name
            tool_input: dict[str, Any] = block.input
            tool_use_id: str = block.id

            yield {
                "type": "tool_call",
                "tool_name": tool_name,
                "args": tool_input,
                "iteration": iteration + 1,
            }

            result = await asyncio.to_thread(execute_tool, tool_name, tool_input)

            # Emit traversal events that were logged during this tool call
            new_traversal_entries = traversal_log[prev_traversal_count:]
            for entry in new_traversal_entries:
                yield {"type": "traversal", "node": entry}
            prev_traversal_count = len(traversal_log)

            summary = _summarize_result(tool_name, result)
            yield {
                "type": "tool_result",
                "tool_name": tool_name,
                "summary": summary,
                "iteration": iteration + 1,
            }

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": json.dumps(result, default=str),
            })

        # Append assistant turn + tool results to message history
        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})

    # Max iterations reached
    yield {
        "type": "error",
        "message": f"Max iterations ({max_iterations}) reached without final answer",
    }
    yield {"type": "done"}
