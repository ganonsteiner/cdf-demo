# aircraft-health-cag-demo

Demo project showcasing Cognite Data Fusion (CDF) architecture and Context Augmented Generation (CAG). The generic UI shows how a fleet of aircraft would typically be managed while the AI Assistant shows the value of CAG and its ability to connect the dots between a number of data sources and formats.

**Operator:** Desert Sky Aviation — flight school, KPHX  
**Fleet:** Four 1978 Cessna 172N Skyhawks (N4798E, N2251K, N8834Q, N1156P)  
**Engine:** Lycoming O-320-H2AD (shared across fleet)  
**Stack:** Python (FastAPI) + React/TypeScript

All code lives in [`aircraft-health-cag-demo/`](aircraft-health-cag-demo/). Commands below assume you're inside that directory.

## Quick Start

```bash
cd aircraft-health-cag-demo
```

**Prerequisites:** Node.js 18+ and Python 3.9+. A virtualenv is recommended:

```bash
python3 -m venv .venv && source .venv/bin/activate
```

**One-time setup** — installs deps, generates CSVs, runs ingestion:

```bash
npm run bootstrap
```

**Configure the agent:**

```bash
cp .env.example .env
# edit .env — set ANTHROPIC_API_KEY to your key from console.anthropic.com
```

**Start everything:**

```bash
npm run dev
```

Open [http://localhost:4000](http://localhost:4000)

| Service | Port |
|---------|------|
| Vite Dev Server (website) | 4000 |
| Mock CDF Server | 4001 |
| API Server | 8080 |

## What It Demonstrates

This project ingests three types of operational data into a unified knowledge graph and runs an AI agent over the connected result.

| Data Type | In This Project |
|-----------|----------------|
| **OT** (sensor/telemetry) | In-flight instrument/sensor readings, pilot reports |
| **IT** (business records) | Maintenance logbook, squawks, annual inspections |
| **ET** (engineering docs) | POH sections, FAA Airworthiness Directives, Service Bulletins |

CDF resource types used:

| Resource | Types |
|----------|-------|
| Assets | Fleet owner; aircraft; engine model; components (engine, cylinders, oil system, propeller, airframe, avionics, fuel system); operational policies |
| TimeSeries | Recorded per-flight - Aircraft: hobbs, tach, cycles, fuel used. Engine: oil pressure (min/max), oil temperature, CHT, EGT |
| Datapoints | Numeric, time-stamped in-flight instrument/sensor readings |
| Events | Flight; squawk; inspection; maintenance record |
| Relationships | `HAS_COMPONENT`, `IS_TYPE`, `GOVERNED_BY`, `HAS_POLICY`, `PERFORMED_ON`, `REFERENCES_AD`, `IDENTIFIED_ON`, `LINKED_TO` |
| Files | POH; airworthiness directives; service bulletins |

### CAG vs RAG

Standard RAG embeds text chunks and retrieves by vector similarity. It loses structure and can't traverse relationships between assets.

CAG traverses the knowledge graph — aircraft → components → events → relationships → documents — assembling context from connected data. No vector store, no embeddings. The UI shows every graph node visited in real time as the agent works.

### Cross-Aircraft Pattern Discovery

The most useful thing this demo shows: the agent can reason about what might be going wrong on one aircraft by comparing its situation to what went wrong on another in the past. It does that by reading fleet data—sensors, events, maintenance notes—and spotting similarities.

That kind of assistance augments human operators: critical connections are easier to notice and less likely to be lost in a steady stream of new data.

### Swapping Mock for Real CDF

Two lines in `.env`:

```bash
# Mock (default)
CDF_BASE_URL=http://localhost:4001
CDF_TOKEN=mock-token

# Real tenant
CDF_BASE_URL=https://api.cognitedata.com
CDF_TOKEN=<your-oidc-token>
```

No code changes needed. The `cognite-sdk` client works against both.

## Fleet

| Aircraft | Status | Notes |
|----------|--------|-------|
| N4798E | Airworthy | Cleared for normal operations |
| N2251K | Ferry only | Oil change is slightly past due on tach time; ferry to maintenance is authorized, no other flying |
| N8834Q | Caution | Recent flights show elevated CHT and a rough mag check... extra scrutiny warranted after N1156P's recent failure |
| N1156P | Not airworthy | Catastrophic engine failure; connecting rod failure with chronic lean detonation confirmed at teardown; engine condemned |

## Fleet Policies

Policies are stored as nodes in the knowledge graph and retrieved by the agent at query time.

- **Oil change grace** — ferry permitted if overdue ≤5 tach hours
- **Annual inspection** — no grace period (FAR 91.409)
- **Oil analysis** — every third oil change or annually

## Project Structure

```
aircraft-health-cag-demo/
├── package.json
├── pyproject.toml
├── data/
│   ├── flight_data_{TAIL}.csv
│   ├── maintenance_{TAIL}.csv
│   └── documents/
├── mock_cdf/
│   ├── server.py               # mock CDF API (port 4001)
│   └── store/store.py
├── src/
│   ├── agent/
│   │   ├── tools.py            # CDF graph traversal tools
│   │   ├── context.py          # context assembly
│   │   └── agent.py            # ReAct loop + SSE streaming
│   ├── ingest/
│   │   ├── index.py            # ingestion entry point
│   │   ├── ingest_assets.py
│   │   ├── ingest_flights.py
│   │   ├── ingest_maintenance.py
│   │   ├── ingest_fleet_graph.py
│   │   └── ingest_documents.py
│   └── api.py                  # FastAPI server (port 8080)
├── scripts/
│   ├── dataset.py              # single source of truth for fleet data
│   ├── transform_*.py
│   └── reset.py                # wipe and re-ingest
└── client/src/
    ├── components/
    │   ├── SetupBanner.tsx
    │   ├── FloatingChatDock.tsx
    │   ├── FleetPage.tsx
    │   ├── QueryInterface.tsx
    │   ├── GraphTraversalPanel.tsx
    │   ├── TraversalGraph.tsx
    │   ├── KnowledgeGraph.tsx
    │   ├── MaintenanceTimeline.tsx
    │   ├── FlightHistory.tsx
    │   └── AircraftComponents.tsx
    └── lib/
        ├── store.ts
        ├── types.ts
        └── api.ts
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/query` | SSE-streamed agent response |
| GET | `/api/health` | Service status |
| GET | `/api/fleet` | All four aircraft status |
| GET | `/api/status?aircraft={tail}` | Single aircraft health data |
| GET | `/api/squawks?aircraft={tail}` | Open squawks |
| GET | `/api/maintenance/upcoming?aircraft={tail}` | Upcoming maintenance due |
| GET | `/api/maintenance/history?aircraft={tail}` | Maintenance records |
| GET | `/api/flights?aircraft={tail}` | Flight history |
| GET | `/api/components?aircraft={tail}` | Component hierarchy |
| GET | `/api/graph` | Full knowledge graph |
| GET | `/api/policies` | Fleet policies |
