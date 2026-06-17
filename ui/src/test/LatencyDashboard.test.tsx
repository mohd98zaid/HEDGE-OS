import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { LatencyDashboard } from "../panels/LatencyDashboard";
import type { LatencyAggregate } from "../types/latency";

vi.mock("../store/cockpitStore", () => ({
  useCockpitStore: vi.fn(),
}));

vi.mock("../lib/format", () => ({
  fmtNanos: (n: number) => `${(n / 1_000_000).toFixed(1)}ms`,
}));

// Mock Panel
vi.mock("../components/Panel", () => ({
  Panel: ({ title, children, status }: { title: string; children: React.ReactNode; status?: React.ReactNode }) => (
    <div data-testid="panel">
      <h2>{title}</h2>
      {status && <span>{status}</span>}
      {children}
    </div>
  ),
}));

// Mock EmptyState
vi.mock("../components/EmptyState", () => ({
  EmptyState: () => <div>No data yet</div>,
}));

import { useCockpitStore, type CockpitState } from "../store/cockpitStore";
const mockUseCockpitStore = vi.mocked(useCockpitStore);

const STAGES = [
  "TickIngest", "FeatureExtraction", "AiScoringFetch",
  "RiskCheck", "ExecutionRouting", "BrokerSubmit",
] as const;

describe("LatencyDashboard", () => {
  beforeEach(() => {
    mockUseCockpitStore.mockImplementation((selector: (state: CockpitState) => unknown) => {
      return selector({
        latency: { aggregates: {}, records: [] },
      }) as ReturnType<typeof selector>;
    });
  });

  it("renders all 6 stages in the table when records exist", () => {
    mockUseCockpitStore.mockImplementation((selector: (state: CockpitState) => unknown) => {
      return selector({
        latency: { aggregates: {}, records: [{ stage: "TickIngest" }] },
      }) as ReturnType<typeof selector>;
    });
    render(<LatencyDashboard />);
    for (const stage of STAGES) {
      expect(screen.getByText(stage)).toBeInTheDocument();
    }
  });

  it("shows empty state when no records", () => {
    render(<LatencyDashboard />);
    expect(screen.getByText("No data yet")).toBeInTheDocument();
  });

  it("renders p50/p95/p99 values when aggregates exist", () => {
    const aggregates: Record<string, LatencyAggregate> = {
      TickIngest: {
        stage: "TickIngest",
        p50_nanos: 500_000,
        p95_nanos: 1_200_000,
        p99_nanos: 1_800_000,
        budget_nanos: 2_000_000,
        samples: 100,
        breach_count: 2,
      },
    };
    mockUseCockpitStore.mockImplementation((selector: (state: CockpitState) => unknown) => {
      return selector({
        latency: { aggregates, records: [{ stage: "TickIngest" }] },
      }) as ReturnType<typeof selector>;
    });
    render(<LatencyDashboard />);
    expect(screen.getByText("0.5ms")).toBeInTheDocument();
    expect(screen.getByText("1.2ms")).toBeInTheDocument();
    expect(screen.getByText("1.8ms")).toBeInTheDocument();
  });
});
