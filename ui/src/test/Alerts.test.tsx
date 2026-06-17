import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { Alerts } from "../panels/Alerts";
import { ALERT_SEVERITY_RANK, type Alert, type AlertSeverity } from "../types/alerts";

vi.mock("../store/cockpitStore", () => ({
  useCockpitStore: vi.fn(),
}));

vi.mock("../lib/format", () => ({
  tsAgo: (_ts: number) => "1s ago",
}));

// Mock Panel to avoid its internal store reads
vi.mock("../components/Panel", () => ({
  Panel: ({ title, children, status }: { title: string; children: React.ReactNode; status?: React.ReactNode }) => (
    <div data-testid="panel">
      <h2>{title}</h2>
      {status && <span>{status}</span>}
      {children}
    </div>
  ),
}));

import { useCockpitStore, type CockpitState } from "../store/cockpitStore";
const mockUseCockpitStore = vi.mocked(useCockpitStore);

function makeAlert(id: string, severity: AlertSeverity, ts_ns: number): Alert {
  return { id, severity, title: `Alert ${id}`, body: `Body ${id}`, ts_ns };
}

describe("Alerts panel", () => {
  beforeEach(() => {
    mockUseCockpitStore.mockImplementation((selector: (state: CockpitState) => unknown) => {
      return selector({
        alerts: { list: [] },
        meta: { feedStatus: "ok" },
      }) as ReturnType<typeof selector>;
    });
  });

  it("renders empty state when no alerts", () => {
    render(<Alerts />);
    expect(screen.getByText("No alerts.")).toBeInTheDocument();
  });

  it("renders alerts when present", () => {
    const alerts = [makeAlert("a1", "info", 1000)];
    mockUseCockpitStore.mockImplementation((selector: (state: CockpitState) => unknown) => {
      return selector({ alerts: { list: alerts }, meta: { feedStatus: "ok" } }) as ReturnType<typeof selector>;
    });
    render(<Alerts />);
    expect(screen.getByText("Alert a1")).toBeInTheDocument();
  });

  it("critical severity appears before non-critical in sorted list", () => {
    const alerts = [
      makeAlert("low1", "low", 3000),
      makeAlert("crit1", "critical", 2000),
      makeAlert("med1", "medium", 1000),
    ];
    const sorted = [...alerts].sort((a, b) => {
      const sr = ALERT_SEVERITY_RANK[a.severity] - ALERT_SEVERITY_RANK[b.severity];
      if (sr !== 0) return sr;
      return (b.ts_ns ?? 0) - (a.ts_ns ?? 0);
    });
    mockUseCockpitStore.mockImplementation((selector: (state: CockpitState) => unknown) => {
      return selector({ alerts: { list: sorted }, meta: { feedStatus: "ok" } }) as ReturnType<typeof selector>;
    });
    render(<Alerts />);
    const items = screen.getAllByRole("listitem");
    expect(items.length).toBe(3);
    expect(items[0]).toHaveTextContent("critical");
    expect(items[0]).toHaveTextContent("Alert crit1");
    expect(items[1]).toHaveTextContent("medium");
    expect(items[2]).toHaveTextContent("low");
  });

  it("shows active count in header", () => {
    const alerts = [makeAlert("a1", "high", 1000), makeAlert("a2", "low", 2000)];
    mockUseCockpitStore.mockImplementation((selector: (state: CockpitState) => unknown) => {
      return selector({ alerts: { list: alerts }, meta: { feedStatus: "ok" } }) as ReturnType<typeof selector>;
    });
    render(<Alerts />);
    expect(screen.getByText("2 active")).toBeInTheDocument();
  });
});
