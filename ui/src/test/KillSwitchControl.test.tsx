import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { KillSwitchControl } from "../panels/KillSwitchControl";

vi.mock("../store/cockpitStore", () => ({
  useCockpitStore: vi.fn(),
}));

vi.mock("../lib/format", () => ({
  tsAgo: (_ts: number) => "0s ago",
}));

// Mock Panel
vi.mock("../components/Panel", () => ({
  Panel: ({ title, children, status }: { title: string; children: React.ReactNode; status?: React.ReactNode }) => (
    <div data-testid="panel">
      <h2>{title}</h2>
      {status && <div>{status}</div>}
      {children}
    </div>
  ),
}));

import { useCockpitStore, type CockpitState } from "../store/cockpitStore";
const mockUseCockpitStore = vi.mocked(useCockpitStore);

describe("KillSwitchControl", () => {
  const sendIntent = vi.fn().mockReturnValue(true);

  beforeEach(() => {
    vi.clearAllMocks();
    mockUseCockpitStore.mockImplementation((selector: (state: CockpitState) => unknown) => {
      return selector({
        risk: { killswitch: { active: false, reason: null, ts_ns: 0 } },
      }) as ReturnType<typeof selector>;
    });
  });

  it("renders kill switch panel", () => {
    render(<KillSwitchControl sendIntent={sendIntent} />);
    expect(screen.getByText("Kill Switch")).toBeInTheDocument();
  });

  it("shows ARMED state by default", () => {
    render(<KillSwitchControl sendIntent={sendIntent} />);
    expect(screen.getByText("ARMED")).toBeInTheDocument();
  });

  it("shows ENGAGED when kill switch is active", () => {
    mockUseCockpitStore.mockImplementation((selector: (state: CockpitState) => unknown) => {
      return selector({
        risk: { killswitch: { active: true, reason: "test", ts_ns: 1000 } },
      }) as ReturnType<typeof selector>;
    });
    render(<KillSwitchControl sendIntent={sendIntent} />);
    expect(screen.getByText(/ENGAGED/)).toBeInTheDocument();
  });

  it("clicking engage sends killswitch intent", () => {
    window.confirm = vi.fn().mockReturnValue(true);
    render(<KillSwitchControl sendIntent={sendIntent} />);
    const btn = screen.getByRole("button", { name: /engage kill-switch/i });
    fireEvent.click(btn);
    expect(window.confirm).toHaveBeenCalled();
    expect(sendIntent).toHaveBeenCalledWith(
      expect.objectContaining({ kind: "killswitch", engaged: true })
    );
  });

  it("canceling confirm does not send intent", () => {
    window.confirm = vi.fn().mockReturnValue(false);
    render(<KillSwitchControl sendIntent={sendIntent} />);
    const btn = screen.getByRole("button", { name: /engage kill-switch/i });
    fireEvent.click(btn);
    expect(sendIntent).not.toHaveBeenCalled();
  });
});
