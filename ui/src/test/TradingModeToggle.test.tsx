import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { TradingModeToggle } from "../components/TradingModeToggle";

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {};
  return {
    getItem: vi.fn((key: string) => store[key] ?? null),
    setItem: vi.fn((key: string, value: string) => { store[key] = value; }),
    removeItem: vi.fn((key: string) => { delete store[key]; }),
    clear: vi.fn(() => { store = {}; }),
  };
})();
Object.defineProperty(window, "localStorage", { value: localStorageMock });

describe("TradingModeToggle", () => {
  it("defaults to PAPER mode", () => {
    localStorageMock.getItem.mockReturnValue(null as unknown as string);
    const sendIntent = vi.fn().mockReturnValue(true);
    render(<TradingModeToggle sendIntent={sendIntent} />);
    expect(screen.getByText("PAPER")).toBeInTheDocument();
    expect(sendIntent).toHaveBeenCalledWith({ kind: "trading_mode", live: false });
  });

  it("shows LIVE when localStorage has live=1", () => {
    localStorageMock.getItem.mockReturnValue("1" as unknown as string);
    const sendIntent = vi.fn().mockReturnValue(true);
    render(<TradingModeToggle sendIntent={sendIntent} />);
    expect(screen.getByText("LIVE")).toBeInTheDocument();
  });

  it("toggling to live triggers confirm dialog", () => {
    localStorageMock.getItem.mockReturnValue(null as unknown as string);
    const sendIntent = vi.fn().mockReturnValue(true);
    window.confirm = vi.fn().mockReturnValue(true);
    render(<TradingModeToggle sendIntent={sendIntent} />);
    const checkbox = screen.getByRole("checkbox");
    fireEvent.click(checkbox);
    expect(window.confirm).toHaveBeenCalled();
    expect(sendIntent).toHaveBeenCalledWith({ kind: "trading_mode", live: true });
  });

  it("canceling confirm reverts to PAPER", () => {
    localStorageMock.getItem.mockReturnValue(null as unknown as string);
    const sendIntent = vi.fn().mockReturnValue(true);
    window.confirm = vi.fn().mockReturnValue(false);
    render(<TradingModeToggle sendIntent={sendIntent} />);
    const checkbox = screen.getByRole("checkbox");
    fireEvent.click(checkbox);
    expect(screen.getByText("PAPER")).toBeInTheDocument();
  });
});
