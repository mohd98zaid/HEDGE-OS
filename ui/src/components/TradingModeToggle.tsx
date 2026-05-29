// TradingModeToggle — live vs paper execution switch (full-cockpit-data
// safety control). Default OFF = paper (safe). Turning it ON requires an
// explicit confirm dialog because live mode places real broker orders.
//
// Design: Uiverse.io by Javierrocadev (padlock toggle), adapted to drive
// a `trading_mode` trader-intent and persist the choice in localStorage.
//
// Authority: the Execution_Engine is the source of truth. This toggle is
// optimistic; the engine echoes the confirmed mode on `exec.mode.confirmed`
// (a future store slice can reflect that). The UI never places orders
// itself — it only publishes the intent.

import { useCallback, useEffect, useState } from "react";

import type { TraderIntent } from "../types";

const LS_KEY = "hedge.cockpit.tradingMode.live";

export function TradingModeToggle({
  sendIntent,
}: {
  sendIntent: (intent: TraderIntent) => boolean;
}): JSX.Element {
  const [live, setLive] = useState<boolean>(() => {
    try {
      return localStorage.getItem(LS_KEY) === "1";
    } catch {
      return false;
    }
  });

  // On mount, broadcast the persisted mode so the engine and UI agree.
  useEffect(() => {
    sendIntent({ kind: "trading_mode", live });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const persist = useCallback((next: boolean) => {
    setLive(next);
    try {
      localStorage.setItem(LS_KEY, next ? "1" : "0");
    } catch {
      /* ignore storage failures */
    }
    sendIntent({ kind: "trading_mode", live: next });
  }, [sendIntent]);

  const onChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const wantLive = e.target.checked;
      if (wantLive) {
        // Explicit confirm — live mode places REAL orders.
        const ok = window.confirm(
          "Enable LIVE trading?\n\n" +
            "This routes approved orders to your real broker account and can " +
            "lose real money. Only enable this if you intend to trade live.\n\n" +
            "Click OK to go LIVE, or Cancel to stay in PAPER mode.",
        );
        if (!ok) {
          // Revert the checkbox visually.
          e.target.checked = false;
          return;
        }
      }
      persist(wantLive);
    },
    [persist],
  );

  return (
    <div className="flex items-center gap-2">
      <span
        className={`text-[11px] font-semibold uppercase tracking-wider ${
          live ? "text-emerald-400" : "text-slate-500"
        }`}
      >
        {live ? "LIVE" : "PAPER"}
      </span>
      {/* From Uiverse.io by Javierrocadev (padlock toggle) */}
      <label className="relative inline-flex items-center cursor-pointer" title={live ? "Live trading enabled — real orders" : "Paper trading — simulated orders"}>
        <input
          type="checkbox"
          checked={live}
          onChange={onChange}
          className="sr-only peer"
        />
        <div className="group peer ring-0 bg-gradient-to-r from-rose-400 to-red-900 rounded-full outline-none duration-700 after:duration-300 w-16 h-8 shadow-md peer-checked:bg-gradient-to-r peer-checked:from-emerald-500 peer-checked:to-emerald-900 peer-focus:outline-none after:content-[''] after:rounded-full after:absolute after:bg-gray-50 after:outline-none after:h-7 after:w-7 after:top-0.5 after:left-0.5 peer-checked:after:translate-x-8 peer-hover:after:scale-95">
          <svg
            className="group-hover:scale-75 duration-300 absolute top-0.5 left-8 stroke-gray-900 w-7 h-7"
            height="100"
            preserveAspectRatio="xMidYMid meet"
            viewBox="0 0 100 100"
            width="100"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              className="svg-fill-primary"
              d="M50,18A19.9,19.9,0,0,0,30,38v8a8,8,0,0,0-8,8V74a8,8,0,0,0,8,8H70a8,8,0,0,0,8-8V54a8,8,0,0,0-8-8H38V38a12,12,0,0,1,23.6-3,4,4,0,1,0,7.8-2A20.1,20.1,0,0,0,50,18Z"
            />
          </svg>
          <svg
            className="group-hover:scale-75 duration-300 absolute top-0.5 left-0.5 stroke-gray-900 w-7 h-7"
            height="100"
            preserveAspectRatio="xMidYMid meet"
            viewBox="0 0 100 100"
            width="100"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              d="M30,46V38a20,20,0,0,1,40,0v8a8,8,0,0,1,8,8V74a8,8,0,0,1-8,8H30a8,8,0,0,1-8-8V54A8,8,0,0,1,30,46Zm32-8v8H38V38a12,12,0,0,1,24,0Z"
              fillRule="evenodd"
            />
          </svg>
        </div>
      </label>
    </div>
  );
}
