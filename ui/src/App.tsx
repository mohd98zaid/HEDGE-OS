// PROJECT HEDGE Human_Control_UI — placeholder shell.
// Concrete panels (Live Market, Orderflow Heatmap, Risk Panel, Latency
// Dashboard, ...) land in tasks E2/E3.

export default function App() {
  return (
    <main className="min-h-screen bg-hedge-bg text-slate-100 font-mono p-8">
      <header className="mb-8 border-b border-slate-800 pb-4">
        <h1 className="text-3xl font-semibold tracking-tight">
          PROJECT <span className="text-hedge-accent">HEDGE</span>
        </h1>
        <p className="text-slate-400 text-sm mt-1">
          Human_Control_UI — scaffolding shell. WebSocket cockpit lands in task E2.
        </p>
      </header>

      <section className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {[
          "Live Market",
          "Orderflow Heatmap",
          "Positions",
          "Risk Panel",
          "AI Confidence",
          "Latency Dashboard",
        ].map((panel) => (
          <div
            key={panel}
            className="bg-hedge-panel rounded-lg border border-slate-800 p-4 min-h-[120px]"
          >
            <div className="text-xs uppercase tracking-wider text-slate-500">
              panel
            </div>
            <div className="text-lg mt-1">{panel}</div>
            <div className="text-slate-500 text-xs mt-3">
              wired in task E2 / E3
            </div>
          </div>
        ))}
      </section>
    </main>
  );
}
