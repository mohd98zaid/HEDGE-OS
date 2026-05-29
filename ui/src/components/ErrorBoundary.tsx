// ErrorBoundary — isolates a render crash to a single panel.
//
// Why this exists: the cockpit renders 16 independent panels off a live
// WebSocket store. Before this boundary, a single panel throwing during
// render (e.g. a malformed envelope reaching `.toFixed`/`.slice`/`.map`)
// unmounted the WHOLE React tree — the screen went blank and only a manual
// refresh brought it back, until the next bad frame blanked it again.
//
// React error boundaries must be class components (there is no hook
// equivalent for `componentDidCatch`). Each panel is wrapped in its own
// boundary so one failing panel degrades to a small inline error card while
// the other fifteen keep streaming. The boundary auto-retries on an interval
// so a transient bad frame recovers without user action.

import { Component, type ErrorInfo, type ReactNode } from "react";

interface ErrorBoundaryProps {
  /** Human-readable panel name shown in the fallback card. */
  name: string;
  /** Auto-retry delay in ms. The boundary clears its error state after this
   *  delay so a transient bad frame recovers on the next render. */
  retryMs?: number;
  children: ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
  message?: string;
}

export class ErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  private retryTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(error: unknown): ErrorBoundaryState {
    return {
      hasError: true,
      message: error instanceof Error ? error.message : String(error),
    };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // Log once so the crash is visible in the console without taking down
    // the app. Includes the panel name and component stack for triage.
    // eslint-disable-next-line no-console
    console.error(
      `[cockpit] panel "${this.props.name}" crashed during render:`,
      error,
      info.componentStack,
    );
    this.scheduleRetry();
  }

  componentWillUnmount(): void {
    if (this.retryTimer) clearTimeout(this.retryTimer);
  }

  private scheduleRetry(): void {
    if (this.retryTimer) clearTimeout(this.retryTimer);
    const delay = this.props.retryMs ?? 3000;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.setState({ hasError: false, message: undefined });
    }, delay);
  }

  private handleManualRetry = (): void => {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    this.setState({ hasError: false, message: undefined });
  };

  render(): ReactNode {
    if (this.state.hasError) {
      return (
        <section className="rounded-lg border border-hedge-danger/40 bg-hedge-panel p-4">
          <header className="mb-2 flex items-baseline justify-between">
            <h2 className="text-xs font-semibold uppercase tracking-wider text-hedge-danger">
              {this.props.name} · error
            </h2>
            <button
              type="button"
              onClick={this.handleManualRetry}
              className="text-[10px] font-mono text-slate-400 underline hover:text-slate-200"
            >
              retry
            </button>
          </header>
          <p className="text-[11px] text-slate-500">
            This panel hit a render error and was isolated so the rest of the
            cockpit keeps running. Auto-retrying…
          </p>
          {this.state.message ? (
            <p className="mt-1 font-mono text-[10px] text-slate-600 break-words">
              {this.state.message}
            </p>
          ) : null}
        </section>
      );
    }
    return this.props.children;
  }
}
