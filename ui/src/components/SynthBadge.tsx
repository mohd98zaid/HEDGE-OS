// SynthBadge — small "synth" pill rendered in a panel header when the most
// recent envelope on that panel's backing channel carried `_synth: true`.
// Drives full-cockpit-data REQ-13.

import { useCockpitStore } from "../store/cockpitStore";
import type { ChannelId } from "../types";

export function SynthBadge({ channel }: { channel: ChannelId }): JSX.Element | null {
  const synth = useCockpitStore((s) => s.meta.synthChannels[channel] === true);
  if (!synth) return null;
  return (
    <span
      className="ml-2 rounded-sm bg-violet-500/20 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wider text-violet-300"
      title="Synthetic data — produced by hedge-demo-synth, not a real publisher"
    >
      synth
    </span>
  );
}
