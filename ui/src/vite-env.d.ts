/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_HEDGE_GATEWAY_URL?: string;
  readonly VITE_UI_HIGH_VOL_THRESHOLD?: string;
  readonly VITE_UI_REFRESH_MS_NORMAL?: string;
  readonly VITE_UI_REFRESH_MS_HIGHVOL?: string;
  readonly VITE_UI_WAR_MODE_START_IST?: string;
  readonly VITE_UI_WAR_MODE_END_IST?: string;
  readonly VITE_UI_WAR_MODE_MIN_CONFIDENCE?: string;
  readonly VITE_UI_WAR_MODE_SCAN_MULTIPLIER?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
