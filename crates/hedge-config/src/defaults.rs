//! Concrete default values mirroring design § Configuration Surface and
//! Defaults verbatim. Each function returns a fresh owned value to keep
//! `Default` impls trivially correct.

use chrono::NaiveTime;
use hedge_core::BrokerId;

use crate::models::{
    AiConfig, BrokerConfig, CapitalConfig, DegradedModeConfig, GovernanceConfig, HedgeConfig,
    ObservabilityConfig, OllamaConfig, OllamaModelConfig, OllamaRole, PostTargetPolicy,
    PsychologyThresholds, RankingFactorsConfig, RetentionConfig, RiskConfig, SessionConfig,
    TraderPsychologyConfig, UiConfig, WarModeConfig, WarmCacheConfig,
};

/// `09:15:00` IST.
pub fn ist_0915() -> NaiveTime {
    NaiveTime::from_hms_opt(9, 15, 0).expect("constant")
}

/// `09:45:00` IST.
pub fn ist_0945() -> NaiveTime {
    NaiveTime::from_hms_opt(9, 45, 0).expect("constant")
}

/// `15:30:00` IST.
pub fn ist_1530() -> NaiveTime {
    NaiveTime::from_hms_opt(15, 30, 0).expect("constant")
}

/// Capital defaults — R32.1, R32.2, R32.3.
pub fn capital() -> CapitalConfig {
    CapitalConfig {
        base_inr: 20_000,
        daily_profit_target_min_inr: 300,
        daily_profit_target_max_inr: 1_000,
        post_target_policy: PostTargetPolicy::ReduceSizeToZero,
    }
}

/// Risk defaults tuned to a ₹20,000 base (design YAML).
pub fn risk() -> RiskConfig {
    RiskConfig {
        max_daily_loss_inr: 600,
        max_position_per_symbol: 200,
        max_position_portfolio: 500,
        max_leverage_per_symbol: 5.0,
        max_leverage_account: 5.0,
        max_drawdown_inr: 1_000,
        max_trades_per_minute: 4,
        max_trades_per_hour: 30,
        max_trades_per_session: 60,
        max_exposure_per_symbol_inr: 20_000,
        max_exposure_per_sector_inr: 30_000,
        slippage_threshold_bps: 25,
        slippage_cooldown_ms: 60_000,
        volatility_block_threshold: 0.06,
        broker_latency_block_ms: 250,
        base_risk_per_trade_inr: 100,
    }
}

/// Session window defaults (R26.1).
pub fn session() -> SessionConfig {
    SessionConfig { start_ist: ist_0915(), end_ist: ist_1530() }
}

/// War_Mode window and gating defaults (R26.2, R26.3).
pub fn war_mode() -> WarModeConfig {
    WarModeConfig {
        start_ist: ist_0915(),
        end_ist: ist_0945(),
        min_confidence: 0.6,
        scan_multiplier: 2.0,
    }
}

/// UI defaults.
pub fn ui() -> UiConfig {
    UiConfig { high_vol_threshold: 0.05 }
}

/// AI defaults (R10, R17, R23).
pub fn ai() -> AiConfig {
    AiConfig {
        shadow_components: Vec::new(),
        governance: GovernanceConfig { drift_warn: 0.20, drift_critical: 0.35 },
        rank_p95_budget_ms: 5,
        ranking_factors: RankingFactorsConfig {
            orderflow: 0.30,
            technical_strength: 0.25,
            news_sentiment: 0.20,
            market_regime: 0.15,
            trader_discipline: 0.10,
        },
    }
}

/// Trader_Psychology defaults (R16).
pub fn trader_psychology() -> TraderPsychologyConfig {
    TraderPsychologyConfig {
        thresholds: PsychologyThresholds {
            warning: 0.6,
            cooldown: 0.5,
            suppression: 0.4,
            critical: 0.3,
        },
    }
}

/// Broker primary/backup defaults (R6.5, R7.1).
pub fn brokers() -> BrokerConfig {
    BrokerConfig {
        primary: BrokerId::Zerodha,
        backup: BrokerId::Dhan,
        failover_error_rate: 0.20,
        failover_latency_ms: 250,
    }
}

/// Ollama_Infrastructure defaults (R10).
pub fn ollama() -> OllamaConfig {
    OllamaConfig {
        models: vec![
            OllamaModelConfig {
                name: "qwen2.5:14b".to_string(),
                role: OllamaRole::Primary,
                quant: "q4_k_m".to_string(),
            },
            OllamaModelConfig {
                name: "mistral:7b".to_string(),
                role: OllamaRole::Fast,
                quant: "q4_k_m".to_string(),
            },
            OllamaModelConfig {
                name: "deepseek-r1".to_string(),
                role: OllamaRole::Deep,
                quant: "q4_k_m".to_string(),
            },
            OllamaModelConfig {
                name: "phi".to_string(),
                role: OllamaRole::Lightweight,
                quant: "q4_k_m".to_string(),
            },
        ],
    }
}

/// Observability defaults (R27, R28).
pub fn observability() -> ObservabilityConfig {
    ObservabilityConfig {
        retention: RetentionConfig { metrics_days: 30, logs_days: 14, traces_days: 7 },
        degraded_mode: DegradedModeConfig {
            drop_low_severity_logs_at_loki_unavailable: true,
            sample_traces_at_jaeger_overload: 0.1,
        },
    }
}

/// WarmCache defaults — design § Hot_Path Architecture (WarmCache).
pub fn warm_cache() -> WarmCacheConfig {
    WarmCacheConfig {
        trade_confidence_lru_size: 8_192,
        staleness_window_ms: 5_000,
        nats_url: "nats://127.0.0.1:4222".to_string(),
    }
}

/// Composed default `HedgeConfig` matching the design YAML exactly.
pub fn hedge_config() -> HedgeConfig {
    HedgeConfig {
        capital: capital(),
        risk: risk(),
        session: session(),
        war_mode: war_mode(),
        ui: ui(),
        ai: ai(),
        trader_psychology: trader_psychology(),
        brokers: brokers(),
        ollama: ollama(),
        observability: observability(),
        warm_cache: warm_cache(),
    }
}
