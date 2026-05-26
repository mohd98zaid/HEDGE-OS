//! `SessionController` — IST clock observer that emits the
//! `ops.session.start` / `ops.session.end` transitions per design
//! § Configuration Surface and Defaults and Components § Risk_Engine
//! (R31.1, R31.2, R31.3, R31.4).
//!
//! ## Responsibilities
//!
//! * Observe the IST wall clock and, on each Trading_Session day, fire
//!   `ops.session.start` at `session.start_ist` (default `09:15:00`) and
//!   `ops.session.end` at `session.end_ist` (default `15:30:00`).
//! * Be **edge-triggered** with **idempotent emission**: the count of
//!   emitted `start`/`end` events equals the count of distinct
//!   session-active transitions in any sample of wall-clock time, so a
//!   clock-skew double-fire never produces a duplicate (Property 8).
//! * Carry the `SessionId` derived from the IST date as `YYYYMMDD`
//!   packed into a `u64`, matching the convention documented on
//!   [`hedge_core::SessionId`].
//!
//! ## Relationship to the session-time gate (R31.1)
//!
//! The Risk_Engine's session-time gate is implemented locally inside
//! [`hedge_risk::RiskEngine::evaluate`] — it consults the IST wall
//! clock against [`hedge_config::SessionConfig`] and rejects with
//! `Rejected { reason: SessionClosed }` outside `[start_ist, end_ist]`.
//! That gate is independent of this controller's emissions: an isolated
//! Risk_Engine without a connected `hedge-session` still rejects orders
//! correctly. This controller's responsibility is to **announce** the
//! boundary on the bus so other Hot_Path components and the UI can
//! react edge-triggered (e.g. UI banner switch, Previous_Day_Memory_Engine
//! pre-session compute job, R31.4's session-end cancel request).
//!
//! ## State machine
//!
//! ```text
//!     ┌────────┐  reach start_ist   ┌────────┐  reach end_ist     ┌────────┐
//!     │Inactive│ ──────────────────▶│ Active │ ──────────────────▶│Inactive│
//!     └────────┘                    └────────┘                    └────────┘
//!         ▲                                                            │
//!         └────────────── advance to next trading day ─────────────────┘
//! ```
//!
//! Transitions are evaluated by sampling the IST clock once per
//! scheduled deadline. There is **no polling loop** — the controller
//! computes the next deadline (start, end, or next-day-start) up front
//! and awaits a single one-shot tokio `sleep_until` per transition. The
//! `sleep_until` call is annotated with the workspace's
//! `hedge-allow: polling-loop` marker because the no-polling CI rule is
//! for steady-state busy loops, not for one-shot deadlines on a
//! state-machine edge (R30.3).
//!
//! ## Test hooks
//!
//! Two abstractions allow the controller to be exercised in unit tests
//! without a live NATS connection or a real system clock:
//!
//! * [`crate::WallClock`] — returns the current `DateTime<Utc>`.
//!   Production uses [`crate::SystemWallClock`]; tests substitute a
//!   deterministic fake. The trait is shared with
//!   [`crate::WarModeController`] so a single test clock can drive both
//!   controllers in integration-style tests.
//! * [`SessionEventPublisher`] — publishes a typed [`SessionEvent`].
//!   Production uses [`crate::publisher::NatsSessionEventPublisher`];
//!   tests substitute an in-memory implementation.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::Asia::Kolkata;
use chrono_tz::Tz;
use hedge_bus::BusError;
use hedge_config::SessionConfig;
use hedge_core::{now_ns, SessionId};
use tokio::time::Instant as TokioInstant;
use tracing::{info, instrument, warn};

use crate::controller::WallClock;
use crate::session_event::{SessionEvent, SessionPhase};

// ---------------------------------------------------------------------------
// Ops-event publisher abstraction -------------------------------------------
// ---------------------------------------------------------------------------

/// Sink for `ops.session.<phase>` events.
///
/// The trait is intentionally narrow: a single typed publish call. Tests
/// implement a vector-backed fake; production uses
/// [`crate::publisher::NatsSessionEventPublisher`] which composes typed
/// `Subject<SessionEvent>` + `JsonCodec<SessionEvent>` over an
/// [`hedge_bus::NatsClient`].
#[async_trait]
pub trait SessionEventPublisher: Send + Sync + 'static {
    /// Publish a `SessionEvent` on the subject corresponding to its
    /// [`SessionPhase`] (`ops.session.start` for `Start`,
    /// `ops.session.end` for `End`).
    async fn publish_session(&self, event: &SessionEvent) -> Result<(), BusError>;
}

// ---------------------------------------------------------------------------
// State machine -------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Internal Trading_Session state — exposed only for testing.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SessionState {
    /// Trading_Session window has not started yet (or already ended for
    /// the day).
    Inactive,
    /// Trading_Session window is currently active.
    Active,
}

/// Schedule for one Trading_Session day, expressed as wall-clock UTC
/// instants computed from the IST `start_ist` / `end_ist` clock times.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SessionDaySchedule {
    /// IST date this schedule applies to (used as the `SessionId` source).
    pub ist_date: NaiveDate,
    /// `start_ist` of this day, expressed in UTC.
    pub start_utc: DateTime<Utc>,
    /// `end_ist` of this day, expressed in UTC.
    pub end_utc: DateTime<Utc>,
}

impl SessionDaySchedule {
    /// Build the schedule for `ist_date` from a `SessionConfig`.
    ///
    /// The IST timezone (`Asia/Kolkata`) is fixed UTC+05:30 with no
    /// DST, so [`Tz::from_local_datetime`] always returns a single
    /// mapping for the same local civil time. We still defensively
    /// unwrap with `.single()` and fall back to `.earliest()` when an
    /// exotic edge case (an invalid `NaiveTime` constructed by a future
    /// change) renders the local time ambiguous.
    pub fn for_date(date: NaiveDate, cfg: &SessionConfig) -> Self {
        let start_utc = ist_to_utc(date, cfg.start_ist);
        let end_utc = ist_to_utc(date, cfg.end_ist);
        Self {
            ist_date: date,
            start_utc,
            end_utc,
        }
    }

    /// `SessionId` derived from the IST date as `YYYYMMDD` packed in u64.
    /// Matches the convention documented on
    /// [`hedge_core::SessionId`] (date-derived monotonic counter).
    #[inline]
    pub fn session_id(&self) -> SessionId {
        let y = self.ist_date.year() as u64;
        let m = self.ist_date.month() as u64;
        let d = self.ist_date.day() as u64;
        SessionId::new(y * 10_000 + m * 100 + d)
    }
}

/// Convert an IST date + civil time into the corresponding UTC instant.
///
/// `Asia/Kolkata` is UTC+05:30 with no DST — every local civil time is
/// unique. We pull the result via `.single()`, falling back to
/// `.earliest()` if a future change ever puts us on a DST-bearing zone.
fn ist_to_utc(date: NaiveDate, time: NaiveTime) -> DateTime<Utc> {
    let local = date.and_time(time);
    let ist_dt = match Kolkata.from_local_datetime(&local) {
        chrono::LocalResult::Single(dt) => dt,
        // Defensive: IST has no DST today, but if the schedule moves to
        // a timezone that does we prefer the earliest interpretation
        // (consistent with how Self_Healing_Supervisor handles the
        // session boundary in §29.6).
        chrono::LocalResult::Ambiguous(early, _late) => early,
        chrono::LocalResult::None => {
            // Genuinely impossible for IST today; we fall back to a
            // direct conversion via the fixed +05:30 offset so the
            // controller never aborts on a surprising config value.
            let fixed = chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60)
                .expect("IST offset is well-known");
            return fixed
                .from_local_datetime(&local)
                .single()
                .expect("fixed-offset timezone is unambiguous")
                .with_timezone(&Utc);
        }
    };
    ist_dt.with_timezone(&Utc)
}

// ---------------------------------------------------------------------------
// Controller --------------------------------------------------------------
// ---------------------------------------------------------------------------

/// Owns the Trading_Session state machine and one tokio task.
///
/// Construct with a [`SessionConfig`], a [`WallClock`], and a
/// [`SessionEventPublisher`]; call [`SessionController::run`] from a
/// `spawn` to take over the task. The future returns once the wall
/// clock advances past `i64::MAX` nanoseconds — i.e. essentially never
/// under realistic operating conditions; tests that need a bounded run
/// use the [`SessionController::tick_until`] helper.
pub struct SessionController<C, P>
where
    C: WallClock,
    P: SessionEventPublisher,
{
    cfg: SessionConfig,
    clock: Arc<C>,
    publisher: Arc<P>,
    state: SessionState,
}

impl<C, P> SessionController<C, P>
where
    C: WallClock,
    P: SessionEventPublisher,
{
    /// Construct a new controller in the [`SessionState::Inactive`]
    /// state. The first call to `run` (or `tick_until`) will reconcile
    /// against the wall clock and either fire a `start` (if the clock
    /// is already inside today's window) or schedule the next start.
    pub fn new(cfg: SessionConfig, clock: Arc<C>, publisher: Arc<P>) -> Self {
        Self {
            cfg,
            clock,
            publisher,
            state: SessionState::Inactive,
        }
    }

    /// Borrow the current state — exposed for tests and operator
    /// observability.
    #[inline]
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Run forever, sleeping until the next state-machine deadline and
    /// firing the corresponding event on arrival.
    ///
    /// **No steady-state polling.** Each iteration computes a single
    /// deadline ([`Self::next_deadline`]) and awaits exactly one
    /// one-shot tokio `sleep_until` for it; every awakening
    /// corresponds to a real state-machine edge (`start` reached,
    /// `end` reached, or next-day-start reached), so emission counts
    /// equal the count of distinct window transitions in any sampled
    /// wall-clock window (Property 8). The `sleep_until` call carries
    /// the documented `hedge-allow: polling-loop` marker because the
    /// no-polling CI rule applies to steady-state busy loops, not
    /// one-shot deadlines on a state-machine edge.
    #[instrument(level = "info", skip(self), fields(start_ist = %self.cfg.start_ist, end_ist = %self.cfg.end_ist))]
    pub async fn run(mut self) -> Result<(), BusError> {
        loop {
            // Reconcile current state against the wall clock; this
            // fires a `start` immediately if we joined the bus
            // mid-window.
            self.reconcile_now().await?;

            // Compute next deadline and sleep until it arrives.
            let now_utc = self.clock.now_utc();
            let next = self.next_deadline(now_utc);
            sleep_until_utc(now_utc, next).await;
        }
    }

    /// Drive the controller across a fixed window — `[from, to)` UTC —
    /// without sleeping. Used by tests to assert the emission count
    /// without a real timer.
    ///
    /// The implementation walks the wall clock forward by visiting
    /// every scheduled transition that falls inside `[from, to)`, in
    /// order, and firing the corresponding event. Idempotency is
    /// preserved by the state machine itself.
    pub async fn tick_until(
        &mut self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<(), BusError> {
        // First reconcile at `from`.
        self.reconcile_at(from).await?;

        // Then walk forward through every scheduled transition.
        let mut cursor = from;
        loop {
            let next = self.next_deadline(cursor);
            if next >= to {
                return Ok(());
            }
            cursor = next;
            self.reconcile_at(cursor).await?;
        }
    }

    /// Reconcile the state machine against the wall clock as observed
    /// by the configured [`WallClock`].
    async fn reconcile_now(&mut self) -> Result<(), BusError> {
        let now = self.clock.now_utc();
        self.reconcile_at(now).await
    }

    /// Reconcile the state machine against an explicit UTC instant.
    /// Internal: this is the single point at which transitions are
    /// emitted so the idempotency guard sits in one place.
    async fn reconcile_at(&mut self, now: DateTime<Utc>) -> Result<(), BusError> {
        let day = self.day_for(now);
        let in_window = day.start_utc <= now && now < day.end_utc;
        match (self.state, in_window) {
            // Transition into the active window.
            (SessionState::Inactive, true) => {
                self.emit(SessionPhase::Start, day.session_id()).await?;
                self.state = SessionState::Active;
            }
            // Transition out of the active window.
            (SessionState::Active, false) => {
                self.emit(SessionPhase::End, day.session_id()).await?;
                self.state = SessionState::Inactive;
            }
            // Inside the window already and still active — do nothing
            // (idempotency: a second wake-up with the same boolean
            // state does NOT re-emit).
            (SessionState::Active, true) => {}
            // Outside the window and inactive — do nothing.
            (SessionState::Inactive, false) => {}
        }
        Ok(())
    }

    /// Emit the typed event with a monotonic timestamp, routing through
    /// the [`SessionEventPublisher`].
    async fn emit(&self, phase: SessionPhase, session_id: SessionId) -> Result<(), BusError> {
        let ts_ns = now_ns();
        let event = match phase {
            SessionPhase::Start => SessionEvent::start(session_id, ts_ns),
            SessionPhase::End => SessionEvent::end(session_id, ts_ns),
        };
        match self.publisher.publish_session(&event).await {
            Ok(()) => {
                info!(
                    target: "hedge_session::session",
                    session_id = session_id.raw(),
                    phase = phase.as_str(),
                    "trading session transition emitted"
                );
                Ok(())
            }
            Err(err) => {
                warn!(
                    target: "hedge_session::session",
                    session_id = session_id.raw(),
                    phase = phase.as_str(),
                    error = %err,
                    "trading session transition publish failed"
                );
                Err(err)
            }
        }
    }

    /// Compute the next deadline relative to `now`.
    ///
    /// Strategy: build today's IST schedule and pick whichever of
    /// `start_utc` / `end_utc` is strictly in the future, or roll
    /// forward to tomorrow's start.
    fn next_deadline(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let today = self.day_for(now);
        if now < today.start_utc {
            today.start_utc
        } else if now < today.end_utc {
            today.end_utc
        } else {
            // Window has closed for today; the next deadline is
            // tomorrow's start.
            let tomorrow_ist = today.ist_date + Duration::days(1);
            SessionDaySchedule::for_date(tomorrow_ist, &self.cfg).start_utc
        }
    }

    /// The IST-date schedule containing `now`. We compute the IST date
    /// once and reuse it on both the start and end branches so an
    /// observer that crosses midnight does not see a one-off schedule
    /// mismatch.
    fn day_for(&self, now: DateTime<Utc>) -> SessionDaySchedule {
        let ist: DateTime<Tz> = now.with_timezone(&Kolkata);
        SessionDaySchedule::for_date(ist.date_naive(), &self.cfg)
    }
}

/// Sleep until `target` UTC arrives.
///
/// We translate the UTC instant into a `tokio::time::Instant` by
/// computing the duration from `now` to `target` and adding it to
/// `tokio::time::Instant::now()`. If `target` is in the past we return
/// immediately so the caller's loop can move on to the next deadline.
async fn sleep_until_utc(now: DateTime<Utc>, target: DateTime<Utc>) {
    let Ok(delta) = (target - now).to_std() else {
        // Target is in the past; do not sleep.
        return;
    };
    let deadline = TokioInstant::now() + delta;
    tokio::time::sleep_until(deadline).await; // hedge-allow: polling-loop
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_config::defaults;
    use std::sync::Mutex;

    /// In-memory [`SessionEventPublisher`] used in unit tests.
    #[derive(Default)]
    struct FakePublisher {
        events: Mutex<Vec<SessionEvent>>,
    }

    impl FakePublisher {
        fn snapshot(&self) -> Vec<SessionEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SessionEventPublisher for FakePublisher {
        async fn publish_session(&self, event: &SessionEvent) -> Result<(), BusError> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    /// Deterministic [`WallClock`] returning a fixed instant.
    struct FrozenClock(DateTime<Utc>);

    impl WallClock for FrozenClock {
        fn now_utc(&self) -> DateTime<Utc> {
            self.0
        }
    }

    /// Helper: a `SessionConfig` with the design defaults
    /// (09:15:00–15:30:00 IST).
    fn cfg() -> SessionConfig {
        defaults::session()
    }

    /// Helper: build a UTC instant for an IST civil time on `ist_date`.
    fn ist_at(year: i32, month: u32, day: u32, h: u32, m: u32, s: u32) -> DateTime<Utc> {
        let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
        let time = NaiveTime::from_hms_opt(h, m, s).unwrap();
        ist_to_utc(date, time)
    }

    #[test]
    fn ist_to_utc_subtracts_five_thirty_for_a_known_day() {
        // 2025-11-30 09:15:00 IST  ==  2025-11-30 03:45:00 UTC
        let utc = ist_at(2025, 11, 30, 9, 15, 0);
        let civil = utc.format("%Y-%m-%d %H:%M:%S").to_string();
        assert_eq!(civil, "2025-11-30 03:45:00");

        // 2025-11-30 15:30:00 IST  ==  2025-11-30 10:00:00 UTC
        let utc = ist_at(2025, 11, 30, 15, 30, 0);
        let civil = utc.format("%Y-%m-%d %H:%M:%S").to_string();
        assert_eq!(civil, "2025-11-30 10:00:00");
    }

    #[test]
    fn day_schedule_session_id_is_yyyymmdd() {
        let day =
            SessionDaySchedule::for_date(NaiveDate::from_ymd_opt(2025, 11, 30).unwrap(), &cfg());
        assert_eq!(day.session_id().raw(), 20_251_130);
    }

    #[test]
    fn day_schedule_start_and_end_are_in_correct_utc_order() {
        let day =
            SessionDaySchedule::for_date(NaiveDate::from_ymd_opt(2025, 11, 30).unwrap(), &cfg());
        assert!(day.start_utc < day.end_utc);
    }

    #[test]
    fn day_schedule_window_spans_six_hours_fifteen_minutes() {
        // 09:15:00 → 15:30:00 IST is a 6 h 15 m window.
        let day =
            SessionDaySchedule::for_date(NaiveDate::from_ymd_opt(2025, 11, 30).unwrap(), &cfg());
        let span = day.end_utc - day.start_utc;
        assert_eq!(span, Duration::hours(6) + Duration::minutes(15));
    }

    #[tokio::test]
    async fn before_start_inactive_no_emission() {
        let now = ist_at(2025, 11, 30, 9, 14, 0);
        let pub_ = Arc::new(FakePublisher::default());
        let mut ctrl =
            SessionController::new(cfg(), Arc::new(FrozenClock(now)), Arc::clone(&pub_));
        ctrl.reconcile_at(now).await.unwrap();
        assert_eq!(ctrl.state(), SessionState::Inactive);
        assert_eq!(pub_.snapshot().len(), 0);
    }

    #[tokio::test]
    async fn at_start_fires_start_exactly_once() {
        let pub_ = Arc::new(FakePublisher::default());
        let mut ctrl = SessionController::new(
            cfg(),
            Arc::new(FrozenClock(ist_at(2025, 11, 30, 9, 14, 59))),
            Arc::clone(&pub_),
        );
        // Reconcile right before the window — no emission.
        ctrl.reconcile_at(ist_at(2025, 11, 30, 9, 14, 59))
            .await
            .unwrap();
        assert_eq!(pub_.snapshot().len(), 0);

        // Step into the window — fires `start` once.
        ctrl.reconcile_at(ist_at(2025, 11, 30, 9, 15, 0))
            .await
            .unwrap();
        let events = pub_.snapshot();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].phase, SessionPhase::Start);
        assert_eq!(events[0].session_id, 20_251_130);
        assert_eq!(ctrl.state(), SessionState::Active);

        // Idempotency: re-entering the same window state does NOT
        // re-emit. This is Property 8 in miniature.
        ctrl.reconcile_at(ist_at(2025, 11, 30, 9, 16, 0))
            .await
            .unwrap();
        ctrl.reconcile_at(ist_at(2025, 11, 30, 12, 0, 0))
            .await
            .unwrap();
        assert_eq!(pub_.snapshot().len(), 1, "no extra emissions while active");
    }

    #[tokio::test]
    async fn at_end_fires_end_exactly_once() {
        let pub_ = Arc::new(FakePublisher::default());
        let mut ctrl = SessionController::new(
            cfg(),
            Arc::new(FrozenClock(ist_at(2025, 11, 30, 9, 30, 0))),
            Arc::clone(&pub_),
        );
        // Walk through start, mid-window, then past the end.
        ctrl.reconcile_at(ist_at(2025, 11, 30, 9, 30, 0))
            .await
            .unwrap();
        ctrl.reconcile_at(ist_at(2025, 11, 30, 15, 30, 0))
            .await
            .unwrap();

        let events = pub_.snapshot();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].phase, SessionPhase::Start);
        assert_eq!(events[1].phase, SessionPhase::End);
        assert_eq!(ctrl.state(), SessionState::Inactive);

        // Idempotency: another reconcile after the window stays
        // Inactive and emits nothing.
        ctrl.reconcile_at(ist_at(2025, 11, 30, 16, 0, 0))
            .await
            .unwrap();
        assert_eq!(pub_.snapshot().len(), 2);
    }

    #[tokio::test]
    async fn next_day_window_emits_start_again() {
        let pub_ = Arc::new(FakePublisher::default());
        let mut ctrl = SessionController::new(
            cfg(),
            Arc::new(FrozenClock(ist_at(2025, 11, 30, 9, 30, 0))),
            Arc::clone(&pub_),
        );
        ctrl.reconcile_at(ist_at(2025, 11, 30, 9, 30, 0))
            .await
            .unwrap(); // start day 1
        ctrl.reconcile_at(ist_at(2025, 11, 30, 15, 30, 0))
            .await
            .unwrap(); // end day 1
        ctrl.reconcile_at(ist_at(2025, 12, 1, 9, 15, 0))
            .await
            .unwrap(); // start day 2

        let events = pub_.snapshot();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].phase, SessionPhase::Start);
        assert_eq!(events[0].session_id, 20_251_130);
        assert_eq!(events[1].phase, SessionPhase::End);
        assert_eq!(events[1].session_id, 20_251_130);
        assert_eq!(events[2].phase, SessionPhase::Start);
        assert_eq!(events[2].session_id, 20_251_201);
    }

    #[tokio::test]
    async fn join_mid_window_fires_start_immediately() {
        // Property 8 corollary: a controller spun up at 12:00:00 IST
        // while the window is already open emits `start` on first
        // reconcile and then `end` once at 15:30:00.
        let pub_ = Arc::new(FakePublisher::default());
        let mut ctrl = SessionController::new(
            cfg(),
            Arc::new(FrozenClock(ist_at(2025, 11, 30, 12, 0, 0))),
            Arc::clone(&pub_),
        );
        ctrl.reconcile_at(ist_at(2025, 11, 30, 12, 0, 0))
            .await
            .unwrap();
        assert_eq!(ctrl.state(), SessionState::Active);
        assert_eq!(pub_.snapshot().len(), 1);
        assert_eq!(pub_.snapshot()[0].phase, SessionPhase::Start);
    }

    #[tokio::test]
    async fn tick_until_walks_a_two_day_window_and_emits_four_events() {
        let pub_ = Arc::new(FakePublisher::default());
        let mut ctrl = SessionController::new(
            cfg(),
            Arc::new(FrozenClock(ist_at(2025, 11, 30, 0, 0, 0))),
            Arc::clone(&pub_),
        );
        ctrl.tick_until(
            ist_at(2025, 11, 30, 0, 0, 0),
            ist_at(2025, 12, 2, 0, 0, 0),
        )
        .await
        .unwrap();

        // Two start/end pairs.
        let events = pub_.snapshot();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].phase, SessionPhase::Start);
        assert_eq!(events[1].phase, SessionPhase::End);
        assert_eq!(events[2].phase, SessionPhase::Start);
        assert_eq!(events[3].phase, SessionPhase::End);
    }

    #[tokio::test]
    async fn double_reconcile_at_same_instant_is_idempotent() {
        // Defends against clock-skew double-fire (Property 8): calling
        // `reconcile_at` twice with the same `now` produces exactly the
        // same event count as calling it once.
        let pub_ = Arc::new(FakePublisher::default());
        let mut ctrl = SessionController::new(
            cfg(),
            Arc::new(FrozenClock(ist_at(2025, 11, 30, 9, 15, 0))),
            Arc::clone(&pub_),
        );
        let t = ist_at(2025, 11, 30, 9, 15, 0);
        ctrl.reconcile_at(t).await.unwrap();
        ctrl.reconcile_at(t).await.unwrap();
        ctrl.reconcile_at(t).await.unwrap();
        assert_eq!(pub_.snapshot().len(), 1);
    }

    #[test]
    fn next_deadline_chooses_today_start_then_today_end_then_tomorrow_start() {
        let pub_ = Arc::new(FakePublisher::default());
        let ctrl = SessionController::new(
            cfg(),
            Arc::new(FrozenClock(ist_at(2025, 11, 30, 0, 0, 0))),
            Arc::clone(&pub_),
        );

        // Before today's start.
        let now = ist_at(2025, 11, 30, 0, 0, 0);
        assert_eq!(ctrl.next_deadline(now), ist_at(2025, 11, 30, 9, 15, 0));

        // Inside the window.
        let now = ist_at(2025, 11, 30, 12, 0, 0);
        assert_eq!(ctrl.next_deadline(now), ist_at(2025, 11, 30, 15, 30, 0));

        // After today's end.
        let now = ist_at(2025, 11, 30, 18, 0, 0);
        assert_eq!(ctrl.next_deadline(now), ist_at(2025, 12, 1, 9, 15, 0));
    }

    #[tokio::test]
    async fn property8_emission_count_equals_transition_count_over_random_walk() {
        // Mini Property-8-shaped test: walk a stream of timestamps that
        // cross the window edge multiple times across two days, and
        // assert that the emission count equals the count of distinct
        // session-active transitions.
        let pub_ = Arc::new(FakePublisher::default());
        let mut ctrl = SessionController::new(
            cfg(),
            Arc::new(FrozenClock(ist_at(2025, 11, 30, 0, 0, 0))),
            Arc::clone(&pub_),
        );

        // Hand-rolled stream: many points before the window, several
        // points inside, several after, then re-enter on the next day.
        let stream = [
            ist_at(2025, 11, 30, 0, 0, 0),
            ist_at(2025, 11, 30, 9, 0, 0),
            ist_at(2025, 11, 30, 9, 14, 59),
            ist_at(2025, 11, 30, 9, 15, 0), // start day 1
            ist_at(2025, 11, 30, 10, 0, 0),
            ist_at(2025, 11, 30, 12, 30, 0),
            ist_at(2025, 11, 30, 15, 29, 59),
            ist_at(2025, 11, 30, 15, 30, 0), // end day 1
            ist_at(2025, 11, 30, 16, 0, 0),
            ist_at(2025, 11, 30, 23, 59, 59),
            ist_at(2025, 12, 1, 9, 14, 59),
            ist_at(2025, 12, 1, 9, 15, 0),  // start day 2
            ist_at(2025, 12, 1, 12, 0, 0),
            ist_at(2025, 12, 1, 15, 30, 0), // end day 2
            ist_at(2025, 12, 1, 18, 0, 0),
        ];

        // Count distinct transitions via the same boolean test the
        // controller uses internally.
        let mut transitions = 0_usize;
        let mut prev_active: Option<bool> = None;
        for &t in &stream {
            let day = SessionDaySchedule::for_date(
                t.with_timezone(&Kolkata).date_naive(),
                &cfg(),
            );
            let in_window = day.start_utc <= t && t < day.end_utc;
            if let Some(prev) = prev_active {
                if prev != in_window {
                    transitions += 1;
                }
            }
            prev_active = Some(in_window);
            ctrl.reconcile_at(t).await.unwrap();
        }
        assert_eq!(pub_.snapshot().len(), transitions);
        assert_eq!(transitions, 4); // start, end, start, end
    }

    #[tokio::test]
    async fn publisher_receives_payload_with_session_id_and_ts_ns() {
        let pub_ = Arc::new(FakePublisher::default());
        let mut ctrl = SessionController::new(
            cfg(),
            Arc::new(FrozenClock(ist_at(2025, 11, 30, 9, 14, 59))),
            Arc::clone(&pub_),
        );
        ctrl.reconcile_at(ist_at(2025, 11, 30, 9, 15, 0))
            .await
            .unwrap();
        let events = pub_.snapshot();
        let ev = &events[0];
        // session_id is the IST-date YYYYMMDD packed into u64.
        assert_eq!(ev.session_id, 20_251_130);
        // ts_ns is monotonic with respect to `hedge_core::now_ns`; we
        // only verify it is a real counter (not zero) here. Property 8
        // in the PBT suite covers monotonicity end-to-end.
        assert!(ev.ts_ns > 0);
    }

    /// `SessionConfig` with a non-default window — a config-time knob
    /// the operator might set for half-day Saturday sessions or
    /// dry-run windows. The controller honours whatever window the
    /// config carries.
    #[tokio::test]
    async fn honours_custom_session_window_from_config() {
        use chrono::NaiveTime;
        let cfg = SessionConfig {
            start_ist: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            end_ist: NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
        };
        let pub_ = Arc::new(FakePublisher::default());
        let mut ctrl = SessionController::new(
            cfg,
            Arc::new(FrozenClock(ist_at(2025, 11, 30, 9, 59, 59))),
            Arc::clone(&pub_),
        );
        ctrl.reconcile_at(ist_at(2025, 11, 30, 9, 59, 59))
            .await
            .unwrap();
        assert_eq!(pub_.snapshot().len(), 0);
        ctrl.reconcile_at(ist_at(2025, 11, 30, 10, 0, 0))
            .await
            .unwrap();
        assert_eq!(pub_.snapshot().len(), 1);
        assert_eq!(pub_.snapshot()[0].phase, SessionPhase::Start);
        ctrl.reconcile_at(ist_at(2025, 11, 30, 14, 0, 0))
            .await
            .unwrap();
        let events = pub_.snapshot();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].phase, SessionPhase::End);
    }
}
