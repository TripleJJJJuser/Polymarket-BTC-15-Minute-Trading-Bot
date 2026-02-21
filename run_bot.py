"""
Complete BTC 15-Min Trading Bot - FIXED VERSION
- Uses time-based filtering (proven to work from test)
- $1 per trade maximum
- Reloads instruments every 12 minutes
- Pre-loads price history on startup
- Full P&L tracking in simulation
"""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
import math
from decimal import Decimal
import time
from dataclasses import dataclass
from typing import List, Optional, Deque
import random
import json
import urllib.request
from collections import deque

# Add project to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Apply patch BEFORE importing Nautilus
try:
    from patch_gamma_markets import apply_gamma_markets_patch, verify_patch
    patch_applied = apply_gamma_markets_patch()
    if patch_applied:
        verify_patch()
    else:
        print("ERROR: Failed to apply gamma_market patch")
        sys.exit(1)
except ImportError as e:
    print(f"ERROR: Could not import patch module: {e}")
    print("Make sure patch_gamma_markets.py is in the same directory")
    sys.exit(1)

# Now import Nautilus
from nautilus_trader.config import (
    InstrumentProviderConfig,
    LiveDataEngineConfig,
    LiveExecEngineConfig,
    LiveRiskEngineConfig,
    LoggingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.adapters.polymarket import POLYMARKET
from nautilus_trader.adapters.polymarket import (
    PolymarketDataClientConfig,
    PolymarketExecClientConfig,
)
from nautilus_trader.adapters.polymarket.factories import (
    PolymarketLiveDataClientFactory,
    PolymarketLiveExecClientFactory,
)
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.identifiers import InstrumentId, ClientOrderId
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.objects import Quantity, Price
from nautilus_trader.model.data import QuoteTick

from dotenv import load_dotenv
from loguru import logger
import redis

# Import our phases
from core.strategy_brain.signal_processors.spike_detector import SpikeDetectionProcessor
from core.strategy_brain.signal_processors.sentiment_processor import SentimentProcessor
from core.strategy_brain.signal_processors.divergence_processor import PriceDivergenceProcessor
from core.strategy_brain.fusion_engine.signal_fusion import get_fusion_engine
from execution.risk_engine import get_risk_engine
from monitoring.performance_tracker import get_performance_tracker
from monitoring.grafana_exporter import get_grafana_exporter
from feedback.learning_engine import get_learning_engine

load_dotenv()


@dataclass
class PaperTrade:
    """Track closed paper/simulation trades."""
    timestamp: datetime
    direction: str
    size_usd: float
    price: float
    signal_score: float
    signal_confidence: float
    outcome: str = "PENDING"

    def to_dict(self):
        return {
            'timestamp': self.timestamp.isoformat(),
            'direction': self.direction,
            'size_usd': self.size_usd,
            'price': self.price,
            'signal_score': self.signal_score,
            'signal_confidence': self.signal_confidence,
            'outcome': self.outcome,
        }


@dataclass
class PaperPosition:
    """Track an open paper position marked-to-market from live ticks."""
    trade_id: str
    entry_time: datetime
    direction: str
    instrument_id: InstrumentId
    size_usd: Decimal
    entry_mid: Decimal
    signal_score: float
    signal_confidence: float
    hold_until: datetime
    market_end_time: Optional[datetime] = None
    latest_mid: Optional[Decimal] = None
    unrealized_pnl_usd: Decimal = Decimal("0")


def init_redis():
    """Initialize Redis connection for simulation mode control."""
    try:
        redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 2)),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_keepalive=True
        )
        redis_client.ping()
        logger.info("Redis connection established")
        return redis_client
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}")
        logger.warning("Simulation mode will be static (from .env)")
        return None


class IntegratedBTCStrategy(Strategy):
    """
    Integrated BTC Strategy combining:
    - Nautilus trading framework
    - Our 7-phase system
    - Redis simulation control
    - Paper trading tracking
    - Auto-reload instruments every 12 minutes
    - Pre-loaded price history for immediate trading
    """
    
    def __init__(self, redis_client=None, enable_grafana=True, test_mode=False, simulation_mode=True, allow_redis_live_switch=False):
        super().__init__()
        
        # Nautilus
        self.instrument_id = None
        self.current_condition_id = None
        self.current_market_end_time = None
        self.up_instrument_id = None
        self.down_instrument_id = None
        self.redis_client = redis_client
        self.current_simulation_mode = simulation_mode
        self.allow_redis_live_switch = allow_redis_live_switch
        self.allow_synthetic_history = simulation_mode
        symbols_env = os.getenv("TRADE_SYMBOLS", "BTC,ETH,SOL,XRP")
        self.trade_symbols = [sym.strip().upper() for sym in symbols_env.split(",") if sym.strip()]
        self.selected_symbol = self.trade_symbols[0] if self.trade_symbols else "BTC"
        
        # Phase 4: Signal Processors
        self.spike_detector = SpikeDetectionProcessor(
            spike_threshold=float(os.getenv('SPIKE_THRESHOLD', 0.15)),
            lookback_periods=20,
        )
        self.sentiment_processor = SentimentProcessor(
            extreme_fear_threshold=25,
            extreme_greed_threshold=75,
        )
        self.divergence_processor = PriceDivergenceProcessor(
            divergence_threshold=0.05,
        )

        divergence_env = os.getenv("ENABLE_DIVERGENCE")
        if divergence_env is None:
            live_guard_active = os.getenv("LIVE_TRADING_ENABLED", "").strip() == "YES_I_UNDERSTAND"
            self.enable_divergence = simulation_mode and not live_guard_active
        else:
            self.enable_divergence = divergence_env.strip().lower() in {"1", "true", "yes", "on"}
        
        # Phase 4: Signal Fusion
        self.fusion_engine = get_fusion_engine()
        
        # Phase 5: Risk Management
        self.risk_engine = get_risk_engine()
        
        # Phase 6: Performance Tracking
        self.performance_tracker = get_performance_tracker()
        
        # Phase 7: Learning Engine
        self.learning_engine = get_learning_engine()
        
        # Phase 6: Grafana (optional)
        if enable_grafana:
            self.grafana_exporter = get_grafana_exporter()
        else:
            self.grafana_exporter = None
        
        # Price history for signal processing
        self.price_history = []  # current instrument convenience view
        self.max_history = 100
        self.price_history_by_instrument = {}
        self.real_quote_count = 0
        
        # Paper trading tracker
        self.paper_trades: List[PaperTrade] = []
        self.paper_positions: List[PaperPosition] = []
        
        # Last trading decision time (to prevent multiple trades per interval)
        self.last_trade_time = 0
        
        # Last instrument reload time
        self.last_reload_time = datetime.min.replace(tzinfo=timezone.utc)

        # Prevent log spam for fallback metadata warnings
        self._logged_simulation_fallback_warning = False

        # External context cache for higher-quality metadata (spot + sentiment)
        self._external_context_cache = {}
        self._last_external_refresh = datetime.min.replace(tzinfo=timezone.utc)
        self._external_refresh_seconds = int(os.getenv("EXTERNAL_CONTEXT_REFRESH_SEC", "120"))

        # Learning controls
        self._learning_interval_trades = int(os.getenv("LEARNING_INTERVAL_TRADES", "10"))

        # Execution quality controls
        self.max_spread_pct = float(os.getenv("MAX_SPREAD_PCT", "0.04"))
        self.max_slippage_pct = float(os.getenv("MAX_SLIPPAGE_PCT", "0.02"))
        self.min_fusion_score = float(os.getenv("MIN_FUSION_SCORE", "60"))
        self.min_fusion_confidence = float(os.getenv("MIN_FUSION_CONFIDENCE", "0.60"))

        self.latest_bid: Optional[Decimal] = None
        self.latest_ask: Optional[Decimal] = None
        self.decision_audit_file = os.getenv("DECISION_AUDIT_FILE", "decision_audit.jsonl")
        self.order_mode = os.getenv("ORDER_MODE", "smart_limit").strip().lower()

        # Live order hard/soft safety caps
        self.disable_live_orders = os.getenv("DISABLE_LIVE_ORDERS", "0").strip() == "1"
        self.max_orders_per_hour = int(os.getenv("MAX_ORDERS_PER_HOUR", "0"))
        self.max_daily_notional = Decimal(os.getenv("MAX_DAILY_NOTIONAL", "0"))
        self.live_order_timestamps: Deque[datetime] = deque()
        self.daily_notional_usd = Decimal("0")
        self.daily_notional_date = datetime.now(timezone.utc).date()

        self.market_recency_weight = float(os.getenv("MARKET_RECENCY_WEIGHT", "0.6"))
        self.market_spread_weight = float(os.getenv("MARKET_SPREAD_WEIGHT", "0.4"))

        self.test_mode = test_mode

        if test_mode:
            logger.info("=" * 80)
            logger.info("⚠️  TEST MODE ACTIVE - Trading every minute!")
            logger.info("=" * 80)
        
        logger.info("=" * 80)
        logger.info("INTEGRATED BTC STRATEGY INITIALIZED")
        logger.info("  Phase 4: Signal processors ready")
        logger.info("  Phase 5: Risk engine ready")
        logger.info("  Phase 6: Performance tracking ready")
        logger.info("  Phase 7: Learning engine ready")
        logger.info("  $1 per trade maximum")
        logger.info("  Reloads instruments every 12 minutes")
        logger.info(f"  Symbols: {', '.join(self.trade_symbols)}")
        logger.info(f"  Divergence: {'enabled' if self.enable_divergence else 'disabled'}")
        logger.info("=" * 80)
    
    async def check_simulation_mode(self) -> bool:
        """Check Redis for current simulation mode."""
        if not self.redis_client:
            return self.current_simulation_mode
        
        try:
            sim_mode = self.redis_client.get('btc_trading:simulation_mode')
            if sim_mode is not None:
                redis_simulation = sim_mode == '1'
                
                if (not redis_simulation) and (not self.allow_redis_live_switch):
                    logger.warning(
                        "LIVE SAFETY: Redis requested live mode but redis-driven live switching is disabled. "
                        "Ignoring request."
                    )
                    return self.current_simulation_mode

                if redis_simulation != self.current_simulation_mode:
                    self.current_simulation_mode = redis_simulation
                    mode_text = "SIMULATION" if redis_simulation else "LIVE TRADING"
                    logger.warning(f"Trading mode changed to: {mode_text}")

                    if not redis_simulation:
                        logger.warning("LIVE TRADING ACTIVE - Real money at risk!")

                return redis_simulation
        except Exception as e:
            logger.warning(f"Failed to check Redis simulation mode: {e}")
        
        return self.current_simulation_mode
    
    def on_start(self):
        """Called when strategy starts."""
        logger.info("=" * 80)
        logger.info("INTEGRATED BTC STRATEGY STARTED")
        logger.info("=" * 80)

        if not self.enable_divergence:
            logger.warning("Divergence disabled")
        
        # Find BTC instrument FIRST and wait for it
        self._find_btc_instrument()
        
        # Generate synthetic history only in simulation/test mode
        if self.allow_synthetic_history:
            logger.info("Generating synthetic price history for testing...")
            if len(self.price_history) < 20:
                self._generate_synthetic_history(target_count=20, existing_count=len(self.price_history))
        else:
            logger.info("LIVE SAFETY: synthetic history disabled; waiting for real market quotes.")
        
        # Try to get real price if instrument exists and we have quotes
        if self.instrument_id:
            try:
                # Get the most recent quote from cache
                quote = self.cache.quote_tick(self.instrument_id)
                if quote and quote.bid_price and quote.ask_price:
                    current_price = (quote.bid_price + quote.ask_price) / 2
                    # Replace last synthetic price with real one
                    if self.price_history:
                        self.price_history[-1] = current_price
                    else:
                        self.price_history.append(current_price)
                    logger.info(f"Real price from cache: ${float(current_price):.4f}")
            except Exception as e:
                logger.debug(f"Could not get real price: {e}")
                logger.debug("Using synthetic prices until real quotes arrive")
        
        # Start async services on the strategy event loop only
        try:
            loop = asyncio.get_running_loop()
            if self.grafana_exporter:
                loop.create_task(self._start_grafana())
            loop.create_task(self._preload_price_history())
        except RuntimeError:
            logger.warning("No running event loop in on_start; async startup tasks skipped")
        
        logger.info("=" * 80)
        logger.info("Strategy active - will trade every 15 minutes")
        logger.info(f"Price history: {len(self.price_history)} points")
        if len(self.price_history) >= 20:
            logger.info("✓ READY TO TRADE at next 15-minute mark!")
        else:
            logger.warning(f"⚠ Need more history ({len(self.price_history)}/20)")
        logger.info("=" * 80)
        logger.info("Use Ctrl+C to stop")
                
    async def _preload_price_history(self):
        """Pre-load price history from cache or generate synthetic data for testing."""
        logger.info("=" * 80)
        logger.info("PRE-LOADING PRICE HISTORY")
        logger.info("=" * 80)
        
        # Get current instrument
        if not self.instrument_id:
            logger.warning("No instrument ID, skipping preload")
            return
        
        # Try to get current price from cache first
        quote = self.cache.quote_tick(self.instrument_id)
        if quote:
            current_price = (quote.bid_price + quote.ask_price) / 2
            self.price_history.append(current_price)
            logger.info(f"Current price from cache: ${float(current_price):.4f}")
        
        # Try to get historical quotes from cache
        # Note: This depends on your data provider storing history
        quotes = self.cache.quote_tick(self.instrument_id)
        if quotes and len(quotes) > 0:
            for quote in quotes[-20:]:  # Take last 20 quotes
                mid_price = (quote.bid_price + quote.ask_price) / 2
                self.price_history.append(mid_price)
            logger.info(f"Loaded {len(quotes)} historical quotes from cache")
        
        # Remove duplicates while preserving order
        seen = set()
        unique_history = []
        for price in self.price_history:
            price_str = str(price)
            if price_str not in seen:
                seen.add(price_str)
                unique_history.append(price)
        self.price_history = unique_history
        if self.instrument_id:
            self.price_history_by_instrument[self.instrument_id] = deque(self.price_history, maxlen=self.max_history)
        
        # If still not enough, generate synthetic data only in simulation mode
        if len(self.price_history) < 20:
            if self.allow_synthetic_history:
                logger.warning(f"Only {len(self.price_history)} historical quotes found, generating synthetic data to fill")
                self._generate_synthetic_history(existing_count=len(self.price_history))
            else:
                logger.warning(
                    f"LIVE SAFETY: only {len(self.price_history)} historical quotes available; "
                    "will wait for live quotes instead of generating synthetic history"
                )
        
        logger.info(f"Final price history: {len(self.price_history)} points")
        if len(self.price_history) >= 20:
            logger.info("✓ SUFFICIENT HISTORY - Ready to trade!")
        else:
            logger.warning("⚠ Still need more history - will collect from live data")
        
        # Show first few prices
        logger.info("Sample price points:")
        for i, price in enumerate(self.price_history[:5]):
            logger.info(f"  Price {i+1}: ${float(price):.4f}")
        
        logger.info("=" * 80)
    
    def _generate_synthetic_history(self, target_count: int = 20, existing_count: int = 0):
        """Generate synthetic price history for testing/initialization."""
        # Get current price if available
        if self.price_history and len(self.price_history) > 0:
            base_price = self.price_history[-1]
            logger.info(f"Using last real price as base: ${float(base_price):.4f}")
        else:
            # Use a reasonable default for prediction markets
            base_price = Decimal("0.5")
            logger.info(f"No real price available, using default base: ${float(base_price):.4f}")
        
        needed = target_count - existing_count
        if needed <= 0:
            return
        
        logger.info(f"Generating {needed} synthetic price points")
        
        # Generate realistic looking price movement (random walk)
        for i in range(needed):
            # Random walk with small steps (±3% max change)
            change = Decimal(str(random.uniform(-0.03, 0.03)))
            new_price = base_price * (Decimal("1.0") + change)
            
            # Ensure price stays in 0-1 range for prediction markets
            new_price = max(Decimal("0.01"), min(Decimal("0.99"), new_price))
            
            self.price_history.append(new_price)
            base_price = new_price
        
        if self.instrument_id:
            self.price_history_by_instrument[self.instrument_id] = deque(self.price_history, maxlen=self.max_history)

        logger.info(f"Generated {needed} synthetic price points")
        logger.info(f"Now have {len(self.price_history)} total price points")
    
    def _maybe_reload_instruments(self, now: datetime) -> None:
        """Reload instruments every 12 minutes on the strategy thread."""
        if (now - self.last_reload_time).total_seconds() < 720:
            return

        self.last_reload_time = now
        logger.info("=" * 80)
        logger.info("RELOADING INSTRUMENTS (12-minute interval)")
        logger.info("=" * 80)

        try:
            instruments = self.cache.instruments()
            logger.info(f"Before reload: {len(instruments)} instruments in cache")
            self._find_btc_instrument()
            logger.info("Instruments reloaded successfully")
        except Exception as e:
            logger.error(f"Failed to reload instruments: {e}")

    async def _start_grafana(self):
        """Start Grafana on the strategy event loop."""
        try:
            await self.grafana_exporter.start()
            logger.info("Grafana metrics started on port 8000")
        except Exception as e:
            logger.error(f"Failed to start Grafana: {e}")
    
    def _classify_market_outcome(self, instrument) -> Optional[str]:
        """Classify an instrument outcome as up/down from metadata hints."""
        info = instrument.info if hasattr(instrument, "info") and instrument.info else {}
        outcome_index = info.get("outcome_index")
        try:
            if outcome_index is not None:
                idx = int(outcome_index)
                if idx == 0:
                    return "up"
                if idx == 1:
                    return "down"
        except (TypeError, ValueError):
            pass

        text = " ".join(
            str(info.get(k, ""))
            for k in ("outcome", "name", "title", "question", "description")
        ).lower()

        up_markers = ("yes", "up", "above", "higher", "rise", "bull")
        down_markers = ("no", "down", "below", "lower", "fall", "bear")

        if any(marker in text for marker in up_markers):
            return "up"
        if any(marker in text for marker in down_markers):
            return "down"
        return None

    def _find_btc_instrument(self):
        """Find and pair UP/DOWN outcome tokens for the active 15-min market by condition_id."""
        instruments = self.cache.instruments()
        logger.info(f"Checking {len(instruments)} loaded instruments...")

        if not instruments:
            logger.error("NO INSTRUMENTS LOADED!")
            return

        now = datetime.now(timezone.utc)
        current_timestamp = int(now.timestamp())

        markets_by_condition = {}

        for instrument in instruments:
            try:
                if not hasattr(instrument, "info") or not instrument.info:
                    continue

                info = instrument.info
                question = str(info.get("question", "")).lower()
                slug = str(info.get("market_slug", "")).lower()

                if not (any(sym.lower() in question or sym.lower() in slug for sym in self.trade_symbols) and "15m" in slug):
                    continue

                condition_id = info.get("condition_id")
                if not condition_id:
                    continue

                try:
                    market_timestamp = int(slug.split("-")[-1])
                except (ValueError, IndexError):
                    continue

                record = markets_by_condition.get(condition_id)
                if record is None:
                    end_timestamp = None
                    end_date = info.get("end_date_iso")
                    if end_date:
                        end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                        end_timestamp = int(end_dt.timestamp())

                    time_diff = market_timestamp - current_timestamp
                    record = {
                        "condition_id": condition_id,
                        "slug": slug,
                        "question": question,
                        "market_timestamp": market_timestamp,
                        "end_timestamp": end_timestamp,
                        "time_diff_minutes": time_diff / 60,
                        "symbol": next((sym for sym in self.trade_symbols if sym.lower() in question or sym.lower() in slug), self.selected_symbol),
                        "tokens": {"up": None, "down": None},
                        "best_spread_pct": None,
                        "has_quote": False,
                    }
                    markets_by_condition[condition_id] = record

                side = self._classify_market_outcome(instrument)
                if side in ("up", "down") and record["tokens"][side] is None:
                    record["tokens"][side] = instrument.id

                quote = self.cache.quote_tick(instrument.id)
                if quote and quote.bid_price and quote.ask_price:
                    bid = quote.bid_price.as_decimal()
                    ask = quote.ask_price.as_decimal()
                    mid = (bid + ask) / 2
                    if mid > 0:
                        spread_pct = float((ask - bid) / mid)
                        best = record["best_spread_pct"]
                        record["best_spread_pct"] = spread_pct if best is None else min(best, spread_pct)
                    record["has_quote"] = True
            except Exception:
                continue

        paired_markets = [m for m in markets_by_condition.values() if m["tokens"]["up"] and m["tokens"]["down"]]
        if not paired_markets:
            logger.error("NO PAIRED 15-MIN MARKETS FOUND (missing UP/DOWN token mapping)")
            return

        current_markets = [m for m in paired_markets if m["time_diff_minutes"] <= 0 and m["time_diff_minutes"] > -15]
        future_markets = [m for m in paired_markets if m["time_diff_minutes"] > 0]

        logger.info("=" * 80)
        logger.info("TARGET 15-MIN MARKET CONDITIONS:")
        for m in paired_markets:
            status = "CURRENT" if m in current_markets else "FUTURE" if m["time_diff_minutes"] > 0 else "PAST"
            logger.info(f"  {m['slug']} [{m['condition_id']}]: {status} (starts in {m['time_diff_minutes']:.1f} min)")
        logger.info("=" * 80)

        candidates = current_markets if current_markets else future_markets
        if not candidates:
            logger.error("No current/future 15m market conditions available")
            return

        for c in candidates:
            recency_score = max(0.0, 1.0 - (abs(c["time_diff_minutes"]) / 15.0))
            spread_score = 0.0
            spread_pct = c.get("best_spread_pct")
            if spread_pct is not None:
                spread_score = max(0.0, 1.0 - (spread_pct / max(self.max_spread_pct, 1e-6)))
            quote_bonus = 0.25 if c.get("has_quote") else 0.0
            c["selection_score"] = (
                self.market_recency_weight * recency_score
                + self.market_spread_weight * spread_score
                + quote_bonus
            )

        candidates.sort(key=lambda x: x.get("selection_score", 0.0), reverse=True)
        selected = candidates[0]

        self.selected_symbol = selected.get("symbol", self.selected_symbol)
        self.current_condition_id = selected["condition_id"]
        end_ts = selected.get("end_timestamp")
        self.current_market_end_time = datetime.fromtimestamp(end_ts, tz=timezone.utc) if end_ts else None
        self.up_instrument_id = selected["tokens"]["up"]
        self.down_instrument_id = selected["tokens"]["down"]

        new_instrument_id = self.up_instrument_id
        if self.instrument_id != new_instrument_id:
            logger.debug(f"Switching instrument history: {self.instrument_id} -> {new_instrument_id}")
            self.price_history_by_instrument[new_instrument_id] = deque(maxlen=self.max_history)
        self.instrument_id = new_instrument_id
        self.price_history = list(self._get_current_history())

        self.subscribe_quote_ticks(self.up_instrument_id)
        self.subscribe_quote_ticks(self.down_instrument_id)

        logger.info(
            f"Selected condition {self.current_condition_id} ({self.selected_symbol}) "
            f"UP={self.up_instrument_id} DOWN={self.down_instrument_id}"
        )

    def on_quote_tick(self, tick: QuoteTick):
        """Handle quote tick updates."""
        try:
            # Check if we have valid prices
            if tick.bid_price is None or tick.ask_price is None:
                logger.debug(f"Skipping incomplete quote: bid={tick.bid_price}, ask={tick.ask_price}")
                return
            
            # Get decimal values properly
            bid_decimal = tick.bid_price.as_decimal()
            ask_decimal = tick.ask_price.as_decimal()
            
            # Calculate mid price
            mid_price = (bid_decimal + ask_decimal) / 2

            # Update per-instrument price history
            inst_id = getattr(tick, "instrument_id", self.instrument_id)
            if inst_id == self.instrument_id:
                self.latest_bid = bid_decimal
                self.latest_ask = ask_decimal
            if inst_id not in self.price_history_by_instrument:
                self.price_history_by_instrument[inst_id] = deque(maxlen=self.max_history)
            self.price_history_by_instrument[inst_id].append(mid_price)

            # Keep convenience view pointed at current instrument history only
            if inst_id == self.instrument_id:
                self.price_history = list(self.price_history_by_instrument[inst_id])

            logger.debug(
                f"History[{inst_id}] size={len(self.price_history_by_instrument[inst_id])} "
                f"(current={self.instrument_id})"
            )

            self.real_quote_count += 1
            
            # Check if we should trade
            now = datetime.now(timezone.utc)
            self._update_paper_positions(inst_id=inst_id, mid_price=mid_price, now=now)
            self._maybe_reload_instruments(now)
            
            if self.test_mode:
                # TEST MODE: Trade every minute at the start of each minute
                current_minute = now.replace(second=0, microsecond=0)
                seconds_since_minute = now.second
                
                if seconds_since_minute < 5:  # Within first 5 seconds of each minute
                    if current_minute != self.last_trade_time:
                        self.last_trade_time = current_minute
                        logger.info("=" * 80)
                        logger.info(f"TEST MODE - MINUTE REACHED: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                        logger.info(f"Current price: ${float(mid_price):,.4f}")
                        logger.info(f"Bid: ${float(bid_decimal):,.4f}, Ask: ${float(ask_decimal):,.4f}")
                        logger.info(f"Price history size (current instrument): {len(self._get_current_history())}")
                        logger.info("=" * 80)
                        
                        # Make trading decision
                        asyncio.create_task(self._make_trading_decision(Decimal(str(float(mid_price)))))
            else:
                # NORMAL MODE: Trade every 15 minutes
                seconds_since_interval = now.timestamp() % 900
                
                if seconds_since_interval < 30:
                    current_interval = int(now.timestamp() // 900)
                    
                    if current_interval != self.last_trade_time:
                        self.last_trade_time = current_interval
                        logger.info("=" * 80)
                        logger.info(f"15-MIN INTERVAL REACHED: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                        logger.info(f"Current price: ${float(mid_price):,.4f}")
                        logger.info(f"Bid: ${float(bid_decimal):,.4f}, Ask: ${float(ask_decimal):,.4f}")
                        logger.info(f"Price history size (current instrument): {len(self._get_current_history())}")
                        logger.info("=" * 80)
                        
                        # Make trading decision
                        asyncio.create_task(self._make_trading_decision(Decimal(str(float(mid_price)))))
        
        except Exception as e:
            logger.error(f"Error processing quote tick: {e}")
            import traceback
            traceback.print_exc()
    async def _make_trading_decision(self, current_price):
        """Make trading decision using our 7-phase system."""
        
        # Check simulation mode
        is_simulation = await self.check_simulation_mode()
        mode_text = "SIMULATION" if is_simulation else "LIVE TRADING"
        logger.info(f"Mode: {mode_text}")

        if not is_simulation and self.real_quote_count < 20:
            logger.warning(
                "LIVE SAFETY: waiting for at least 20 real quote ticks before trading "
                f"(currently {self.real_quote_count})."
            )
            return
        
        # Need price history
        current_history = self._get_current_history()
        if len(current_history) < 20:
            logger.warning(f"Not enough price history yet ({len(current_history)}/20) for instrument {self.instrument_id}")
            return
        
        logger.info(f"Current price: ${float(current_price):,.4f}")
        
        metadata = await self._build_signal_metadata(current_price=current_price, is_simulation=is_simulation)
        
        # Phase 4: Process signals
        signals = self._process_signals(current_price, metadata)
        
        if not signals:
            logger.info("No signals generated")
            return

        logger.info(f"Generated {len(signals)} signals:")
        for sig in signals:
            logger.info(f"  [{sig.source}] {sig.direction.value}: score={sig.score:.1f}")

        # Phase 4: Fuse signals
        fused = self.fusion_engine.fuse_signals(signals, min_signals=1, min_score=self.min_fusion_score)

        if not fused:
            logger.info("No actionable fused signal")
            return

        logger.info(f"FUSED SIGNAL: {fused.direction.value} (score={fused.score:.1f}, confidence={fused.confidence:.2%})")

        if fused.confidence < self.min_fusion_confidence:
            self._audit_decision("skipped", {"reason": "low_confidence", "confidence": fused.confidence, "score": fused.score})
            logger.warning(f"Skipping trade: confidence {fused.confidence:.2%} below min {self.min_fusion_confidence:.2%}")
            return

        if self.latest_bid is not None and self.latest_ask is not None:
            mid = (self.latest_bid + self.latest_ask) / 2
            if mid > 0:
                spread_pct = float((self.latest_ask - self.latest_bid) / mid)
                if spread_pct > self.max_spread_pct:
                    self._audit_decision("skipped", {"reason": "wide_spread", "spread_pct": spread_pct, "max_spread_pct": self.max_spread_pct})
                    logger.warning(f"Skipping trade: spread {spread_pct:.2%} above max {self.max_spread_pct:.2%}")
                    return

        # Phase 5: Calculate position size (with $1 cap)
        spread_pct_for_sizing = None
        if self.latest_bid is not None and self.latest_ask is not None:
            mid_for_sizing = (self.latest_bid + self.latest_ask) / 2
            if mid_for_sizing > 0:
                spread_pct_for_sizing = float((self.latest_ask - self.latest_bid) / mid_for_sizing)

        position_size = self.risk_engine.calculate_position_size(
            signal_confidence=fused.confidence,
            signal_score=fused.score,
            current_price=current_price,  # Pass Decimal to risk engine
            symbol=self.selected_symbol,
            spread_pct=spread_pct_for_sizing,
            volatility_pct=self._estimate_volatility_pct(),
        )

        logger.info(f"Calculated position size: ${float(position_size):.2f}")

        # Phase 5: Validate with risk engine
        direction = "long" if "BULLISH" in str(fused.direction) else "short"
        is_valid, error = self.risk_engine.validate_new_position(
            size=position_size,
            direction=direction,
            current_price=current_price,
            symbol=self.selected_symbol,
        )

        if not is_valid:
            logger.warning(f"Position rejected by risk engine: {error}")
            return

        self._audit_decision("accepted", {
            "direction": direction,
            "score": fused.score,
            "confidence": fused.confidence,
            "position_size": float(position_size),
            "symbol": self.selected_symbol,
        })

        # Execute trade (simulation or live based on Redis)
        if is_simulation:
            await self._record_paper_trade(fused, position_size, current_price, direction)
        else:
            await self._place_real_order(fused, position_size, current_price, direction)

    async def _build_signal_metadata(self, current_price: Decimal, is_simulation: bool) -> dict:
        """Build metadata for signal processors with live-trading safety guards."""
        metadata = {}

        env_sentiment = os.getenv("BOT_SENTIMENT_SCORE")
        env_spot = os.getenv("BOT_SPOT_PRICE")

        if env_sentiment is not None:
            try:
                metadata["sentiment_score"] = float(env_sentiment)
            except ValueError:
                logger.warning("BOT_SENTIMENT_SCORE is not numeric; ignoring")

        if env_spot is not None:
            try:
                metadata["spot_price"] = float(env_spot)
            except ValueError:
                logger.warning("BOT_SPOT_PRICE is not numeric; ignoring")

        if metadata:
            return metadata

        external_context = await self._get_external_context()
        if "spot_price" in external_context:
            metadata["spot_price"] = external_context["spot_price"]
        if "sentiment_score" in external_context:
            metadata["sentiment_score"] = external_context["sentiment_score"]

        if metadata:
            return metadata

        if is_simulation:
            if not self._logged_simulation_fallback_warning:
                logger.warning(
                    "Simulation fallback active: using synthetic sentiment/spot metadata. "
                    "Set BOT_SENTIMENT_SCORE and BOT_SPOT_PRICE to use deterministic values."
                )
                self._logged_simulation_fallback_warning = True

            current_price_float = float(current_price)
            return {
                "sentiment_score": random.uniform(10, 90),
                "spot_price": current_price_float * random.uniform(0.95, 1.05),
            }

        logger.warning(
            "LIVE SAFETY: no metadata configured. Sentiment/divergence processors disabled; "
            "only spike detection will be used."
        )
        return {}
            
    async def _get_external_context(self) -> dict:
        """Get cached external context (spot + sentiment) for more accurate signals."""
        now = datetime.now(timezone.utc)
        age = (now - self._last_external_refresh).total_seconds()

        if self._external_context_cache and age < self._external_refresh_seconds:
            return self._external_context_cache

        context = self._fetch_external_context_sync()
        if context:
            self._external_context_cache = context
            self._last_external_refresh = now

        return self._external_context_cache

    def _fetch_external_context_sync(self) -> dict:
        """Fetch external market/sentiment data without adding event-loop coupling."""
        context = {}

        # Coinbase spot
        try:
            with urllib.request.urlopen(
                "https://api.exchange.coinbase.com/products/BTC-USD/ticker", timeout=4
            ) as response:
                data = json.loads(response.read().decode("utf-8"))
                spot = float(data.get("price"))
                if spot > 0:
                    context["spot_price"] = spot
        except Exception as e:
            logger.debug(f"External context: Coinbase spot unavailable ({e})")

        # Fear & Greed
        try:
            with urllib.request.urlopen("https://api.alternative.me/fng/", timeout=4) as response:
                data = json.loads(response.read().decode("utf-8"))
                rows = data.get("data", [])
                if rows:
                    context["sentiment_score"] = float(rows[0].get("value"))
        except Exception as e:
            logger.debug(f"External context: sentiment unavailable ({e})")

        return context

    async def _maybe_optimize_weights(self) -> None:
        """Periodically optimize fusion weights based on recent trade outcomes."""
        trade_count = len(self.paper_trades)
        if trade_count == 0 or trade_count % self._learning_interval_trades != 0:
            return

        try:
            new_weights = await self.learning_engine.optimize_weights()
            self.learning_engine.save_state()
            logger.info(f"Learning update applied at trade #{trade_count}: {new_weights}")
        except Exception as e:
            logger.warning(f"Learning update failed: {e}")

    def _paper_hold_delta(self) -> timedelta:
        """Return configured paper hold period."""
        return timedelta(minutes=1 if self.test_mode else 15)

    def _update_paper_positions(self, inst_id: InstrumentId, mid_price: Decimal, now: datetime) -> None:
        """Mark-to-market open paper positions and close when rules are met."""
        for position in list(self.paper_positions):
            if position.instrument_id != inst_id:
                continue

            position.latest_mid = mid_price
            if position.entry_mid > 0:
                position.unrealized_pnl_usd = position.size_usd * ((mid_price - position.entry_mid) / position.entry_mid)

            close_reason = None
            if now >= position.hold_until:
                close_reason = "hold_period"
            if position.market_end_time and now >= position.market_end_time:
                close_reason = "market_end"

            if close_reason:
                self._close_paper_position(position=position, exit_mid=mid_price, exit_time=now, reason=close_reason)

    def _close_paper_position(self, position: PaperPosition, exit_mid: Decimal, exit_time: datetime, reason: str) -> None:
        """Close an open paper position using observed market mid."""
        if position.entry_mid <= 0:
            pnl = Decimal("0")
        else:
            pnl = position.size_usd * ((exit_mid - position.entry_mid) / position.entry_mid)

        outcome = "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT"
        self.risk_engine.register_trade_result(pnl)

        paper_trade = PaperTrade(
            timestamp=exit_time,
            direction=position.direction.upper(),
            size_usd=float(position.size_usd),
            price=float(position.entry_mid),
            signal_score=position.signal_score,
            signal_confidence=position.signal_confidence,
            outcome=outcome,
        )
        self.paper_trades.append(paper_trade)

        self.performance_tracker.record_trade(
            trade_id=position.trade_id,
            direction=position.direction,
            entry_price=position.entry_mid,
            exit_price=exit_mid,
            size=position.size_usd,
            entry_time=position.entry_time,
            exit_time=exit_time,
            signal_score=position.signal_score,
            signal_confidence=position.signal_confidence,
            metadata={
                "simulated": True,
                "instrument_id": str(position.instrument_id),
                "close_reason": reason,
                "hold_seconds": (position.hold_until - position.entry_time).total_seconds(),
            },
        )

        if self.grafana_exporter:
            self.grafana_exporter.increment_trade_counter(won=(pnl > 0))
            self.grafana_exporter.record_trade_duration((exit_time - position.entry_time).total_seconds())

        logger.info("=" * 80)
        logger.info("[SIMULATION] PAPER POSITION CLOSED")
        logger.info(f"  Trade ID: {position.trade_id}")
        logger.info(f"  Direction: {position.direction.upper()} (BUY token)")
        logger.info(f"  Instrument: {position.instrument_id}")
        logger.info(f"  Size: ${float(position.size_usd):.2f}")
        logger.info(f"  Entry Mid: ${float(position.entry_mid):,.4f}")
        logger.info(f"  Exit Mid: ${float(exit_mid):,.4f}")
        logger.info(f"  Realized P&L: ${float(pnl):+.2f}")
        logger.info(f"  Outcome: {outcome}")
        logger.info(f"  Close Reason: {reason}")
        logger.info(f"  Total Closed Paper Trades: {len(self.paper_trades)}")
        logger.info("=" * 80)

        self.paper_positions.remove(position)
        self._save_paper_trades()

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._maybe_optimize_weights())
        except RuntimeError:
            logger.debug("No running event loop for optimize_weights scheduling")

    async def _record_paper_trade(self, signal, position_size, current_price, direction):
        """Open a paper position tracked by real observed mid prices."""
        target_instrument_id = self.up_instrument_id if direction == "long" else self.down_instrument_id
        if not target_instrument_id:
            logger.warning("Simulation skip: no target paired instrument for paper position")
            return

        quote = self.cache.quote_tick(target_instrument_id)
        entry_mid = current_price
        if quote and quote.bid_price and quote.ask_price:
            entry_mid = (quote.bid_price.as_decimal() + quote.ask_price.as_decimal()) / 2

        now = datetime.now(timezone.utc)
        hold_delta = self._paper_hold_delta()
        trade_id = f"paper_{int(now.timestamp() * 1000)}"

        position = PaperPosition(
            trade_id=trade_id,
            entry_time=now,
            direction=direction,
            instrument_id=target_instrument_id,
            size_usd=position_size,
            entry_mid=entry_mid,
            latest_mid=entry_mid,
            signal_score=signal.score,
            signal_confidence=signal.confidence,
            hold_until=now + hold_delta,
            market_end_time=self.current_market_end_time,
        )
        self.paper_positions.append(position)

        logger.info("=" * 80)
        logger.info("[SIMULATION] PAPER POSITION OPENED")
        logger.info(f"  Trade ID: {trade_id}")
        logger.info(f"  Direction: {direction.upper()} (BUY token)")
        logger.info(f"  Instrument: {target_instrument_id}")
        logger.info(f"  Size: ${float(position_size):.2f}")
        logger.info(f"  Entry Mid: ${float(entry_mid):,.4f}")
        logger.info(f"  Hold Until: {(now + hold_delta).isoformat()}")
        if self.current_market_end_time:
            logger.info(f"  Market End: {self.current_market_end_time.isoformat()}")
        logger.info(f"  Open Paper Positions: {len(self.paper_positions)}")
        logger.info("=" * 80)

    def _save_paper_trades(self):
        """Save paper trades to JSON file."""
        import json
        try:
            trades_data = [t.to_dict() for t in self.paper_trades]
            with open('paper_trades.json', 'w') as f:
                json.dump(trades_data, f, indent=2)
            logger.info(f"Saved {len(trades_data)} paper trades to paper_trades.json")
        except Exception as e:
            logger.error(f"Failed to save paper trades: {e}")
    
    def _reset_daily_notional_if_needed(self, now: datetime) -> None:
        """Reset daily notional counter at UTC day boundary."""
        current_day = now.date()
        if current_day != self.daily_notional_date:
            self.daily_notional_date = current_day
            self.daily_notional_usd = Decimal("0")

    def _check_live_order_limits(self, notional_usd: Decimal) -> Optional[str]:
        """Return blocking reason when live-order safety limits are exceeded."""
        now = datetime.now(timezone.utc)
        self._reset_daily_notional_if_needed(now)

        if self.disable_live_orders:
            return "DISABLE_LIVE_ORDERS=1"

        if self.max_orders_per_hour > 0:
            cutoff = now - timedelta(hours=1)
            while self.live_order_timestamps and self.live_order_timestamps[0] < cutoff:
                self.live_order_timestamps.popleft()
            if len(self.live_order_timestamps) >= self.max_orders_per_hour:
                return (
                    f"MAX_ORDERS_PER_HOUR exceeded "
                    f"({len(self.live_order_timestamps)}/{self.max_orders_per_hour})"
                )

        if self.max_daily_notional > 0 and (self.daily_notional_usd + notional_usd) > self.max_daily_notional:
            projected = self.daily_notional_usd + notional_usd
            return (
                "MAX_DAILY_NOTIONAL exceeded "
                f"(projected=${float(projected):.2f}, limit=${float(self.max_daily_notional):.2f})"
            )

        return None

    def _record_live_order_usage(self, notional_usd: Decimal) -> None:
        """Track accepted live-order usage against hourly/daily limits."""
        now = datetime.now(timezone.utc)
        self._reset_daily_notional_if_needed(now)
        self.live_order_timestamps.append(now)
        self.daily_notional_usd += notional_usd

    async def _place_real_order(self, signal, position_size, current_price, direction):
        """Place REAL order using Nautilus (BUY-only: bullish->UP, bearish->DOWN)."""
        if not self.up_instrument_id or not self.down_instrument_id:
            logger.error("No paired UP/DOWN instruments available")
            return

        try:
            logger.info("=" * 80)
            logger.info("LIVE MODE - PLACING REAL ORDER!")
            logger.info("=" * 80)

            side = OrderSide.BUY
            target_instrument_id = self.up_instrument_id if direction == "long" else self.down_instrument_id

            instrument = self.cache.instrument(target_instrument_id)
            if not instrument:
                logger.error("Instrument not in cache")
                return

            quote = self.cache.quote_tick(target_instrument_id)
            ref_bid = self.latest_bid
            ref_ask = self.latest_ask
            if quote and quote.bid_price and quote.ask_price:
                ref_bid = quote.bid_price.as_decimal()
                ref_ask = quote.ask_price.as_decimal()

            if ref_bid is not None and ref_ask is not None:
                mid = (ref_bid + ref_ask) / 2
                if mid > 0:
                    slippage_pct = float(abs(ref_ask - mid) / mid)
                    if slippage_pct > self.max_slippage_pct:
                        self._audit_decision("skipped", {
                            "reason": "slippage_too_high",
                            "slippage_pct": slippage_pct,
                            "max_slippage_pct": self.max_slippage_pct,
                            "side": side.name,
                            "target_instrument_id": str(target_instrument_id),
                        })
                        logger.warning(
                            f"Skipping real order: expected slippage {slippage_pct:.2%} exceeds max {self.max_slippage_pct:.2%}"
                        )
                        return

            trade_price = float(current_price)
            max_usd_amount = float(position_size)
            requested_notional = Decimal(str(max_usd_amount))

            live_order_block_reason = self._check_live_order_limits(requested_notional)
            if live_order_block_reason:
                logger.warning(
                    "LIVE SAFETY: blocking real order due to limit/override: "
                    f"{live_order_block_reason}"
                )
                self._audit_decision("skipped", {
                    "reason": "live_order_blocked",
                    "detail": live_order_block_reason,
                    "requested_notional_usd": float(requested_notional),
                })
                return

            timestamp_ms = int(time.time() * 1000)
            unique_id = f"{self.selected_symbol}-15MIN-{direction.upper()}-${max_usd_amount:.0f}-{timestamp_ms}"

            if self.order_mode == "market":
                quote_precision = max(2, getattr(instrument, "size_precision", 2))
                quote_notional = max(max_usd_amount, 0.01)
                qty = Quantity(quote_notional, precision=quote_precision)

                order = self.order_factory.market(
                    instrument_id=target_instrument_id,
                    order_side=side,
                    quantity=qty,
                    client_order_id=ClientOrderId(unique_id),
                    quote_quantity=True,
                    time_in_force=TimeInForce.IOC,
                )
                self.submit_order(order)
                logger.info("REAL MARKET BUY ORDER SUBMITTED!")
                logger.info(f"  quote_quantity=True, qty(USDC)={quote_notional:.2f}")
            else:
                limit_price = current_price
                if ref_bid is not None and ref_ask is not None:
                    mid = (ref_bid + ref_ask) / 2
                    cap = mid * (Decimal("1") + Decimal(str(self.max_slippage_pct)))
                    limit_price = min(ref_ask, cap)

                if trade_price > 0:
                    token_qty = max_usd_amount / trade_price
                else:
                    token_qty = max_usd_amount * 2

                precision = instrument.size_precision
                token_qty = round(token_qty, precision)
                min_qty = 10 ** (-precision)
                if token_qty < min_qty:
                    token_qty = min_qty

                qty = Quantity(token_qty, precision=precision)
                order = self.order_factory.limit(
                    instrument_id=target_instrument_id,
                    order_side=side,
                    quantity=qty,
                    price=Price.from_str(f"{float(limit_price):.4f}"),
                    client_order_id=ClientOrderId(unique_id),
                    quote_quantity=False,
                    time_in_force=TimeInForce.IOC,
                )
                self.submit_order(order)
                logger.info(f"REAL SMART LIMIT BUY ORDER SUBMITTED @ {float(limit_price):.4f}!")
                logger.info(f"  quote_quantity=False, qty(tokens)={token_qty:.6f}")

            logger.info(f"  Order ID: {unique_id}")
            logger.info(f"  Signal Direction: {direction}")
            logger.info(f"  Side: {side.name}")
            logger.info(f"  Condition ID: {self.current_condition_id}")
            logger.info(f"  UP Instrument: {self.up_instrument_id}")
            logger.info(f"  DOWN Instrument: {self.down_instrument_id}")
            logger.info(f"  Target Instrument: {target_instrument_id}")
            logger.info(f"  Estimated Cost: ~${max_usd_amount:.2f}")
            logger.info(f"  Price: ${trade_price:.4f}")
            logger.info("=" * 80)

            self._record_live_order_usage(requested_notional)

            self.performance_tracker.increment_order_counter("placed")

        except Exception as e:
            logger.error(f"Error placing real order: {e}")
            import traceback
            traceback.print_exc()
            self.performance_tracker.increment_order_counter("rejected")

    def _get_current_history(self) -> list:
        """Return history for the current instrument only."""
        if self.instrument_id in self.price_history_by_instrument:
            return list(self.price_history_by_instrument[self.instrument_id])
        return self.price_history

    def _estimate_volatility_pct(self) -> Optional[float]:
        """Estimate short-horizon volatility from recent mid prices."""
        hist = self._get_current_history()
        if len(hist) < 20:
            return None
        window = [float(p) for p in hist[-20:]]
        returns = []
        for i in range(1, len(window)):
            prev = window[i - 1]
            if prev <= 0:
                continue
            returns.append((window[i] - prev) / prev)
        if not returns:
            return None
        mean_r = sum(returns) / len(returns)
        var = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        return var ** 0.5

    def _process_signals(self, current_price, metadata=None):
        """Process all signal processors."""
        signals = []
        
        if metadata is None:
            metadata = {}
        
        # Convert metadata values to Decimal where needed by processors
        processed_metadata = {}
        for key, value in metadata.items():
            if isinstance(value, float):
                # Convert float to Decimal for processors that expect Decimal
                processed_metadata[key] = Decimal(str(value))
            else:
                processed_metadata[key] = value
        
        current_history = self._get_current_history()

        # Spike detection
        spike_signal = self.spike_detector.process(
            current_price=current_price,
            historical_prices=current_history,
            metadata=processed_metadata,
        )
        if spike_signal:
            signals.append(spike_signal)
        
        # Sentiment processor (if we have sentiment data)
        if 'sentiment_score' in processed_metadata:
            sentiment_signal = self.sentiment_processor.process(
                current_price=current_price,
                historical_prices=current_history,
                metadata=processed_metadata,
            )
            if sentiment_signal:
                signals.append(sentiment_signal)
        
        # Divergence processor (if enabled and we have spot price)
        if self.enable_divergence and 'spot_price' in processed_metadata:
            divergence_signal = self.divergence_processor.process(
                current_price=current_price,
                historical_prices=current_history,
                metadata=processed_metadata,
            )
            if divergence_signal:
                signals.append(divergence_signal)
        
        return signals

    def _audit_decision(self, status: str, payload: dict) -> None:
        """Append machine-readable decision events to local audit log."""
        try:
            event = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": status,
                "symbol": self.selected_symbol,
                **payload,
            }
            with open(self.decision_audit_file, "a") as f:
                f.write(json.dumps(event) + "\n")

            if status == "skipped" and self.grafana_exporter:
                reason = str(payload.get("reason", "unknown"))
                self.grafana_exporter.increment_skip_reason(reason)
        except Exception as e:
            logger.debug(f"Decision audit write failed: {e}")

    def on_order_filled(self, event):
        """Handle when a REAL order is filled."""
        logger.info("=" * 80)
        logger.info(f"ORDER FILLED!")
        logger.info(f"  Order: {event.client_order_id}")
        logger.info(f"  Fill Price: ${float(event.last_px):.4f}")
        logger.info(f"  Quantity: {float(event.last_qty):.6f}")
        logger.info("=" * 80)
        
        self.performance_tracker.increment_order_counter("filled")
    
    def on_order_denied(self, event):
        """Handle when an order is denied."""
        logger.error("=" * 80)
        logger.error(f"ORDER DENIED!")
        logger.error(f"  Order: {event.client_order_id}")
        logger.error(f"  Reason: {event.reason}")
        logger.error("=" * 80)
        
        self.performance_tracker.increment_order_counter("rejected")
    
    def on_stop(self):
        """Called when strategy stops."""
        logger.info("Integrated BTC strategy stopped")

        stop_time = datetime.now(timezone.utc)
        for position in list(self.paper_positions):
            exit_mid = position.latest_mid if position.latest_mid is not None else position.entry_mid
            self._close_paper_position(
                position=position,
                exit_mid=exit_mid,
                exit_time=stop_time,
                reason="strategy_stop",
            )

        logger.info(f"Total paper trades recorded: {len(self.paper_trades)}")

        if self.grafana_exporter:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.grafana_exporter.stop())
            except RuntimeError:
                logger.warning("No running event loop in on_stop; Grafana stop skipped")


def run_integrated_bot(simulation: bool = True, enable_grafana: bool = True, test_mode: bool = False, allow_redis_live_switch: bool = False):
    """Run the integrated BTC 15-min trading bot."""
    print("=" * 80)
    print("INTEGRATED POLYMARKET 15-MIN TRADING BOT")
    print("Nautilus + 7-Phase System + Redis Control")
    print("=" * 80)
    
    # Initialize Redis
    redis_client = init_redis()
    
    # Set initial simulation mode in Redis
    if redis_client:
        try:
            redis_client.set('btc_trading:simulation_mode', '1' if simulation else '0')
            logger.info(f"Initial mode set in Redis: {'SIMULATION' if simulation else 'LIVE'}")
        except Exception as e:
            logger.warning(f"Could not set Redis simulation mode: {e}")
    
    print(f"\nConfiguration:")
    print(f"  Initial Mode: {'SIMULATION' if simulation else 'LIVE TRADING'}")
    print(f"  Redis Control: {'Enabled' if redis_client else 'Disabled'}")
    print(f"  Grafana: {'Enabled' if enable_grafana else 'Disabled'}")
    print(f"  Max Trade Size: $1.00")
    print(f"  Instrument Reload: Every 12 minutes")
    print(f"  Price History: Pre-loaded on startup")
    print()
    
    # CORRECT APPROACH: Use time-based filtering (proven to work from test)
    # DO NOT use tag_id=744 - it returns empty!
    
    now = datetime.now(timezone.utc)

    # IMPORTANT: Use ISO format strings for dates
    filters = {
        "active": True,
        "closed": False,
        "archived": False,
        "end_date_min": now.isoformat().replace('+00:00', 'Z'),  # Markets expiring after now
        "end_date_max": (now + timedelta(minutes=30)).isoformat().replace('+00:00', 'Z'),  # Within 30 min
        "limit": 1000,  # Request more markets per page
    }

    
    logger.info("=" * 80)
    logger.info("Using TIME-BASED FILTERING (proven to work)")
    logger.info(f"  Expiring between: {now.strftime('%H:%M:%S')} - {(now + timedelta(minutes=30)).strftime('%H:%M:%S')} UTC")
    logger.info("  This will load all active markets including BTC 15-min")
    logger.info("=" * 80)
    
    # CRITICAL: use_gamma_markets=True enables filtering!
    instrument_cfg = InstrumentProviderConfig(
        load_all=True,  # Load all markets matching filters
        filters=filters,
        use_gamma_markets=True,  # CRITICAL!
    )
    
    # Polymarket data client config
    poly_data_cfg = PolymarketDataClientConfig(
        private_key=os.getenv("POLYMARKET_PK"),
        api_key=os.getenv("POLYMARKET_API_KEY"),
        api_secret=os.getenv("POLYMARKET_API_SECRET"),
        passphrase=os.getenv("POLYMARKET_PASSPHRASE"),
        instrument_provider=instrument_cfg,
    )
    
    # Polymarket execution client config
    poly_exec_cfg = PolymarketExecClientConfig(
        private_key=os.getenv("POLYMARKET_PK"),
        api_key=os.getenv("POLYMARKET_API_KEY"),
        api_secret=os.getenv("POLYMARKET_API_SECRET"),
        passphrase=os.getenv("POLYMARKET_PASSPHRASE"),
        instrument_provider=instrument_cfg,
    )
    
    # Trading node configuration
    config = TradingNodeConfig(
        environment="live",
        trader_id="BTC-15MIN-INTEGRATED-001",
        logging=LoggingConfig(
            log_level="INFO",
            log_directory="./logs/nautilus",
        ),
        data_engine=LiveDataEngineConfig(qsize=6000),
        exec_engine=LiveExecEngineConfig(qsize=6000),
        risk_engine=LiveRiskEngineConfig(
            bypass=simulation,
        ),
        data_clients={POLYMARKET: poly_data_cfg},
        exec_clients={POLYMARKET: poly_exec_cfg},
    )
    
    # Create integrated strategy
    strategy = IntegratedBTCStrategy(
        redis_client=redis_client,
        enable_grafana=enable_grafana,
        test_mode=test_mode,
        simulation_mode=simulation,
        allow_redis_live_switch=allow_redis_live_switch,
    )
    
    # Build Nautilus node
    print("\nBuilding Nautilus node...")
    print("=" * 80)
    
    node = TradingNode(config=config)
    
    # Add Polymarket factories
    node.add_data_client_factory(POLYMARKET, PolymarketLiveDataClientFactory)
    node.add_exec_client_factory(POLYMARKET, PolymarketLiveExecClientFactory)
    
    # Add strategy
    node.trader.add_strategy(strategy)
    
    # Build and start
    node.build()
    logger.info("Nautilus node built successfully")
    
    print()
    print("=" * 80)
    print("BOT STARTING")
    print("=" * 80)
    
    try:
        node.run()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        node.dispose()
        logger.info("Bot stopped")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Integrated BTC 15-Min Trading Bot")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Run in LIVE mode (real money at risk!). Default is simulation."
    )
    parser.add_argument(
        "--no-grafana",
        action="store_true",
        help="Disable Grafana metrics"
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Run in TEST MODE (trade every minute for faster testing)"
    )
    parser.add_argument(
        "--confirm-live",
        action="store_true",
        help="Required with --live to prevent accidental real-money execution"
    )
    parser.add_argument(
        "--allow-redis-live-switch",
        action="store_true",
        help="Allow Redis control to switch from simulation to live mode at runtime"
    )
    
    args = parser.parse_args()
    
    simulation = not args.live
    enable_grafana = not args.no_grafana
    test_mode = args.test_mode
    
    if not simulation:
        print("WARNING: LIVE TRADING MODE - REAL MONEY AT RISK!")
        if not args.confirm_live:
            print("Refusing to start live trading without --confirm-live.")
            return

        if os.getenv("LIVE_TRADING_ENABLED", "").strip() != "YES_I_UNDERSTAND":
            print("Refusing to start live trading: set LIVE_TRADING_ENABLED=YES_I_UNDERSTAND")
            return

        required_live_env = [
            "POLYMARKET_PK",
            "POLYMARKET_API_KEY",
            "POLYMARKET_API_SECRET",
            "POLYMARKET_PASSPHRASE",
        ]
        missing = [k for k in required_live_env if not os.getenv(k)]
        if missing:
            print(f"Refusing to start live trading: missing required credentials: {', '.join(missing)}")
            return
    
    run_integrated_bot(
        simulation=simulation,
        enable_grafana=enable_grafana,
        test_mode=test_mode,
        allow_redis_live_switch=args.allow_redis_live_switch,
    )


if __name__ == "__main__":
    main()
