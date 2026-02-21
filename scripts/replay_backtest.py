"""Simple replay/backtest harness for 15m strategy result files.

Input CSV columns required:
- timestamp
- direction (long/short)
- entry_price
- exit_price
- size

Outputs:
- precision/recall by direction
- expectancy per trade
- max drawdown
- pass/fail gate based on CLI thresholds
"""
import argparse
import csv
from dataclasses import dataclass
from typing import List


@dataclass
class ReplayTrade:
    direction: str
    entry_price: float
    exit_price: float
    size: float

    @property
    def predicted_up(self) -> bool:
        return self.direction.lower() == "long"

    @property
    def realized_up(self) -> bool:
        return self.exit_price > self.entry_price

    @property
    def pnl(self) -> float:
        if self.direction.lower() == "long":
            return self.size * ((self.exit_price - self.entry_price) / self.entry_price)
        return self.size * ((self.entry_price - self.exit_price) / self.entry_price)


def load_trades(path: str) -> List[ReplayTrade]:
    trades: List[ReplayTrade] = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trades.append(
                ReplayTrade(
                    direction=row["direction"],
                    entry_price=float(row["entry_price"]),
                    exit_price=float(row["exit_price"]),
                    size=float(row.get("size", 1.0)),
                )
            )
    return trades


def precision_recall(trades: List[ReplayTrade], positive_up: bool):
    tp = fp = fn = 0
    for t in trades:
        pred = t.predicted_up if positive_up else (not t.predicted_up)
        truth = t.realized_up if positive_up else (not t.realized_up)
        if pred and truth:
            tp += 1
        elif pred and not truth:
            fp += 1
        elif (not pred) and truth:
            fn += 1
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    return precision, recall


def drawdown_curve(pnls: List[float]):
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        equity += p
        peak = max(peak, equity)
        dd = (peak - equity)
        max_dd = max(max_dd, dd)
    return max_dd


def main():
    parser = argparse.ArgumentParser(description="Replay backtest harness")
    parser.add_argument("--csv", required=True, help="CSV with trade outcomes")
    parser.add_argument("--min-expectancy", type=float, default=0.0)
    parser.add_argument("--max-drawdown", type=float, default=5.0)
    parser.add_argument("--min-precision", type=float, default=0.5)
    args = parser.parse_args()

    trades = load_trades(args.csv)
    if not trades:
        raise SystemExit("No trades found")

    pnls = [t.pnl for t in trades]
    expectancy = sum(pnls) / len(pnls)
    max_dd = drawdown_curve(pnls)

    p_up, r_up = precision_recall(trades, positive_up=True)
    p_down, r_down = precision_recall(trades, positive_up=False)

    print(f"trades={len(trades)}")
    print(f"expectancy={expectancy:.4f}")
    print(f"max_drawdown={max_dd:.4f}")
    print(f"precision_up={p_up:.4f} recall_up={r_up:.4f}")
    print(f"precision_down={p_down:.4f} recall_down={r_down:.4f}")

    passed = (
        expectancy >= args.min_expectancy
        and max_dd <= args.max_drawdown
        and p_up >= args.min_precision
        and p_down >= args.min_precision
    )

    if not passed:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
