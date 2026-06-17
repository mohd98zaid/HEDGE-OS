# 📈 Portfolio Backtest Results: CompositeAlphaBreakout (Updated)

We have successfully refined the `CompositeAlphaBreakout` strategy by implementing strong trend alignment, volatility filters (ATR), and proper realistic capital allocation (10 Lakh INR / ~2 Nifty Lots) to properly account for the 60₹ round-trip fee. The strategy logic was also debugged to fix string matching that was completely blocking signals for Nifty 50.

## 📊 Global Portfolio Results (₹1,000,000 / trade / ₹60 fee)

| Metric | Value |
|--------|-------|
| **Processed Ticks** | 3,323,160 |
| **Total Signals** | 1,980 |
| **Winning Trades** | 1,426 |
| **Losing Trades** | 554 |
| **Win Rate** | **72.02%** |
| **Gross PNL** | ₹1,003,383.70 |
| **Net PNL (After Fees)** | **₹884,583.70** |
| **Max Drawdown** | ₹94,737.54 |

---

## 🔬 Individual Symbol Breakdown

### 1. NSE_INDEX|Nifty 50
- **Signals**: 982
- **Wins**: 690 | **Losses**: 292
- **Win Rate**: 70.26%
- **Net PNL**: **₹350,504.10**
- **Max Drawdown**: ₹21,262.63

### 2. NSE_INDEX|Nifty Bank
- **Signals**: 440
- **Wins**: 346 | **Losses**: 94
- **Win Rate**: 78.64%
- **Net PNL**: **₹152,854.43**
- **Max Drawdown**: ₹30,745.61

### 3. NSE_INDEX|Nifty Fin Service
- **Signals**: 182
- **Wins**: 144 | **Losses**: 38
- **Win Rate**: 79.12%
- **Net PNL**: **₹170,020.98**
- **Max Drawdown**: ₹15,716.11

### 4. NSE_INDEX|Nifty 100
- **Signals**: 91
- **Wins**: 75 | **Losses**: 16
- **Win Rate**: 82.42%
- **Net PNL**: **₹176,280.14**
- **Max Drawdown**: ₹2,242.83

### 5. NSE_INDEX|Nifty Next 50
- **Signals**: 285
- **Wins**: 171 | **Losses**: 114
- **Win Rate**: 60.00%
- **Net PNL**: **₹34,924.05**
- **Max Drawdown**: ₹78,591.99

---

## 🛠️ Key Improvements

1. **Realistic Fee Modeling**: A micro-scalping strategy with ₹10,000 capital can mathematically never overcome a ₹60 fee (since the target is smaller than the fee). By trading a realistic size of 2 Nifty Lots (₹10 Lakh notional value), the profits scale appropriately and the fee drag is easily overcome.
2. **Circuit Breaker implementation**: Evaluates all signals unrestrictedly on profitable streaks but halts all new trades across the portfolio after accumulating exactly 3 losses per day, preserving generated capital.
3. **Volatility Filter**: Only triggers trades if the `ATR` > 1.0 point (100 paise), preventing the system from over-trading in dead sideways chop.
4. **Strong Trend Alignment**: Demands that `Price > Fast EMA > Slow EMA > Trend EMA`, capturing momentum pushes with extremely high conviction (72% win rate globally).
