import pandas as pd
import pandas_ta as ta

print("Loading market data...")
df = pd.read_csv("market_data_top15.csv", parse_dates=["timestamp"])
df.set_index("timestamp", inplace=True)

symbols = df['symbol'].unique()
timeframes = {'1min': '1min', '5min': '5min', '15min': '15min'}

results = []

for symbol in symbols:
    sym_df = df[df['symbol'] == symbol]
    
    for tf_name, tf in timeframes.items():
        ohlc = sym_df['price'].resample(tf).ohlc()
        ohlc.dropna(inplace=True)
        
        if len(ohlc) < 50:
            continue
        
        ohlc['EMA_9'] = ta.ema(ohlc['close'], length=9)
        ohlc['EMA_21'] = ta.ema(ohlc['close'], length=21)
        ohlc['EMA_50'] = ta.ema(ohlc['close'], length=50)
        ohlc['ATR'] = ta.atr(ohlc['high'], ohlc['low'], ohlc['close'], length=14)
        
        donchian = ta.donchian(ohlc['high'], ohlc['low'], lower_length=20, upper_length=20)
        if donchian is not None and not donchian.empty:
            ohlc['DC_LOWER'] = donchian.iloc[:, 0]
            ohlc['DC_UPPER'] = donchian.iloc[:, 2]
        else:
            continue
            
        ohlc.dropna(inplace=True)
        
        long_condition = (ohlc['close'] > ohlc['EMA_9']) & (ohlc['EMA_21'] > ohlc['EMA_50']) & (ohlc['close'] >= ohlc['DC_UPPER']) & (ohlc['ATR'] > 1.0)
        short_condition = (ohlc['close'] < ohlc['EMA_9']) & (ohlc['EMA_21'] < ohlc['EMA_50']) & (ohlc['close'] <= ohlc['DC_LOWER']) & (ohlc['ATR'] > 1.0)
        
        position = 0
        entry_price = 0.0
        trades = []
        
        inr_pnl = 0.0
        
        # Simple stop-and-reverse backtest simulation
        for i in range(1, len(ohlc)):
            prev_long = long_condition.iloc[i-1]
            prev_short = short_condition.iloc[i-1]
            current_open = ohlc['open'].iloc[i]
            
            if position == 0:
                if prev_long:
                    position = 1
                    entry_price = current_open
                elif prev_short:
                    position = -1
                    entry_price = current_open
            elif position == 1:
                if prev_short: # Stop and reverse
                    pnl_points = current_open - entry_price
                    qty = 10_00_000 / entry_price
                    trade_inr = qty * pnl_points
                    trades.append(trade_inr)
                    inr_pnl += trade_inr
                    
                    position = -1
                    entry_price = current_open
            elif position == -1:
                if prev_long: # Stop and reverse
                    pnl_points = entry_price - current_open
                    qty = 10_00_000 / entry_price
                    trade_inr = qty * pnl_points
                    trades.append(trade_inr)
                    inr_pnl += trade_inr
                    
                    position = 1
                    entry_price = current_open
                    
        total_trades = len(trades)
        wins = len([t for t in trades if t > 0])
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
        results.append({
            'Symbol': symbol,
            'Timeframe': tf_name,
            'Trades': total_trades,
            'Win Rate': f"{win_rate:.1f}%",
            'Net Profit': f"INR {inr_pnl:,.2f}"
        })

print("\n--- OHLC MULTI-TIMEFRAME BACKTEST RESULTS ---")
res_df = pd.DataFrame(results)
print(res_df.to_string(index=False))
