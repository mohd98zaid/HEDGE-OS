import pandas as pd
import pandas_ta as ta
import plotly.graph_objects as go
import os
import sys

print("Loading market data...")
df = pd.read_csv("market_data_stocks.csv", parse_dates=["timestamp"])
df.set_index("timestamp", inplace=True)

symbols = df['symbol'].unique()
timeframes = {'1min': '1min', '5min': '5min', '15min': '15min'}

print(f"Found symbols: {symbols}")

artifact_dir = r"C:\Users\Xaid\.gemini\antigravity-ide\brain\55183bf2-409a-4c72-8a57-ea9c03e8f117"

for symbol in symbols:
    print(f"Processing {symbol}...")
    sym_df = df[df['symbol'] == symbol]
    
    for tf_name, tf in timeframes.items():
        print(f"  Resampling {tf_name}...")
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
        
        # Reduce points to plot to make the HTML lighter
        plot_df = ohlc.tail(1000) # Plot last 1000 candles
        
        fig = go.Figure(data=[go.Candlestick(x=plot_df.index,
                        open=plot_df['open'], high=plot_df['high'],
                        low=plot_df['low'], close=plot_df['close'],
                        name='OHLC')])
                        
        fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df['EMA_9'], name='EMA 9', line=dict(color='blue')))
        fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df['EMA_21'], name='EMA 21', line=dict(color='orange')))
        fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df['EMA_50'], name='EMA 50', line=dict(color='white')))
        fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df['DC_UPPER'], name='DC Upper', line=dict(color='green', dash='dot')))
        fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df['DC_LOWER'], name='DC Lower', line=dict(color='red', dash='dot')))
        
        buys = plot_df[long_condition.reindex(plot_df.index, fill_value=False)]
        sells = plot_df[short_condition.reindex(plot_df.index, fill_value=False)]
        
        fig.add_trace(go.Scatter(x=buys.index, y=buys['low'] - buys['ATR'], mode='markers', marker=dict(symbol='triangle-up', size=12, color='#00ff00', line=dict(width=2, color='DarkSlateGrey')), name='Buy Signal'))
        fig.add_trace(go.Scatter(x=sells.index, y=sells['high'] + sells['ATR'], mode='markers', marker=dict(symbol='triangle-down', size=12, color='#ff0000', line=dict(width=2, color='DarkSlateGrey')), name='Sell Signal'))
        
        fig.update_layout(
            title=f"CompositeAlphaBreakout | {symbol} | {tf_name}", 
            template='plotly_dark',
            xaxis_rangeslider_visible=False,
            height=800
        )
        
        safe_sym = symbol.replace(' ', '_').replace('|', '_')
        filename = f"{safe_sym}_{tf_name}.html"
        filepath = os.path.join(artifact_dir, filename)
        
        fig.write_html(filepath)
        print(f"    Saved {filepath}")

print("Done!")
