import pandas as pd
import sys
import os

def analyze_decisions(file_path):
    print(f"--- Analyzing {file_path} ---")
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist.")
        return

    try:
        # Read only necessary columns to save memory
        df = pd.read_csv(file_path)
        
        if df.empty:
            print("File is empty.")
            return

        print(f"Total Rows: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        
        # Check for 'action' column (adjust if your CSV uses different names)
        # Based on previous context, columns might be: timestamp, symbol, price, nobi, rsi, volume_ratio, vwap_gap_pct, ml_confidence, decision, reason
        # The user's code might use 'decision' instead of 'action'
        action_col = 'decision' if 'decision' in df.columns else 'action'
        
        if action_col not in df.columns:
            print(f"Error: '{action_col}' column not found.")
            print("First 5 rows:")
            print(df.head())
            return

        # 1. Action Distribution
        print(f"\n[Action/Decision Distribution]")
        print(df[action_col].value_counts())

        # 2. Trade Analysis (if 'pnl' or 'profit' column exists)
        # Adjust column names based on your actual CSV structure
        pnl_col = next((col for col in ['pnl', 'profit', 'realized_pnl'] if col in df.columns), None)
        
        if pnl_col:
            trades = df[df[pnl_col].notna() & (df[pnl_col] != 0)]
            if not trades.empty:
                total_pnl = trades[pnl_col].sum()
                win_trades = trades[trades[pnl_col] > 0]
                loss_trades = trades[trades[pnl_col] <= 0]
                win_rate = len(win_trades) / len(trades) * 100
                
                print("\n[Performance Metrics]")
                print(f"Total PnL: ${total_pnl:.2f}")
                print(f"Total Trades: {len(trades)}")
                print(f"Win Rate: {win_rate:.2f}%")
                print(f"Avg Win: ${win_trades[pnl_col].mean():.2f}")
                print(f"Avg Loss: ${loss_trades[pnl_col].mean():.2f}")
            else:
                print("\nNo realized trades found in this file.")
        else:
            print("\nNo PnL column found. Cannot calculate profitability.")

        # 3. Sample Data
        print("\n[Last 5 Decisions]")
        cols_to_show = ['timestamp', 'symbol', action_col, 'price']
        if 'confidence' in df.columns: cols_to_show.append('confidence')
        if 'ml_confidence' in df.columns: cols_to_show.append('ml_confidence')
        if 'reason' in df.columns: cols_to_show.append('reason')
        
        # Filter only existing columns
        cols_to_show = [c for c in cols_to_show if c in df.columns]
        
        print(df.tail(5)[cols_to_show].to_string(index=False))

    except Exception as e:
        print(f"Error analyzing file: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_logs.py <path_to_csv>")
    else:
        analyze_decisions(sys.argv[1])
