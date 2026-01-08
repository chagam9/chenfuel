import pandas as pd
import json
import os
import yfinance as yf
import time
import math

# --- Configuration & Mappings ---
# Mapping Hebrew/Text names from CSV to Yahoo Finance Tickers
# Note: TASE stocks need '.TA' suffix.
NAME_TO_TICKER = {
    # US Stocks
    "PAYPAL HOLDINGS": "PYPL",
    "INTEL CORP": "INTC",
    "NVIDIA CORP": "NVDA",
    "VERIZON COMMUNICATI": "VZ",
    "JOHNSON&JOHNSON": "JNJ",
    "CHEVRON CORP": "CVX",
    "ROBLOX CORP - A": "RBLX",
    "SPDR-COMM SERV": "XLC",
    "ARK INNOVAT ETF": "ARKK",
    "CELLEBRITE DI LT": "CLBT",
    "ADV MICRO DEVICE": "AMD",
    "AMAZON.COM INC": "AMZN",
    "ALPHABET INC-A": "GOOGL",
    "TESLA INC": "TSLA",
    "SOFI TECHNOLOGIE": "SOFI",
    "PALANTIR TECHN-A": "PLTR",
    "PALO ALTO NETWORKS": "PANW",
    "APPLE INC": "AAPL",
    "BOEING CO/THE": "BA",
    "WALMART INC": "WMT",
    "ORACLE CORP": "ORCL",
    "DEERE & CO": "DE",
    "JOBY AVIATION IN": "JOBY",
    "ARCHER AVIATION": "ACHR",
    "ZIM INTEGRATED S": "ZIM",
    "ISHARES MSCI JPN": "EWJ",
    "ISHARES MSCI CHI": "MCHI",
    "ISHARES MSCI CAN": "EWC",
    "ISHARES SILVER TRUS": "SLV",
    "VANECK GOLD MINE": "GDX",
    "SPDR GOLD SHARES": "GLD",
    "ALIBABA GRP-ADR": "BABA",
    "NIO INC - ADR": "NIO",
    "TAIWAN SEMIC-ADR": "TSM",
    "ISHARES LITH MP": "LIT",
    "GLOBAL X URANIUM ET": "URA",
    "CAMECO CORP": "CCJ",
    "ENERGY FUELS INC": "UUUU",
    "ALBEMARLE CORP": "ALB",
    "BHP GROUP-ADR": "BHP",
    "ALCOA CORP": "AA",
    "PFIZER INC": "PFE",
    "COCA-COLA CO/THE": "KO",
    "TRUMP MEDIA &": "DJT",
    "RIGETTI COMPUTIN": "RGTI",
    "DRAGANFLY INC": "DPRO",
    "VERTICAL AEROSPA": "EVTL",
    "TEMPUS AI INC": "TEM",
    "MIND MEDICINE MI": "MNMD",
    "QUANTUM COMPUTIN": "QUBT",
    "SOUNDHOUND AI-A": "SOUN",
    "PONY AI INC": "PONY",
    "OSCAR HEALTH -A": "OSCR",
    "CHEGG INC": "CHGG",
    "A/S PUR US CANN": "MSOS", 
    "ABRDN PALLADIUM": "PALL",
    "ABRDN PLATINUM E": "PPLT",
    "SPDR-INDU SELECT": "XLI",
    "SPDR-CONS STAPLE": "XLP",
    "SOUTHERN CO": "SO",
    "ARCHER-DANIELS": "ADM",

    # Israeli Stocks (TASE)
    "פועלים": "POLI.TA",
    "לאומי": "LUMI.TA",
    "דיסקונט       א": "DSCT.TA",
    "מזרחי טפחות": "MZTF.TA",
    "בינלאומי": "FIBI.TA",
    "טבע": "TEVA.TA",
    "אלביט מערכות": "ESLT.TA",
    "טאואר": "TSEM.TA",
    "איי.סי.אל": "ICL.TA",
    "פורמולה מערכות": "FORTY.TA",
    "נובה": "NVMI.TA",
    "קמטק": "CAMT.TA",
    "נאייקס": "NYAX.TA",
    "פוקס": "FOX.TA",
    "דיסקונט השקעות": "DISI.TA",
}

# Translate Columns
COL_MAP = {
    'תאריך ביצוע': 'date',
    'שם ני"ע': 'symbol',
    'פעולה': 'action',
    'כמות ביצוע': 'quantity',
    'שער ביצוע': 'price',
    'מטבע': 'currency',
    'עמלות ודמי ניהול': 'fees',
    'רווח/הפסד': 'profit_loss',
    'מס שנוכה/הוחזר בארץ': 'tax_il',
    'מס חו"ל בשקלים': 'tax_foreign',
    'תמורה נטו לפני מס': 'net_amount'
}

def clean_money(val):
    if isinstance(val, str):
        return float(val.replace(',', ''))
    return float(val)

def get_current_prices(tickers):
    if not tickers:
        return {}
    
    print(f"Fetching prices for {len(tickers)} tickers...")
    try:
        data = yf.download(tickers, period="1d", progress=False)['Close']
        current_prices = {}
        if isinstance(data, pd.Series):
            val = data.iloc[-1]
            current_prices[tickers[0]] = float(val)
        elif isinstance(data, pd.DataFrame):
            latest = data.iloc[-1]
            for ticker in tickers:
                try:
                    val = latest[ticker]
                    if pd.notna(val):
                        current_prices[ticker] = float(val)
                except KeyError:
                    pass
        return current_prices
    except Exception as e:
        print(f"Error fetching prices: {e}")
        return {}

def get_usd_ils_rate():
    print("Fetching USD/ILS exchange rate...")
    try:
        # Ticker for USD to ILS (Yahoo Finance standard)
        ticker = "USDILS=X" 
        data = yf.download(ticker, period="1d", progress=False)['Close']
        if isinstance(data, pd.Series):
             rate = float(data.iloc[-1])
        elif isinstance(data, pd.DataFrame):
             rate = float(data.iloc[-1].iloc[0]) # Handle potential dataframe structure
        print(f"Current USD/ILS Rate: {rate:.4f}")
        return rate
    except Exception as e:
        print(f"Error fetching exchange rate, defaulting to 3.65: {e}")
        return 3.65

def main():
    print("Starting Chenfuel Portfolio Opportunity Analysis [English]...")
    
    csv_path = "data.csv"
    output_dir = "/app/web"
    output_file = os.path.join(output_dir, "dashboard_data.json")

    if not os.path.exists(csv_path):
        print("CSV not found.")
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Clean numeric columns
    numeric_hebrew_cols = ['רווח/הפסד', 'עמלות ודמי ניהול', 'מס שנוכה/הוחזר בארץ', 'מס חו"ל בשקלים', 'כמות ביצוע', 'שער ביצוע', 'תמורה נטו לפני מס']
    for col in numeric_hebrew_cols:
         if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0)
    
    df['שם ני"ע'] = df['שם ני"ע'].fillna('Unknown')
    df['מטבע'] = df['מטבע'].fillna('Unknown')

    # Rename Columns to English for ease of use in Frontend
    df.rename(columns=COL_MAP, inplace=True)
    
    # Standardize Currency
    # 'דולר' -> 'USD', 'ש"ח' -> 'ILS'
    if 'currency' in df.columns:
        df['currency'] = df['currency'].replace({'דולר': 'USD', 'ש"ח': 'ILS'})

    # Parse Dates
    df['date_obj'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
    
    # --- Fetch Historical Rates ---
    # Find range
    min_date = df['date_obj'].min()
    max_date = df['date_obj'].max()
    
    # Add buffer
    start_date = (min_date - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
    end_date = (max_date + pd.Timedelta(days=5)).strftime('%Y-%m-%d')
    
    print(f"Fetching USD/ILS history from {start_date} to {end_date}...")
    
    try:
        usd_ils_history = yf.download("USDILS=X", start=start_date, end=end_date, progress=False)['Close']
        if isinstance(usd_ils_history, pd.DataFrame):
             usd_ils_history = usd_ils_history.iloc[:, 0] # Take first column if DF
        
        # Fill missing dates (weekends) with previous rate
        full_idx = pd.date_range(start=start_date, end=end_date)
        usd_ils_history = usd_ils_history.reindex(full_idx).ffill().bfill()
        
        # Create lookup dict (date string YYYY-MM-DD -> rate or timestamp -> rate)
        # We will use .loc with the date object
    except Exception as e:
        print(f"Error fetching history: {e}. Using fallback 3.65")
        usd_ils_history = None

    # --- Normalize to ILS ---
    def get_rate_for_date(date_obj):
        if pd.isna(date_obj) or usd_ils_history is None:
            return 3.65 # Fallback
        try:
            # We need to access by timestamp or date string
            # Reindexed series uses Timestamp
            # Normalize date_obj to midnight
            ts = pd.Timestamp(date_obj.year, date_obj.month, date_obj.day)
            if ts in usd_ils_history.index:
                return float(usd_ils_history.loc[ts])
            # If not exact match (shouldn't happen with reindex), try fallback
            return 3.65
        except:
            return 3.65

    # Create normalized columns
    def normalize(row, col_name):
        val = row[col_name]
        if row['currency'] == 'USD':
            rate = get_rate_for_date(row['date_obj'])
            return val * rate
        return val

    # We need to ensure we don't crash if currency is missing
    df['profit_loss_ils'] = df.apply(lambda row: normalize(row, 'profit_loss'), axis=1)
    df['fees_ils'] = df.apply(lambda row: normalize(row, 'fees'), axis=1)
    # Tax is already split into 'tax_il' (ILS) and 'tax_foreign' (likely ILS converted at source, or USD?)
    # header 'מס חו"ל בשקלים' implies ILS.
    df['total_tax_ils'] = df['tax_il'] + df['tax_foreign']
    
    df['net_amount_ils'] = df.apply(lambda row: normalize(row, 'net_amount'), axis=1)

    # --- 1. Identify Sold Positions & Tickers ---
    if 'action' in df.columns:
        df['action_en'] = df['action'].replace({'קניה': 'Buy', 'מכירה': 'Sell'})
    
    sales_df = df[df['action'].str.contains('מכירה', na=False)].copy()
    
    # Map to tickers
    unique_names = sales_df['symbol'].unique()
    relevant_tickers = []
    name_to_ticker_map = {}
    
    for name in unique_names:
        if name in NAME_TO_TICKER:
            ticker = NAME_TO_TICKER[name]
            relevant_tickers.append(ticker)
            name_to_ticker_map[name] = ticker

    # --- 2. Fetch Prices ---
    current_prices = get_current_prices(list(set(relevant_tickers)))

    # --- 3. Calculate Opportunity Cost ---
    opportunity_list = []
    
    for idx, row in sales_df.iterrows():
        name = row['symbol']
        if name not in name_to_ticker_map:
            continue
            
        ticker = name_to_ticker_map[name]
        try:
            current_price = current_prices.get(ticker)
        except:
            continue
            
        if current_price is None:
            continue
        
        sale_price = row['price']
        qty_sold = abs(row['quantity'])
        
        diff_per_unit = current_price - sale_price
        total_missed = diff_per_unit * qty_sold
        
        currency = row['currency']
        
        opportunity_list.append({
            "date": row['date'],
            "name": name,
            "ticker": ticker,
            "qty": qty_sold,
            "sale_price": sale_price,
            "current_price": current_price,
            "diff_per_unit": diff_per_unit,
            "total_missed": total_missed,
            "currency": currency,
            "is_success": (total_missed < 0) 
        })

    # --- 4. Summary & Charts ---
    # Global Totals (Normalized to ILS)
    total_pl_gross_ils = df['profit_loss_ils'].sum()
    total_fees_ils = df['fees_ils'].sum()
    total_tax_ils = df['total_tax_ils'].sum() 
    
    # --- MAX EXPOSURE & ROAC ALGORITHM ---
    exposure_df = df.copy()
    exposure_df.sort_values('date_obj', inplace=True)
    
    def calculate_capital_delta(row):
        net_val = row['net_amount_ils']
        action = row['action_en']
        if action == 'Buy':
            return abs(net_val)
        elif action == 'Sell':
            proceeds = abs(net_val)
            profit = row['profit_loss_ils']
            principal = proceeds - profit
            return -1 * principal
        return 0

    exposure_df['capital_delta'] = exposure_df.apply(calculate_capital_delta, axis=1)
    
    # Daily Aggregation
    daily_deltas = exposure_df.groupby('date_obj')['capital_delta'].sum()
    
    # Exposure Series
    exposure_series = daily_deltas.cumsum()
    # Ensure no negative exposure (baseline issues)
    exposure_series = exposure_series.clip(lower=0)
    
    # 1. Max Exposure
    max_exposure_ils = exposure_series.max() if not exposure_series.empty else 0
    
    # 2. Average Exposure (for ROAC)
    avg_exposure_ils = exposure_series.mean() if not exposure_series.empty else 0
    exposure_chart_data = [{"date": ts.strftime('%Y-%m-%d'), "val": val} for ts, val in exposure_series.items()]

    # --- ADVANCED METRICS ---
    
    # 3. Profit Factor & Win Rate
    # Filter for Sales (Realized Events)
    sales = df.loc[df['action_en'] == 'Sell']
    winners = sales.loc[sales['profit_loss_ils'] > 0, 'profit_loss_ils']
    losers = sales.loc[sales['profit_loss_ils'] <= 0, 'profit_loss_ils'] # Includes 0 as not win
    
    total_win_amt = winners.sum()
    total_loss_amt = abs(losers.sum())
    
    profit_factor = 0
    if total_loss_amt == 0:
        profit_factor = 999.0 if total_win_amt > 0 else 0
    else:
        profit_factor = total_win_amt / total_loss_amt
        
    win_rate = 0
    total_trades = len(sales)
    if total_trades > 0:
        win_rate = (len(winners) / total_trades) * 100

    # 4. ROAC (Return on Average Capital)
    total_net_return_ils = total_pl_gross_ils + total_fees_ils - total_tax_ils
    roac_percentage = 0
    if avg_exposure_ils > 0:
        roac_percentage = (total_net_return_ils / avg_exposure_ils) * 100

    # 5. Max Drawdown (MDD)
    # Construct Daily P/L Series
    # Group profit_loss_ils by date
    daily_pl = df.groupby('date_obj')['profit_loss_ils'].sum()
    # Reindex to match exposure series (all dates)
    full_idx = exposure_series.index
    daily_pl = daily_pl.reindex(full_idx).fillna(0)
    
    # Cumulative P/L (Equity Curve approximation starting from 0)
    equity_curve = daily_pl.cumsum()
    
    # Calculate Drawdown
    running_max = equity_curve.cummax()
    drawdown = running_max - equity_curve # Positive value representing the drop
    
    max_drawdown_ils = drawdown.max()
    
    # 6. Sharpe Ratio (Annualized)
    # Daily Return % = Daily P/L / Daily Capital At Risk
    # Avoid division by zero
    
    daily_returns_pct = pd.Series(index=daily_pl.index, dtype=float)
    
    # Align P/L and Exposure
    for dt in daily_pl.index:
        cap = exposure_series.loc[dt]
        pl = daily_pl.loc[dt]
        if cap > 1000: # Ignore noise on tiny capital
            daily_returns_pct.loc[dt] = pl / cap
        else:
            daily_returns_pct.loc[dt] = 0.0
            
    # Calculate Sharpe
    # Assume Rf = 0 (Risk Free Rate)
    mean_daily_ret = daily_returns_pct.mean()
    std_daily_ret = daily_returns_pct.std()
    
    sharpe_ratio = 0
    if std_daily_ret > 0:
        sharpe_ratio = (mean_daily_ret / std_daily_ret) * (252**0.5)

    # Net ROI (on Peak Capital) - "Return on Risk"
    roi_percentage = 0.0
    if max_exposure_ils > 0:
        roi_percentage = (total_net_return_ils / max_exposure_ils) * 100

    security_pl = df.groupby('symbol')['profit_loss_ils'].sum().reset_index()
    security_pl.columns = ['name', 'val']
    security_pl = security_pl.sort_values('val', ascending=False)
    chart_pl_data = pd.concat([security_pl.head(5), security_pl.tail(5)]).drop_duplicates().to_dict(orient='records')

    currency_counts = df['currency'].value_counts().reset_index()
    currency_counts.columns = ['currency', 'count']
    chart_currency_data = {"labels": currency_counts['currency'].tolist(), "data": currency_counts['count'].tolist()}

    # --- Versioning ---
    version_file = "/app/version.txt"
    current_version = 0.00
    
    if os.path.exists(version_file):
        try:
            with open(version_file, "r") as f:
                content = f.read().strip()
                if content:
                    current_version = float(content)
        except:
            pass

    new_version = round(current_version + 0.01, 2)
    
    try:
        with open(version_file, "w") as f:
            f.write(f"{new_version:.2f}")
        print(f"Version updated to: {new_version}")
    except Exception as e:
        print(f"Warning: Could not save version file: {e}")

    # --- 5. Final Output ---
    # User Request: Main "Risk" Card should be Average Exposure, and ROI should be ROAC.
    
    analysis_start = df['date_obj'].min().strftime('%d/%m/%Y')
    analysis_end = df['date_obj'].max().strftime('%d/%m/%Y')
    
    dashboard_data = {
        "summary": {
            "total_pl": total_pl_gross_ils,
            "total_fees": total_fees_ils,
            "total_tax": total_tax_ils,
            "total_invested": avg_exposure_ils, # Now Average
            "roi_percentage": roac_percentage,  # Now ROAC
            "total_net_return": total_net_return_ils,
            "exchange_rate": "Historical (Date Specific)",
            "period_start": analysis_start,
            "period_end": analysis_end,
            "advanced_metrics": {
                "roac_percentage": roac_percentage,
                "profit_factor": profit_factor,
                "win_rate": win_rate,
                "max_drawdown": max_drawdown_ils,
                "sharpe_ratio": sharpe_ratio,
                "max_exposure": max_exposure_ils # Preserved here
            }
        },
        "charts": {
            "pl_by_security": chart_pl_data,
            "currency_distribution": chart_currency_data,
            "exposure_history": exposure_chart_data
        },
        "what_if": {
             "opportunities": opportunity_list,
             "top_regrets": sorted([x for x in opportunity_list if not x['is_success']], key=lambda x: x['total_missed'], reverse=True)[:5],
             "top_smart_moves": sorted([x for x in opportunity_list if x['is_success']], key=lambda x: x['total_missed'])[:5]
        },
        "transactions": df.to_dict(orient='records'),
        "metadata": {
            "row_count": len(df),
            "generated_at": pd.Timestamp.now().isoformat(),
            "version": f"{new_version:.2f}"
        }
    }

    print(f"\n--- Opportunity Analysis ---")
    print(f"Analyzed {len(opportunity_list)} sales events.")
    print(f"Saving dashboard data to {output_file}...")
    
    # Robust NaN Sanitization
    def sanitize(obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj
        elif isinstance(obj, dict):
            return {k: sanitize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [sanitize(v) for v in obj]
        return obj

    dashboard_data_clean = sanitize(dashboard_data)

    with open(output_file, "w") as f:
        json.dump(dashboard_data_clean, f, indent=4, default=str)
        
    print("Analysis Complete.")

if __name__ == "__main__":
    main()
