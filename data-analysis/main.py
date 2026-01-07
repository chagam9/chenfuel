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

    # --- Fetch Rate ---
    usd_ils_rate = get_usd_ils_rate()

    # --- Normalize to ILS ---
    # Create normalized columns
    def normalize(row, col_name):
        val = row[col_name]
        if row['currency'] == 'USD':
            return val * usd_ils_rate
        return val

    # We need to ensure we don't crash if currency is missing
    df['profit_loss_ils'] = df.apply(lambda row: normalize(row, 'profit_loss'), axis=1)
    df['fees_ils'] = df.apply(lambda row: normalize(row, 'fees'), axis=1)
    # Tax is already split into 'tax_il' (ILS) and 'tax_foreign' (likely ILS converted at source, or USD?)
    # The header is 'מס חו"ל בשקלים' which means 'Foreign Tax in Shekels'. So it is ALREADY ILS.
    # 'מס שנוכה/הוחזר בארץ' is also ILS.
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
    total_tax_ils = df['total_tax_ils'].sum() # Already ILS
    
    # Invested (Risk) Calculation
    # Filter: Sales Only
    # Invested = Proceeds (Net Amount) - Gross Profit
    # Use Normalized columns!
    
    sales_indices = df['action_en'] == 'Sell'
    total_proceeds_ils = df.loc[sales_indices, 'net_amount_ils'].sum()
    total_gross_pl_sales_ils = df.loc[sales_indices, 'profit_loss_ils'].sum()
    
    total_invested_ils = total_proceeds_ils - total_gross_pl_sales_ils
    
    # Net ROI
    # ROI = (Net Profit / Invested) * 100
    # Net Profit = Gross PL + Fees - Tax
    # Note: Fees are usually negative. If CSV has them positive, logic reverses.
    # Assuming fees are negative/deduction in 'profit_loss'? No, 'profit_loss' is usually Gross.
    # Let's assume standard: Net = Gross + Fees (if neg) - Tax.
    
    total_net_return_ils = total_pl_gross_ils + total_fees_ils - total_tax_ils
    
    roi_percentage = 0.0
    if total_invested_ils != 0:
        roi_percentage = (total_net_return_ils / total_invested_ils) * 100

    # Chart P/L (Using original values or normalized? Normalized is better for comparison)
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
    dashboard_data = {
        "summary": {
            "total_pl": total_pl_gross_ils,
            "total_fees": total_fees_ils,
            "total_tax": total_tax_ils,
            "total_invested": total_invested_ils,
            "roi_percentage": roi_percentage,
            "total_net_return": total_net_return_ils,
            "exchange_rate": usd_ils_rate
        },
        "charts": {
            "pl_by_security": chart_pl_data,
            "currency_distribution": chart_currency_data
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
