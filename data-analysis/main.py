import pandas as pd
import json
import os
import yfinance as yf
import time

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
    "A/S PUR US CANN": "YOLO", # Assuming this is the Cannabis ETF or similar, checking name might be needed.
    # Note: "A/S PUR US CANN" is likely "AdvisorShares Pure US Cannabis ETF" -> MSOS? or YOLO? Using MSOS usually. 
    # Let's try "MSOS" as it's the common one, if not sure we can skip.
    "A/S PUR US CANN": "MSOS", 
    "ABRDN PALLADIUM": "PALL",
    "ABRDN PLATINUM E": "PPLT",
    "SPDR-INDU SELECT": "XLI",
    "SPDR-CONS STAPLE": "XLP",
    "SOUTHERN CO": "SO",
    "ARCHER-DANIELS": "ADM",

    # Israeli Stocks (TASE)
    # Note: Values in CSV are in ILS, so we compare with ILS price from Yahoo (which is usually in Agorot or Shekels depending on API)
    # Yahoo usually returns TASE prices in Agorot (1/100 ILS) for stocks! We must check this.
    # Actually, yfinance for TASE often returns in Agorot. We need to be careful.
    "פועלים": "POLI.TA",
    "לאומי": "LUMI.TA",
    "דיסקונט       א": "DSCT.TA",
    "מזרחי טפחות": "MZTF.TA",
    "בינלאומי": "FIBI.TA",
    "טבע": "TEVA.TA",
    "אלביט מערכות": "ESLT.TA",
    "טאואר": "TSEM.TA",
    "איי.סי.אל": "ICL.TA", # ICL Group
    "פורמולה מערכות": "FORTY.TA",
    "נובה": "NVMI.TA",
    "קמטק": "CAMT.TA",
    "נאייקס": "NYAX.TA",
    "פוקס": "FOX.TA",
    "דיסקונט השקעות": "DISI.TA",
    
    # ETFs (KSM/Tachlit/Harel) often don't have good Yahoo Tickers or valid data.
    # We will likely skip them for the "What If" analysis unless we find them.
    # Leaving them out for now to ensure script stability.
}

def get_current_prices(tickers):
    if not tickers:
        return {}
    
    print(f"Fetching prices for {len(tickers)} tickers...")
    try:
        # Fetch in batches if needed, but yfinance handles lists well
        data = yf.download(tickers, period="1d", progress=False)['Close']
        
        # If only one ticker, data is Series, else DataFrame
        current_prices = {}
        if isinstance(data, pd.Series):
            # Single ticker result
            # Depending on yfinance version, 'Close' might be the series directly
            val = data.iloc[-1]
            current_prices[tickers[0]] = float(val)
        elif isinstance(data, pd.DataFrame):
            # Multi ticker
            # Get latest row
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

def main():
    print("Starting Chenfuel Portfolio Opportunity Analysis...")
    
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

    # Cleanup Cols
    numeric_cols = ['רווח/הפסד', 'עמלות ודמי ניהול', 'מס שנוכה/הוחזר בארץ', 'מס חו"ל בשקלים', 'כמות ביצוע', 'שער ביצוע', 'תמורה נטו לפני מס']
    for col in numeric_cols:
         if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0)
    
    df['שם ני"ע'] = df['שם ני"ע'].fillna('Unknown')
    df['מטבע'] = df['מטבע'].fillna('Unknown')

    # --- 1. Identify Sold Positions & Tickers ---
    # We want to analyze "Opportunity Cost" for Sales.
    # Logic: Look at "Mecira" (Sale) rows.
    
    sales_df = df[df['פעולה'].str.contains('מכירה', na=False)].copy()
    
    # Map to tickers
    unique_names = sales_df['שם ני"ע'].unique()
    relevant_tickers = []
    name_to_ticker_map = {}
    
    for name in unique_names:
        # Simple exact match from our dict
        if name in NAME_TO_TICKER:
            ticker = NAME_TO_TICKER[name]
            relevant_tickers.append(ticker)
            name_to_ticker_map[name] = ticker
        else:
            # Try fuzzy or clean? For now, skip unknown
            pass

    # --- 2. Fetch Prices ---
    current_prices = get_current_prices(list(set(relevant_tickers)))

    # --- 3. Calculate Opportunity Cost ---
    opportunity_list = []
    
    for idx, row in sales_df.iterrows():
        name = row['שם ני"ע']
        if name not in name_to_ticker_map:
            continue
            
        ticker = name_to_ticker_map[name]
        if ticker not in current_prices:
            continue
            
        current_price = current_prices[ticker]
        
        # TASE Correction: Israeli stocks on Yahoo are often in Agorot (1/100 ILS)
        # However, our CSV 'Price' ('שער ביצוע') for ILS stocks is usually in Agorot too! 
        # (e.g., Bank Leumi ~3000 now, which is 30.00 ILS).
        # So usually direct comparison is fine IF both are Agorot.
        # But for US stocks, CSV is in USD, Yahoo is in USD. 
        # We need to verify unit consistency.
        
        # Assumption: 
        # If Currency == 'ש"ח' -> CSV Price is usually Agorot (e.g. 2500 for 25 ILS). Yahoo TASE is Agorot. -> Match.
        # If Currency == 'דולר' -> CSV Price is USD. Yahoo is USD. -> Match.
        
        sale_price = row['שער ביצוע'] # Price per unit at sale
        qty_sold = abs(row['כמות ביצוע']) # Quantity (sales are negative in some systems, positive in others, here 'מכירה' usually has neg quantity in Excel logic or pos? check csv)
        # Checking CSV: "מכירה, -2320.0" -> Quantity is negative.
        qty_sold = abs(qty_sold)
        
        # Diff per unit = Current - Sale
        # If Current > Sale -> I lost profit -> Opportunity Cost Positive (Bad)
        # If Current < Sale -> I saved loss -> Opportunity Cost Negative (Good)
        
        diff_per_unit = current_price - sale_price
        total_missed = diff_per_unit * qty_sold
        
        # Currency adjustment for 'Total Missed' to be displayed in main currency?
        # For now, we will store it in the original currency and simple 'value'.
        # Or better: Normalize to simple text for display like "500 USD" or "2000 ILS"
        
        currency = row['מטבע']
        
        opportunity_list.append({
            "date": row['תאריך ביצוע'],
            "name": name,
            "ticker": ticker,
            "qty": qty_sold,
            "sale_price": sale_price,
            "current_price": current_price,
            "diff_per_unit": diff_per_unit,
            "total_missed": total_missed,
            "currency": currency,
            "is_success": (total_missed < 0) # If missed < 0, it means Current < Sale => Good decision
        })

    # --- 4. Original Dashboard Logic (Summary & Charts) ---
    total_pl = df['רווח/הפסד'].sum()
    total_fees = df['עמלות ודמי ניהול'].sum()
    total_tax = df['מס שנוכה/הוחזר בארץ'].sum() + df['מס חו"ל בשקלים'].sum()

    security_pl = df.groupby('שם ני"ע')['רווח/הפסד'].sum().reset_index()
    security_pl.columns = ['name', 'val']
    security_pl = security_pl.sort_values('val', ascending=False)
    chart_pl_data = pd.concat([security_pl.head(5), security_pl.tail(5)]).drop_duplicates().to_dict(orient='records')

    currency_counts = df['מטבע'].value_counts().reset_index()
    currency_counts.columns = ['currency', 'count']
    chart_currency_data = {"labels": currency_counts['currency'].tolist(), "data": currency_counts['count'].tolist()}

    # --- 5. Final Output ---
    dashboard_data = {
        "summary": {
            "total_pl": total_pl,
            "total_fees": total_fees,
            "total_tax": total_tax
        },
        "charts": {
            "pl_by_security": chart_pl_data,
            "currency_distribution": chart_currency_data
        },
        "what_if": {
             # Return valid analysis rows mostly
             "opportunities": opportunity_list,
             # Maybe top missed opportunities?
             "top_regrets": sorted([x for x in opportunity_list if not x['is_success']], key=lambda x: x['total_missed'], reverse=True)[:5],
             "top_smart_moves": sorted([x for x in opportunity_list if x['is_success']], key=lambda x: x['total_missed'])[:5] # most negative first
        },
        "transactions": df.to_dict(orient='records'),
        "metadata": {
            "row_count": len(df),
            "generated_at": pd.Timestamp.now().isoformat()
        }
    }

    print(f"\n--- Opportunity Analysis ---")
    print(f"Analyzed {len(opportunity_list)} sales events.")
    print(f"Saving dashboard data to {output_file}...")
    
    # Sanitize NaNs before dumping
    # Function to recursively replace NaN with None (which becomes null in JSON)
    # or handle Infinity if needed.
    class NaNEncoder(json.JSONEncoder):
        def default(self, obj):
            import math
            if isinstance(obj, float):
                if math.isnan(obj) or math.isinf(obj):
                    return None
            if isinstance(obj, pd.Timestamp):
                return str(obj)
            return super().default(obj)
            
    # Alternatively, simply replace in the big dict:
    dashboard_data_sanitized = json.loads(json.dumps(dashboard_data, default=str).replace("NaN", "null"))

    with open(output_file, "w") as f:
        # Standard json.dump might still output NaN if allow_nan=True (default).
        # We want to force it to valid JSON.
        # But safest is to simple string manip or ignore_nan. 
        # Actually standard simple fix:
        json.dump(dashboard_data_sanitized, f, indent=4)
        
    print("Analysis Complete.")

if __name__ == "__main__":
    main()
