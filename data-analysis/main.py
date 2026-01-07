import pandas as pd
import json
import os

def clean_money(val):
    if isinstance(val, str):
        return float(val.replace(',', ''))
    return float(val)

def main():
    print("Starting Chenfuel Portfolio Analysis...")
    
    # Paths
    csv_path = "data.csv"
    output_dir = "/app/web"
    output_file = os.path.join(output_dir, "dashboard_data.json")

    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found.")
        return

    # Read CSV
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Clean numeric columns
    numeric_cols = ['רווח/הפסד', 'עמלות ודמי ניהול', 'מס שנוכה/הוחזר בארץ', 'מס חו"ל בשקלים', 'כמות ביצוע', 'שער ביצוע', 'תמורה נטו לפני מס']
    for col in numeric_cols:
         if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0)
    
    # Ensure string columns
    df['שם ני"ע'] = df['שם ני"ע'].fillna('Unknown')
    df['מטבע'] = df['מטבע'].fillna('Unknown')

    # --- Calculations ---

    # 1. Summary Metrics
    total_pl = df['רווח/הפסד'].sum()
    total_fees = df['עמלות ודמי ניהול'].sum()
    total_tax = df['מס שנוכה/הוחזר בארץ'].sum() + df['מס חו"ל בשקלים'].sum()

    # 2. Charts Data
    
    # Bar Chart: P/L by Security (Top 5 Winners + Top 5 Losers)
    security_pl = df.groupby('שם ני"ע')['רווח/הפסד'].sum().reset_index()
    security_pl.columns = ['name', 'val']
    security_pl = security_pl.sort_values('val', ascending=False)
    
    top_5 = security_pl.head(5)
    bottom_5 = security_pl.tail(5)
    # Combine and deduplicate just in case
    chart_pl_df = pd.concat([top_5, bottom_5]).drop_duplicates()
    
    chart_pl_data = chart_pl_df.to_dict(orient='records')

    # Pie Chart: Transactions by Currency
    currency_counts = df['מטבע'].value_counts().reset_index()
    currency_counts.columns = ['currency', 'count']
    chart_currency_data = {
        "labels": currency_counts['currency'].tolist(),
        "data": currency_counts['count'].tolist()
    }

    # 3. Table Data (Full List)
    # Convert entire DF to dict, but keep float formatting clean if needed or just dump raw
    table_data = df.to_dict(orient='records')

    # Construct Final JSON
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
        "transactions": table_data,
        "metadata": {
            "row_count": len(df),
            "generated_at": pd.Timestamp.now().isoformat()
        }
    }

    print("--- Analysis Summary ---")
    print(f"Total P/L: {total_pl:,.2f}")
    print(f"Generated {len(chart_pl_data)} items for Bar Chart")
    print(f"Generated {len(table_data)} items for Table")
    print("------------------------")

    print(f"Saving dashboard data to {output_file}...")
    with open(output_file, "w") as f:
        json.dump(dashboard_data, f, indent=4, default=str) # default=str handles Timestamps
        
    print("Analysis Complete.")

if __name__ == "__main__":
    main()
