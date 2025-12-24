import pandas as pd
import os

# Paths
input_file = "/Users/agamhen/Desktop/אבא/השקעות אבא פרטי.xlsx"
output_file = "/Users/agamhen/Desktop/אבא/data.csv"

# Columns to extract
# Based on previous analysis:
# ['אסמכתא', "מס' בורסה", 'תאריך ביצוע', 'שם ני"ע', 'פעולה', 'כמות ביצוע', 'שער ביצוע', 'מטבע', 'עמלות ודמי ניהול', 'תמורה נטו לפני מס', 'רווח/הפסד', 'שעור המס', 'מס שנוכה/הוחזר בארץ', 'מס חו"ל בשקלים']

desired_columns = [
    'תאריך ביצוע', 
    'שם ני"ע', 
    'פעולה', 
    'כמות ביצוע', 
    'שער ביצוע', 
    'מטבע', 
    'עמלות ודמי ניהול', 
    'תמורה נטו לפני מס', 
    'רווח/הפסד', 
    'מס שנוכה/הוחזר בארץ', 
    'מס חו"ל בשקלים'
]

print(f"Reading from: {input_file}")

try:
    # Read Excel, skipping first 4 rows (header is at index 4 / row 5)
    df = pd.read_excel(input_file, header=4)
    
    # Filter columns
    df_filtered = df[desired_columns].copy()
    
    # Fill NaN with 0 for numerical columns to avoid issues in JS
    numeric_cols = ['כמות ביצוע', 'שער ביצוע', 'עמלות ודמי ניהול', 'תמורה נטו לפני מס', 'רווח/הפסד', 'מס שנוכה/הוחזר בארץ', 'מס חו"ל בשקלים']
    for col in numeric_cols:
        df_filtered[col] = df_filtered[col].fillna(0)
        
    # Standardize Currency if needed (assuming consistency but good to be safe)
    df_filtered['מטבע'] = df_filtered['מטבע'].astype(str).str.strip()

    # Save to CSV
    df_filtered.to_csv(output_file, index=False, encoding='utf-8-sig') # utf-8-sig for Hebrew support in Excel/Editors
    
    print(f"Successfully converted to: {output_file}")
    print(f"Rows processed: {len(df_filtered)}")
    
except Exception as e:
    print(f"Error converting data: {e}")
