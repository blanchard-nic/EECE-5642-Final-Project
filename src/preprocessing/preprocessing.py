import pandas as pd
import re

input_file = "data/raw/data_compilation.xlsx"
output_file = "data/processed/all_companies_cleaned.xlsx"

target_rows = [
    "Operating Margin (%)",
    "Market Capitalization",
    "EV/Sales"
]

def clean_value(x):
    if pd.isna(x):
        return None

    s = str(x).strip()

    if s in ["--", "- -", "-", ""]:
        return None

    s = s.replace(",", "")

    try:
        return float(s)
    except ValueError:
        return None

def extract_year(x):
    if pd.isna(x):
        return None
    match = re.search(r"\d{4}", str(x))
    return match.group(0) if match else None

def get_row(df, row_name):
    first_col = df.iloc[:, 0].astype(str).str.strip()
    match = df[first_col == row_name]

    if match.empty:
        raise ValueError(f"Could not find row: {row_name}")

    row = match.iloc[0, 1:].tolist()
    return [clean_value(v) for v in row]

def clean_company_sheet(df, sheet_name):
    # Years are always in Excel row 4, starting from column B
    raw_years = df.iloc[3, 1:].tolist()
    years = [extract_year(y) for y in raw_years]

    operating = get_row(df, "Operating Margin (%)")
    market_cap = get_row(df, "Market Capitalization")
    ev_sales = get_row(df, "EV/Sales")

    valid_idx = []
    for i in range(len(years)):
        if (
            years[i] is not None and
            operating[i] is not None and
            market_cap[i] is not None and
            ev_sales[i] is not None
        ):
            valid_idx.append(i)

    clean_years = [years[i] for i in valid_idx]
    clean_operating = [operating[i] for i in valid_idx]
    clean_market_cap = [market_cap[i] for i in valid_idx]
    clean_ev_sales = [ev_sales[i] for i in valid_idx]

    output_rows = [
        [sheet_name] + clean_years,
        ["Operating Margin (%)"] + clean_operating,
        ["Market Capitalization"] + clean_market_cap,
        ["EV/Sales"] + clean_ev_sales,
    ]

    return pd.DataFrame(output_rows)

# Load workbook
excel_file = pd.ExcelFile(input_file)

# Write all cleaned sheets into one new workbook
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    for sheet_name in excel_file.sheet_names:
        try:
            df = pd.read_excel(input_file, sheet_name=sheet_name, header=None)
            cleaned_df = clean_company_sheet(df, sheet_name)
            cleaned_df.to_excel(writer, sheet_name=sheet_name, header=False, index=False)
            print(f"Processed: {sheet_name}")
        except Exception as e:
            print(f"Skipped {sheet_name}: {e}")

print(f"\nSaved cleaned workbook to: {output_file}")