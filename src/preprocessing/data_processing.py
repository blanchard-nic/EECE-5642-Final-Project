import pandas as pd
import numpy as np

# ── CONFIG ─────────────────────────────────────────────────────────────────────
INPUT_FILE  = "raw_data.xlsx"
OUTPUT_FILE = "cleaned_data.xlsx"

# Row indices (0-based, so Excel row 4 = index 3)
ROW_YEAR        = 3
ROW_REVENUE     = 4
ROW_ROIC        = 201
ROW_GROSS_MAR   = 202
ROW_EBITDA_MAR  = 203
ROW_MARKET_CAP  = 282
ROW_EV_SALES    = 291
ROW_PRICE_FCF   = 309

# ── ANOMALY RULES ──────────────────────────────────────────────────────────────
DROP_BEFORE = {
    "AKAM": 1999,
    "AAPL": 1981,
}

VALID_BOUNDS = {
    "Gross Margin (%)":               (-100, 100),
    "EBITDA Margin (%)":              (-500, 100),
    "EV/Sales":                       (0, 500),
    "Return on Invested Capital (%)": (-100, 100),
}

# ── HELPER ─────────────────────────────────────────────────────────────────────
def clean_value(val):
    if val is None or val == "" or val == "- -" or val == "N/A":
        return np.nan
    if isinstance(val, str):
        val = val.strip().replace(",", "").replace("%", "")
        try:
            return float(val)
        except:
            return np.nan
    try:
        return float(val)
    except:
        return np.nan

def clean_year(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).replace(" Y", "").replace("Y", "").strip()
    try:
        return int(float(s))
    except:
        return None

# ── PROCESS ────────────────────────────────────────────────────────────────────
xl     = pd.ExcelFile(INPUT_FILE)
writer = pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl")

for sheet_name in xl.sheet_names:
    print(f"Processing: {sheet_name}")
    ticker = sheet_name.upper()

    df_raw = pd.read_excel(INPUT_FILE, sheet_name=sheet_name, header=None)

    year_row      = df_raw.iloc[ROW_YEAR]
    revenue_row   = df_raw.iloc[ROW_REVENUE]
    roic_row      = df_raw.iloc[ROW_ROIC]
    gross_row     = df_raw.iloc[ROW_GROSS_MAR]
    ebitda_row    = df_raw.iloc[ROW_EBITDA_MAR]
    mktcap_row    = df_raw.iloc[ROW_MARKET_CAP]
    ev_sales_row  = df_raw.iloc[ROW_EV_SALES]
    pfcf_row      = df_raw.iloc[ROW_PRICE_FCF]

    # Parse years
    years = []
    for val in year_row.iloc[1:]:
        y = clean_year(val)
        if y is not None:
            years.append(y)

    # Build records
    records = []
    for col_offset, year in enumerate(years):
        col_idx = col_offset + 1

        records.append({
            "Year":                          year,
            "Sales/Revenue":                 clean_value(revenue_row.iloc[col_idx]),
            "Return on Invested Capital (%)":clean_value(roic_row.iloc[col_idx]),
            "Gross Margin (%)":              clean_value(gross_row.iloc[col_idx]),
            "EBITDA Margin (%)":             clean_value(ebitda_row.iloc[col_idx]),
            "Market Cap":                    clean_value(mktcap_row.iloc[col_idx]),
            "EV/Sales":                      clean_value(ev_sales_row.iloc[col_idx]),
            "Price/FCF":                     clean_value(pfcf_row.iloc[col_idx]),
        })

    df_clean = pd.DataFrame(records)

    # ── ANOMALY FIX 1: Drop years before known good start year ────────────────
    if ticker in DROP_BEFORE:
        cutoff = DROP_BEFORE[ticker]
        before = len(df_clean)
        df_clean = df_clean[df_clean["Year"] >= cutoff]
        dropped = before - len(df_clean)
        if dropped > 0:
            print(f"  Dropped {dropped} rows before {cutoff} for {ticker}")

    # ── ANOMALY FIX 2: Set out-of-bounds values to NaN ────────────────────────
    for col, (lo, hi) in VALID_BOUNDS.items():
        if col in df_clean.columns:
            mask      = df_clean[col].notna() & ((df_clean[col] < lo) | (df_clean[col] > hi))
            n_flagged = mask.sum()
            if n_flagged > 0:
                print(f"  {ticker}: {n_flagged} out-of-bounds values in {col} set to NaN")
                df_clean.loc[mask, col] = np.nan

    # ── ANOMALY FIX 3: Drop rows where all metrics are NaN ────────────────────
    metric_cols = ["Sales/Revenue", "Gross Margin (%)", "EBITDA Margin (%)",
                   "Market Cap", "EV/Sales", "Price/FCF",
                   "Return on Invested Capital (%)"]
    df_clean = df_clean.dropna(subset=metric_cols, how="all")

    # Sort by year
    df_clean = df_clean.sort_values("Year").reset_index(drop=True)

    df_clean.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"  {ticker}: {len(df_clean)} years "
          f"({int(df_clean['Year'].min())} – {int(df_clean['Year'].max())})")

writer.close()
print(f"\nDone. Saved to {OUTPUT_FILE}")