import pandas as pd
import numpy as np
import plotly.graph_objects as go
from scipy.stats import zscore
from pathlib import Path

# Paths
ROOT  = Path(__file__).resolve().parents[2]
FILE  = ROOT / "data" / "processed" / "cleaned_data.xlsx"

# Config
FRAMES_PER_YEAR = 10

# Load Data
xl          = pd.ExcelFile(FILE)
all_records = []

def get_val(val):
    if val == "- -" or pd.isna(val):
        return np.nan
    try:
        return float(val)
    except:
        return np.nan

for sheet_name in xl.sheet_names:
    df_raw = pd.read_excel(FILE, sheet_name=sheet_name)
    ticker = sheet_name.upper()

    cols = ["Sales/Revenue", "Gross Margin (%)", "Market Cap",
            "EV/Sales", "Price/FCF", "Return on Invested Capital (%)"]

    for col in cols:
        df_raw[col] = df_raw[col].apply(get_val)

    df_raw = df_raw[df_raw["Year"].notna()].copy()
    df_raw["Year"] = df_raw["Year"].astype(int)
    df_raw = df_raw[df_raw["Year"] != 2025]
    df_raw["ticker"] = ticker

    all_records.append(df_raw[["ticker", "Year"] + cols])

df = pd.concat(all_records, ignore_index=True)
df = df.rename(columns={
    "Year":                            "year",
    "Sales/Revenue":                   "revenue",
    "Gross Margin (%)":                "gross_margin",
    "Market Cap":                      "market_cap",
    "EV/Sales":                        "ev_sales",
    "Price/FCF":                       "price_fcf",
    "Return on Invested Capital (%)":  "roic",
})

# Derived Metrics
df = df.sort_values(["ticker", "year"]).reset_index(drop=True)

df["revenue_growth"]  = df.groupby("ticker")["revenue"].pct_change() * 100
df["fcf"]             = df["market_cap"] / df["price_fcf"]
df["fcf_margin"]      = df["fcf"] / df["revenue"] * 100
df["psg"]             = df["ev_sales"] / df["revenue_growth"]
df["ev_gross_profit"] = df["ev_sales"] / (df["gross_margin"] / 100)

for col in ["psg", "ev_gross_profit", "ev_sales", "fcf_margin"]:
    lo = df[col].quantile(0.01)
    hi = df[col].quantile(0.99)
    df[col] = df[col].clip(lo, hi)

# Composite Indices 
def safe_zscore(series):
    result = series.copy()
    mask   = series.notna()
    if mask.sum() > 1:
        result[mask] = zscore(series[mask])
    return result

for col in ["ev_sales", "ev_gross_profit", "psg", "fcf_margin", "roic"]:
    df[f"z_{col}"] = safe_zscore(df[col])

df["hype_index"] = (
    2.0 * df["z_ev_sales"].fillna(0) +
    1.0 * df["z_ev_gross_profit"].fillna(0) +
    1.0 * df["z_psg"].fillna(0)
)

df["fundamental_index"] = (
    df["z_fcf_margin"].fillna(0) +
    df["z_roic"].fillna(0)
)

print("Index ranges:")
print(f"  Hype:        {df['hype_index'].min():.2f} to {df['hype_index'].max():.2f}")
print(f"  Fundamental: {df['fundamental_index'].min():.2f} to {df['fundamental_index'].max():.2f}")
print("\nYear 2000 snapshot:")
print(df[df["year"] == 2000][["ticker", "hype_index", "fundamental_index"]]
      .dropna().to_string(index=False))

# Scaling Bubbles
def scale_bubble(mc):
    if pd.isna(mc) or mc <= 0:
        return 0
    normalized = (np.log10(mc) - 2) / (6.6 - 2)
    return max(np.clip(normalized, 0, 1) ** 2, 0.12)

def format_market_cap(mc):
    if pd.isna(mc) or mc <= 0:
        return "N/A"
    if mc >= 1_000_000:
        return f"${mc/1_000_000:.2f}T"
    elif mc >= 1_000:
        return f"${mc/1_000:.1f}B"
    return f"${mc:.0f}M"

df["bubble_size"]    = df["market_cap"].apply(scale_bubble)
df["market_cap_fmt"] = df["market_cap"].apply(format_market_cap)

# Grid
all_years   = sorted(df["year"].unique())
all_tickers = sorted(df["ticker"].unique())

grid = pd.MultiIndex.from_product(
    [all_tickers, all_years], names=["ticker", "year"]
)
df = df.set_index(["ticker", "year"]).reindex(grid).reset_index()
df = df.sort_values(["year", "ticker"]).reset_index(drop=True)

df_idx = df.set_index(["ticker", "year"])

# Interpolate
records = []

for i in range(len(all_years) - 1):
    y0 = all_years[i]
    y1 = all_years[i + 1]

    for step in range(FRAMES_PER_YEAR):
        t           = step / FRAMES_PER_YEAR
        frame_label = f"{y0}_{step:02d}"

        for ticker in all_tickers:
            r0 = df_idx.loc[(ticker, y0)]
            r1 = df_idx.loc[(ticker, y1)]

            has0 = pd.notna(r0["hype_index"]) and pd.notna(r0["fundamental_index"])
            has1 = pd.notna(r1["hype_index"]) and pd.notna(r1["fundamental_index"])

            if has0 and has1:
                hype = r0["hype_index"]        + t * (r1["hype_index"]        - r0["hype_index"])
                fund = r0["fundamental_index"] + t * (r1["fundamental_index"] - r0["fundamental_index"])
                mc   = (r0["market_cap"] + t * (r1["market_cap"] - r0["market_cap"])
                        if pd.notna(r0["market_cap"]) and pd.notna(r1["market_cap"])
                        else r0["market_cap"])
            elif has0:
                hype, fund, mc = r0["hype_index"], r0["fundamental_index"], r0["market_cap"]
            else:
                hype = fund = mc = np.nan

            records.append({
                "ticker":       ticker,
                "frame_label":  frame_label,
                "year_display": str(y0),
                "hype_index":   hype,
                "fund_index":   fund,
                "market_cap":   mc,
                "bubble_size":  scale_bubble(mc),
                "mc_fmt":       format_market_cap(mc),
            })

for ticker in all_tickers:
    r        = df_idx.loc[(ticker, all_years[-1])]
    has_data = pd.notna(r["hype_index"]) and pd.notna(r["fundamental_index"])
    mc       = r["market_cap"] if has_data else np.nan
    records.append({
        "ticker":       ticker,
        "frame_label":  f"{all_years[-1]}_00",
        "year_display": str(all_years[-1]),
        "hype_index":   r["hype_index"]        if has_data else np.nan,
        "fund_index":   r["fundamental_index"] if has_data else np.nan,
        "market_cap":   mc,
        "bubble_size":  scale_bubble(mc),
        "mc_fmt":       format_market_cap(mc),
    })

df_interp        = pd.DataFrame(records)
all_frame_labels = sorted(df_interp["frame_label"].unique())

print(f"\nTotal frames: {len(all_frame_labels)}")
print(f"Companies: {all_tickers}")

# Axis Ranges
x_min = df_interp["hype_index"].min()  - 3.0
x_max = df_interp["hype_index"].max()  + 3.0
y_min = df_interp["fund_index"].min()  - 3.0
y_max = df_interp["fund_index"].max()  + 3.0
mid_x = 0
mid_y = 0

# Colors
colors = [
    "#E74C3C", "#3498DB", "#2ECC71", "#F39C12", "#9B59B6",
    "#1ABC9C", "#E67E22", "#34495E", "#E91E63", "#00BCD4", "#8BC34A"
]
color_map = {t: colors[i % len(colors)] for i, t in enumerate(all_tickers)}

# Figure
first_frame = df_interp[df_interp["frame_label"] == all_frame_labels[0]]

traces = []
for ticker in all_tickers:
    row    = first_frame[first_frame["ticker"] == ticker]
    x_val  = row["hype_index"].values[0] if len(row) > 0 else np.nan
    y_val  = row["fund_index"].values[0]  if len(row) > 0 else np.nan
    b_val  = row["bubble_size"].values[0] if len(row) > 0 else 0.12
    mc_fmt = row["mc_fmt"].values[0]      if len(row) > 0 else "N/A"

    traces.append(go.Scatter(
        x=[x_val] if pd.notna(x_val) else [],
        y=[y_val] if pd.notna(y_val) else [],
        mode="markers",
        marker=dict(
            size=[b_val * 60],
            color=color_map[ticker],
            opacity=0.9,
            line=dict(width=1, color="white"),
        ),
        name=ticker,
        legendgroup=ticker,
        showlegend=True,
        text=[ticker],
        customdata=[mc_fmt],
        hovertemplate=(
            "<b>%{text}</b><br>"
            "Hype Index: %{x:.2f}<br>"
            "Fundamental Index: %{y:.2f}<br>"
            "Market Cap: %{customdata}<br>"
            "<extra></extra>"
        ),
    ))

df_interp_grouped = {
    label: grp.set_index("ticker")
    for label, grp in df_interp.groupby("frame_label")
}

frames = []
for frame_label in all_frame_labels:
    grp          = df_interp_grouped[frame_label]
    year_display = grp["year_display"].iloc[0]
    frame_traces = []

    for ticker in all_tickers:
        if ticker in grp.index:
            row    = grp.loc[ticker]
            x_val  = row["hype_index"]
            y_val  = row["fund_index"]
            b_val  = row["bubble_size"]
            mc_fmt = row["mc_fmt"]
        else:
            x_val = y_val = np.nan
            b_val  = 0
            mc_fmt = "N/A"

        frame_traces.append(go.Scatter(
            x=[x_val] if pd.notna(x_val) else [],
            y=[y_val] if pd.notna(y_val) else [],
            mode="markers",
            marker=dict(
                size=[b_val * 60],
                color=color_map[ticker],
                opacity=0.9,
                line=dict(width=1, color="white"),
            ),
            text=[ticker],
            customdata=[mc_fmt],
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Hype Index: %{x:.2f}<br>"
                "Fundamental Index: %{y:.2f}<br>"
                "Market Cap: %{customdata}<br>"
                "<extra></extra>"
            ),
        ))

    frames.append(go.Frame(
        data=frame_traces,
        name=frame_label,
        layout=go.Layout(title_text=f"Hype vs Fundamentals — {year_display}")
    ))

slider_steps = []
for year in all_years:
    matching = [f for f in all_frame_labels if f.startswith(str(year))]
    if matching:
        slider_steps.append(dict(
            args=[[matching[0]], {"frame": {"duration": 0, "redraw": True},
                                  "mode": "immediate",
                                  "transition": {"duration": 0}}],
            label=str(year),
            method="animate",
        ))

# Layout
fig = go.Figure(
    data=traces,
    frames=frames,
    layout=go.Layout(
        title=dict(
            text="Tech Company Hype vs Fundamentals (Dot-Com Era to AI Era)",
            y=0.97,
        ),
        xaxis=dict(
            title="Hype Index (higher = more speculative)",
            range=[x_min, x_max],
            fixedrange=True,
            domain=[0, 0.82],
        ),
        yaxis=dict(
            title="Fundamental Index (higher = stronger business)",
            range=[y_min, y_max],
            fixedrange=True,
        ),
        legend=dict(
            x=0.84,
            y=0.99,
            xanchor="left",
            yanchor="top",
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="rgba(0,0,0,0.1)",
            borderwidth=1,
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(size=13),
        margin=dict(t=60, b=140, l=60, r=180),
        updatemenus=[{
            "buttons": [
                {
                    "args": [None, {"frame": {"duration": 80, "redraw": True},
                                    "fromcurrent": True,
                                    "transition": {"duration": 60,
                                                   "easing": "linear"}}],
                    "label": "Play",
                    "method": "animate",
                },
                {
                    "args": [[None], {"frame": {"duration": 0, "redraw": True},
                                      "mode": "immediate",
                                      "transition": {"duration": 0}}],
                    "label": "Pause",
                    "method": "animate",
                },
            ],
            "type": "buttons",
            "showactive": False,
            "x": 0.0,
            "y": -0.22,
            "xanchor": "left",
            "yanchor": "top",
        }],
        sliders=[{
            "steps": slider_steps,
            "x": 0.1,
            "y": -0.18,
            "len": 0.75,
            "xanchor": "left",
            "yanchor": "top",
            "currentvalue": {"visible": False},
            "transition": {"duration": 0},
            "pad": {"t": 10, "b": 10},
        }],
        uirevision="constant",
    )
)

# Quadrant Shading
fig.add_shape(type="rect", x0=mid_x, x1=x_max, y0=y_min, y1=mid_y,
    fillcolor="rgba(231, 76, 60, 0.08)", line=dict(width=0), layer="below")
fig.add_shape(type="rect", x0=x_min, x1=mid_x, y0=mid_y, y1=y_max,
    fillcolor="rgba(46, 204, 113, 0.08)", line=dict(width=0), layer="below")
fig.add_shape(type="rect", x0=mid_x, x1=x_max, y0=mid_y, y1=y_max,
    fillcolor="rgba(243, 156, 18, 0.08)", line=dict(width=0), layer="below")
fig.add_shape(type="rect", x0=x_min, x1=mid_x, y0=y_min, y1=mid_y,
    fillcolor="rgba(149, 165, 166, 0.08)", line=dict(width=0), layer="below")

fig.add_annotation(x=(mid_x+x_max)/2, y=(y_min+mid_y)/2,
    text="Speculative Bubble Zone", showarrow=False,
    font=dict(size=11, color="rgba(192, 57, 43, 0.5)"))
fig.add_annotation(x=(x_min+mid_x)/2, y=(mid_y+y_max)/2,
    text="Healthy Zone", showarrow=False,
    font=dict(size=11, color="rgba(39, 174, 96, 0.5)"))
fig.add_annotation(x=(mid_x+x_max)/2, y=(mid_y+y_max)/2,
    text="Expensive but Justified", showarrow=False,
    font=dict(size=11, color="rgba(211, 84, 0, 0.5)"))
fig.add_annotation(x=(x_min+mid_x)/2, y=(y_min+mid_y)/2,
    text="Cheap but Weak", showarrow=False,
    font=dict(size=11, color="rgba(100, 100, 100, 0.5)"))

fig.add_hline(y=mid_y, line_dash="dash", line_color="gray", opacity=0.3)
fig.add_vline(x=mid_x, line_dash="dash", line_color="gray", opacity=0.3)

fig.show()