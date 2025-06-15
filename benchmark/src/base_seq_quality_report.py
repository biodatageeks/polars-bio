import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars_bio as pb


def main():
    script_dir = Path(__file__).parent
    fastq = script_dir / "example.fastq"
    out_html = script_dir / "report_full.html"

    if not fastq.exists():
        print(f"Nie znaleziono pliku: {fastq}", file=sys.stderr)
        sys.exit(1)
    print(f"Parsuję FASTQ: {fastq}")

    df = pb.cacl_base_seq_quality(str(fastq), output_type='pandas.DataFrame')
    print(df.columns)
    print(df[['min', 'q1', 'median', 'q3', 'max']].apply(len)) 
    print(f"Policzono statystyki: {df.shape[0]} pozycji")

    traces = []

    for _, r in df.iterrows():
        traces.append(go.Scatter(
            x=[r["min"], r["max"]],
            y=[r["position"], r["position"]],
            mode="lines",
            line=dict(color="gray", width=2),
            showlegend=False,
            hoverinfo="skip"
        ))

    customdata = df[['min', 'q1', 'median', 'q3', 'max']].to_numpy()

    traces.append(go.Bar(
        x=df["q3"] - df["q1"],
        y=df["position"],
        base=df["q1"],
        orientation="h",
        marker_color="rgba(0,100,80,0.6)",
        marker_line=dict(color="rgba(0,100,80,1)", width=2),
        width=0.6,
        showlegend=False,
        customdata=customdata,
        hovertemplate=(
            "Position: %{y}<br>"
            "Min: %{customdata[0]}<br>"
            "Q1: %{customdata[1]}<br>"
            "Median: %{customdata[2]}<br>"
            "Q3: %{customdata[3]}<br>"
            "Max: %{customdata[4]}<br>"
        )
    ))

    traces.append(go.Scatter(
        x=df["median"],
        y=df["position"],
        mode="markers",
        marker=dict(
            color="white",
            symbol="line-ns-open",
            size=16,
            line=dict(width=2)
        ),
        showlegend=False,
        hoverinfo="skip"
    ))

    traces.append(go.Scatter(
        x=df["median"],
        y=df["position"],
        mode="lines+markers",
        line=dict(color="red", width=2),
        marker=dict(size=4),
        name="Median",
        hoverinfo="skip"
    ))

    height = max(600, df.shape[0] * 60 + 200)

    fig = go.Figure(traces)
    fig.update_yaxes(
        autorange="reversed",
        title="Position in read (bp)",
        dtick=1,
        range=[0, 100]
    )
    fig.update_xaxes(
        title="Phred score",
        dtick=2
    )
    fig.update_layout(
        title="Phred Score per Base Position",
        template="plotly_white",
        height=height,
        margin=dict(l=80, r=20, t=60, b=60)
    )

    plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

    table_html = df.to_html(classes="table table-striped", index=False)

    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>FastQC-like report</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.2/dist/css/bootstrap.min.css">
  <style>
    body {{ margin: 20px; }}
    h1 {{ margin-bottom: 30px; }}
    #plot {{ margin-bottom: 50px; }}
  </style>
</head>
<body>
  <h1>Phred Score per Base Position</h1>
  <div id="plot">
    {plot_html}
  </div>
  <h2>Statistics Table</h2>
  <div id="table">
    {table_html}
  </div>
</body>
</html>
"""
    out_html.write_text(html, encoding='utf-8')
    print(f" Wygenerowano pełny raport: {out_html}")

if __name__ == "__main__":
    main()
