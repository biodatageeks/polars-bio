#!/usr/bin/env python3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

def main():
    # 1) Ścieżka do FASTQ i wyjście HTML
    script_dir = Path(__file__).parent
    fastq = script_dir / "example.fastq"
    out_html = script_dir / "report_full.html"

    if not fastq.exists():
        print(f"❌ Nie znaleziono pliku: {fastq}", file=sys.stderr)
        sys.exit(1)
    print(f"✅ Parsuję FASTQ: {fastq}")

    # 2) Parsowanie i zbieranie Phredów
    scores = {}
    with fastq.open() as f:
        for idx, line in enumerate(f, start=1):
            if idx % 4 == 0:
                for pos, ch in enumerate(line.strip()):
                    scores.setdefault(pos, []).append(ord(ch) - 33)

    # 3) Budowa DataFrame
    rows = []
    for pos in sorted(scores):
        arr = np.array(scores[pos])
        rows.append({
            "position":      pos,
            "min_score":     float(arr.min()),
            "q1_score":      float(np.percentile(arr, 25)),
            "median_score":  float(np.median(arr)),
            "q3_score":      float(np.percentile(arr, 75)),
            "max_score":     float(arr.max()),
            "average_score": float(arr.mean()),
        })
    df = pd.DataFrame(rows)
    print(f"📊 Policzono statystyki: {df.shape[0]} pozycji")

    # 4) Generuj wykres Plotly
    traces = []

    # whiskers (min→max) – pomijamy hover
    for _, r in df.iterrows():
        traces.append(go.Scatter(
            x=[r.min_score, r.max_score],
            y=[r.position, r.position],
            mode="lines",
            line=dict(color="gray", width=2),
            showlegend=False,
            hoverinfo="skip"
        ))

    # BOX: Q1→Q3 z customdata i hovertemplate
    customdata = np.stack([
        df.min_score,
        df.q1_score,
        df.median_score,
        df.q3_score,
        df.max_score,
        df.average_score
    ], axis=1)

    traces.append(go.Bar(
        x=df.q3_score - df.q1_score,
        y=df.position,
        base=df.q1_score,
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
            "Average: %{customdata[5]:.2f}<extra></extra>"
        )
    ))

    # median tick – bez hover
    traces.append(go.Scatter(
        x=df.median_score,
        y=df.position,
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

    # trend średniej – bez hover
    traces.append(go.Scatter(
        x=df.average_score,
        y=df.position,
        mode="lines+markers",
        line=dict(color="red", width=2),
        marker=dict(size=4),
        name="Average",
        hoverinfo="skip"
    ))

    # 5) Layout z powiększonym spacingiem i dtick, ograniczony zakres Y
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

    # Wyciągnij div+script wykresu
    plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

    # 6) Generuj statyczną tabelę HTML
    table_html = df.to_html(classes="table table-striped", index=False)

    # 7) Szablon jednego pliku HTML
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

    # 8) Zapis
    out_html.write_text(html, encoding='utf-8')
    print(f"✅ Wygenerowano pełny raport: {out_html}")

if __name__ == "__main__":
    main()
