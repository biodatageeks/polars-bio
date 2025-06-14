import polars_bio as pb
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path

# Ścieżki
fastq_path = "min_example.fastq"
html_output = "phred_score_report.html"

# Sprawdzenie pliku
if not Path(fastq_path).exists():
    print(f"Błąd: plik '{fastq_path}' nie istnieje.")
    exit(1)

# Wczytaj dane jako DataFrame z Polars, potem Pandas
result = pb.cacl_base_seq_quality(fastq_path, output_type="polars.DataFrame")
df = result.to_pandas()

# Wykres
fig = go.Figure()

# Dodaj linię trendu – mediana Phred score
fig.add_trace(go.Scatter(
    x=df["median"],
    y=df["position"],
    mode='lines+markers',
    name='Median trend',
    line=dict(color='red', width=2)
))

# Dodaj boxploty jako error bar (alternatywa)
fig.add_trace(go.Box(
    x=df["min"],
    y=df["position"],
    name="Min",
    orientation="h",
    marker_color="rgba(0,100,80,0.3)",
    boxpoints=False,
    showlegend=False
))

fig.add_trace(go.Box(
    x=df["max"],
    y=df["position"],
    name="Max",
    orientation="h",
    marker_color="rgba(0,100,80,0.3)",
    boxpoints=False,
    showlegend=False
))

# Odwróć oś Y (bo pozycja = od góry)
fig.update_yaxes(autorange="reversed")

fig.update_layout(
    title="Phred Score vs Position in Read",
    xaxis_title="Phred Score",
    yaxis_title="Position in Read",
    template="plotly_white"
)

# Zapisz do HTML
fig.write_html(html_output)
print(f"✅ Zapisano raport do: {html_output}")
