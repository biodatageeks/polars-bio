from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D

matplotlib.use("Agg")


RESULTS_ROOT = Path(
    "/Users/mwiewior/research/git/polars-bio-bench/results/2026-04-24 08:59:07"
)
REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "docs/blog/posts/figures/dataframe-paths-benchmark-2026-04"

OPS = [
    "overlap",
    "nearest",
    "count_overlaps",
    "coverage",
    "cluster",
    "complement",
    "merge",
    "subtract",
]
THREADS = [1, 2, 4, 8]

VARIANT_ORDER = [
    "polars_bio",
    "polars_bio[polars.LazyFrame->polars.LazyFrame]",
    "polars_bio[polars.DataFrame->polars.DataFrame]",
    "polars_bio[pandas.pyarrow.DataFrame->pandas.DataFrame]",
    "polars_bio[pandas.DataFrame->pandas.DataFrame]",
]
VARIANT_LABELS = {
    "polars_bio": "Direct Parquet (DataFusion)",
    "polars_bio[polars.LazyFrame->polars.LazyFrame]": "Polars lazy",
    "polars_bio[polars.DataFrame->polars.DataFrame]": "Polars eager",
    "polars_bio[pandas.pyarrow.DataFrame->pandas.DataFrame]": "Pandas Arrow dtypes",
    "polars_bio[pandas.DataFrame->pandas.DataFrame]": "Pandas",
}
VARIANT_LABELS_MULTILINE = {
    "polars_bio": "Direct Parquet\n(DataFusion)",
    "polars_bio[polars.LazyFrame->polars.LazyFrame]": "Polars\nlazy",
    "polars_bio[polars.DataFrame->polars.DataFrame]": "Polars\neager",
    "polars_bio[pandas.pyarrow.DataFrame->pandas.DataFrame]": "Pandas\nArrow dtypes",
    "polars_bio[pandas.DataFrame->pandas.DataFrame]": "Pandas",
}
VARIANT_COLORS = {
    "polars_bio": "#1f77b4",
    "polars_bio[polars.LazyFrame->polars.LazyFrame]": "#9467bd",
    "polars_bio[polars.DataFrame->polars.DataFrame]": "#ff7f0e",
    "polars_bio[pandas.pyarrow.DataFrame->pandas.DataFrame]": "#2ca02c",
    "polars_bio[pandas.DataFrame->pandas.DataFrame]": "#7f7f7f",
}
OPERATION_TITLES = {
    "overlap": "Overlap",
    "nearest": "Nearest",
    "count_overlaps": "Count overlaps",
    "coverage": "Coverage",
    "cluster": "Cluster",
    "complement": "Complement",
    "merge": "Merge",
    "subtract": "Subtract",
}
SHORT_OP_LABELS = {
    "overlap": "Overlap",
    "nearest": "Nearest",
    "count_overlaps": "Count\noverlaps",
    "coverage": "Coverage",
    "cluster": "Cluster",
    "complement": "Complement",
    "merge": "Merge",
    "subtract": "Subtract",
}
DATASET_TITLES = {
    "2-1": "1-thread performance by operation — dataset 2-1",
    "8-7": "1-thread performance by operation — dataset 8-7",
}
DATASET_NOTES = {
    "2-1": "exons (439K intervals) vs fBrain (199K intervals); terminal action = count",
    "8-7": "ex-rna (9.9M intervals) vs ex-anno (1.2M intervals); terminal action = count",
}
TITLE_FONTSIZE = 20
SUBTITLE_FONTSIZE = 11.5


def load_results() -> pd.DataFrame:
    rows = []
    for dataset in ["2-1", "8-7"]:
        for op in OPS:
            path = RESULTS_ROOT / f"{op}-parallel-polars-bio-all-inputs_{dataset}.csv"
            df = pd.read_csv(path)
            df["threads"] = df["name"].str.extract(r"-(2|4|8)$").fillna("1").astype(int)
            df["variant"] = df["name"].str.replace(r"-(2|4|8)$", "", regex=True)
            df["operation"] = op
            df["dataset"] = dataset
            rows.append(df)
    return pd.concat(rows, ignore_index=True)


def apply_bar_layout(fig: plt.Figure, title: str, subtitle: str) -> None:
    fig.suptitle(title, fontsize=TITLE_FONTSIZE, y=0.992)
    fig.text(
        0.5,
        0.905,
        subtitle,
        ha="center",
        va="center",
        fontsize=SUBTITLE_FONTSIZE,
        color="#555555",
        wrap=True,
    )
    fig.subplots_adjust(top=0.80, bottom=0.18, left=0.07, right=0.99)


def apply_line_layout(fig: plt.Figure, title: str, subtitle: str) -> None:
    fig.suptitle(title, fontsize=TITLE_FONTSIZE, y=0.992)
    fig.text(
        0.5,
        0.905,
        subtitle,
        ha="center",
        va="center",
        fontsize=SUBTITLE_FONTSIZE,
        color="#555555",
        wrap=True,
    )
    fig.subplots_adjust(top=0.80, bottom=0.12, left=0.08, right=0.99)


def generate_one_thread_figures(all_df: pd.DataFrame) -> None:
    plt.style.use("seaborn-v0_8-whitegrid")
    for dataset in ["2-1", "8-7"]:
        one = all_df[(all_df["dataset"] == dataset) & (all_df["threads"] == 1)].copy()
        pivot = one.pivot(index="operation", columns="variant", values="mean").loc[
            OPS, VARIANT_ORDER
        ]

        fig, ax = plt.subplots(figsize=(15, 8.4))
        width = 0.16
        x = range(len(OPS))
        center = (len(VARIANT_ORDER) - 1) / 2

        for i, variant in enumerate(VARIANT_ORDER):
            offsets = [xi + (i - center) * width for xi in x]
            ax.bar(
                offsets,
                pivot[variant].values,
                width=width,
                label=VARIANT_LABELS_MULTILINE[variant],
                color=VARIANT_COLORS[variant],
                edgecolor="white",
                linewidth=0.8,
            )

        ax.set_ylabel("Wall time (s)", fontsize=16)
        ax.set_xticks(list(x))
        ax.set_xticklabels([SHORT_OP_LABELS[op] for op in OPS], fontsize=13)
        ax.tick_params(axis="y", labelsize=13)
        ax.grid(axis="y", alpha=0.25)
        ax.legend(
            ncol=5,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.12),
            frameon=False,
            fontsize=12,
        )

        apply_bar_layout(fig, DATASET_TITLES[dataset], DATASET_NOTES[dataset])
        fig.savefig(
            OUTPUT_DIR / f"one_thread_all_operations_{dataset.replace('-', '_')}.png",
            dpi=220,
            bbox_inches="tight",
        )
        plt.close(fig)


def generate_thread_scalability_figures(all_df: pd.DataFrame) -> None:
    plt.style.use("seaborn-v0_8-whitegrid")
    native = all_df[all_df["dataset"] == "8-7"].copy()

    for op in OPS:
        sub = native[native["operation"] == op].copy()
        fig, ax = plt.subplots(figsize=(11.8, 8.2))

        for variant in VARIANT_ORDER:
            var_df = sub[sub["variant"] == variant].sort_values("threads")
            y = var_df["mean"].tolist()
            ax.plot(
                THREADS,
                y,
                marker="o",
                linewidth=2.8,
                markersize=8,
                color=VARIANT_COLORS[variant],
                label=VARIANT_LABELS[variant],
            )
            ideal = [y[0] / t for t in THREADS]
            ax.plot(
                THREADS,
                ideal,
                linestyle=":",
                linewidth=2.0,
                color=VARIANT_COLORS[variant],
                alpha=0.45,
            )

        ax.set_xlabel("Thread count", fontsize=15)
        ax.set_ylabel("Wall time (s)", fontsize=15)
        ax.set_xticks(THREADS)
        ax.tick_params(axis="both", labelsize=12)
        ax.grid(axis="both", alpha=0.25)
        ax.set_ylim(bottom=0)

        handles = [
            Line2D(
                [0],
                [0],
                color=VARIANT_COLORS[v],
                marker="o",
                linewidth=2.8,
                markersize=8,
                label=VARIANT_LABELS[v],
            )
            for v in VARIANT_ORDER
        ]
        handles.append(
            Line2D(
                [0],
                [0],
                color="#666666",
                linestyle=":",
                linewidth=2.0,
                label="Ideal scaling",
            )
        )
        ax.legend(
            handles=handles, loc="upper right", ncol=2, frameon=True, fontsize=11.5
        )

        apply_line_layout(
            fig,
            f"{OPERATION_TITLES[op]} thread scalability — dataset 8-7",
            "Dotted lines show ideal scaling from each path's 1-thread baseline",
        )
        fig.savefig(
            OUTPUT_DIR / f"thread_scalability_{op}.png",
            dpi=220,
            bbox_inches="tight",
        )
        plt.close(fig)


def main() -> None:
    all_df = load_results()
    generate_one_thread_figures(all_df)
    generate_thread_scalability_figures(all_df)


if __name__ == "__main__":
    main()
