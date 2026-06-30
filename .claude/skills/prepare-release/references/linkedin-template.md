# LinkedIn announcement template

Goal: a scannable, emoji-led post that a non-bioinformatician scrolling their
feed will still find inviting. One emoji per highlight, lead with the punchiest
features, keep paragraphs to one or two lines.

## Structure

1. **Hook** — 🧬 + one line: what polars-bio is + the version, with energy.
2. **Highlights** — 3–6 bullets, each `<emoji> **Name** — plain-language value`.
   Order by impact, not by changelog section. Translate changelog jargon into
   user benefit ("less data scanned, faster queries" beats "predicate pushdown").
3. **Install** — `📦 pip install polars-bio==X.Y.Z`
4. **CTA** — ⭐ star on GitHub: https://github.com/biodatageeks/polars-bio
5. **Hashtags** — #Bioinformatics #Genomics #Python #Rust #DataFusion #Polars
   #OpenSource #ComputationalBiology

## Emoji palette (reuse for consistency across releases)

- 🧬 the project / genomics
- 🚀 ⚡ speed / performance
- 📊 signal & track formats (BigWig/BigBed)
- 🧊 Zarr / array-native
- 🧫 FASTA / sequence
- 🔧 engine / dependency bumps
- 🐛 bug fixes / reliability
- 📦 install   ⭐ star CTA

## Example (0.32.0)

> 🧬 **polars-bio 0.32.0 is out!** Blazing-fast genomic operations on large
> Python dataframes, now with even broader format coverage. 🚀
>
> 📊 **BigWig & BigBed I/O** — first-class read/scan/register for signal &
> interval tracks, local + cloud, lazy or eager.
> 🧊 **VCF Zarr, leveled up** — `describe_vcf_zarr()` + `register_vcf_zarr()`.
> 🧫 **FASTA register API** — `register_fasta()` completes the triad.
> ⚡ **Robust pushdown** across all formats — less data scanned, faster queries.
> 🔧 **DataFusion 53** under the hood.
> 🐛 **Reliability fixes** — multi-member gzip FASTQ, count(*) on registered
> FASTQ, correct 0-based half-open BED coordinates.
>
> 📦 `pip install polars-bio==0.32.0`
> ⭐ https://github.com/biodatageeks/polars-bio
>
> #Bioinformatics #Genomics #Python #Rust #DataFusion #Polars #OpenSource #ComputationalBiology
