cd /private/tmp/claude-501/-Users-mwiewior-research-git-polars-bio/063ee24e-8412-45ca-b8d4-f77cb47b5147/scratchpad
LIM=/opt/homebrew/Cellar/fastqc/0.12.1/libexec/Configuration/limits.txt
# Fetch a mid-size file (4.3M reads) so sampling modules (kmer/dup) have enough
# reads to expose any per-worker order dependence.
rm -rf sw; mkdir sw
prefetch ERR5897746 -O sw >/dev/null 2>&1 || { echo PREFETCHFAIL; exit 1; }
fasterq-dump sw/ERR5897746 -O sw --split-files -f -e 4 >/dev/null 2>&1 || { echo DUMPFAIL; exit 1; }
bgzip -i -@4 -c sw/ERR5897746_1.fastq > f.fastq.gz && bgzip -r f.fastq.gz
rm -rf sw
echo "reads: $(python3 -c "import pysam" 2>/dev/null && echo via || true)"

echo "===================== RASTQC (default modules) ====================="
for t in 1 8; do rm -rf rq$t; mkdir rq$t; rastqc -t $t -o rq$t f.fastq.gz >/dev/null 2>&1; (cd rq$t && unzip -oq ./*_fastqc.zip); done
D1=$(find rq1 -name fastqc_data.txt); D8=$(find rq8 -name fastqc_data.txt)
# strip the Filename/date header lines that legitimately vary, then diff
grep -vE '^##FastQC|Filename|File type|Encoding' "$D1" > /tmp/rq1.txt
grep -vE '^##FastQC|Filename|File type|Encoding' "$D8" > /tmp/rq8.txt
if diff -q /tmp/rq1.txt /tmp/rq8.txt >/dev/null; then echo "RASTQC: t1 == t8  (IDENTICAL)"; else
  echo "RASTQC: t1 != t8  (DIFFERS). Modules with differing lines:";
  diff /tmp/rq1.txt /tmp/rq8.txt | grep -E '^[<>]' | head -40
  echo "--- differing module sections ---"
  awk '/^>>/{m=$0} /^[<>]/{print m}' <(diff /tmp/rq1.txt /tmp/rq8.txt) 2>/dev/null | sort -u | head
fi

echo "===================== POLARS-BIO ====================="
env -u CONDA_PREFIX python3 - <<'PY'
import polars_bio as pb, polars as pl
def run(p):
    pb.set_option("datafusion.execution.target_partitions", str(p))
    df = pb.fastqc("f.fastq.gz").tidy.collect().sort(["module","label","position","metric"])
    return df
a = run(1); b = run(8)
same = a.equals(b)
print(f"POLARS-BIO: p1 {'==' if same else '!='} p8  ({'IDENTICAL' if same else 'DIFFERS'})  rows={a.height}")
if not same:
    # show first differing rows
    j = a.join(b, on=["module","label","position","metric"], suffix="_p8")
    d = j.filter(pl.col("value") != pl.col("value_p8"))
    print("differing rows:", d.height)
    print(d.select(["module","label","position","metric","value","value_p8"]).head(20))
PY
rm -f f.fastq.gz f.fastq.gz.gzi
echo TI_DONE
