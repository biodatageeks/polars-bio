cd /private/tmp/claude-501/-Users-mwiewior-research-git-polars-bio/063ee24e-8412-45ca-b8d4-f77cb47b5147/scratchpad
bgzip -i -@4 -c sw/ERR5897746_1.fastq > f.fastq.gz && bgzip -r f.fastq.gz
[ -s f.fastq.gz ] || { echo "BGZF FAILED"; exit 1; }
echo "file: $(ls -la f.fastq.gz|awk '{print $5}') bytes | cores: $(sysctl -n hw.ncpu) (12P+4E)"
echo "=== RastQC (compute-parallel; single-threaded reader) ==="
printf "%-4s%-9s%-9s%-11s\n" "-t" "wall" "cpu" "eff_cores"
for t in 1 2 4 6 8 10; do
  rm -rf o; mkdir o
  /usr/bin/time -p rastqc -t $t --nozip -o o f.fastq.gz > log_rq_$t.txt 2>&1
  real=$(grep '^real' log_rq_$t.txt|awk '{print $2}'); user=$(grep '^user' log_rq_$t.txt|awk '{print $2}'); sys=$(grep '^sys' log_rq_$t.txt|awk '{print $2}')
  printf "%-4s%-9s%-9s%-11s\n" "$t" "$real" "$(echo "$user+$sys"|bc)" "$(echo "scale=1;($user+$sys)/$real"|bc)"
done
echo "=== polars-bio (reads+computes in parallel; collect() CPU via getrusage) ==="
env -u CONDA_PREFIX python3 pb_scaling.py 2>/dev/null
rm -f f.fastq.gz f.fastq.gz.gzi; rm -rf sw
echo BOTH_DONE
