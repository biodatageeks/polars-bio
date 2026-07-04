cd /private/tmp/claude-501/-Users-mwiewior-research-git-polars-bio/063ee24e-8412-45ca-b8d4-f77cb47b5147/scratchpad
rm -rf sw; mkdir sw
prefetch ERR5897746 -O sw >/dev/null 2>&1 && fasterq-dump sw/ERR5897746 -O sw --split-files -f -e 4 >/dev/null 2>&1
bgzip -i -@4 -c sw/ERR5897746_1.fastq > f.fastq.gz && bgzip -r f.fastq.gz
rm -rf sw
echo "file: $(ls -la f.fastq.gz | awk '{print $5}') bytes"
echo "cores: $(sysctl -n hw.ncpu)"
echo
printf "%-4s %-8s %-8s %-8s %-14s %s\n" "-t" "wall" "user" "sys" "eff_cores" "rastqc --time breakdown"
for t in 1 2 4 8 16; do
  rm -rf o; mkdir o
  /usr/bin/time -p rastqc -t $t --time --nozip -o o f.fastq.gz > log_$t.txt 2>&1
  real=$(grep '^real' log_$t.txt | awk '{print $2}')
  user=$(grep '^user' log_$t.txt | awk '{print $2}')
  sys=$(grep '^sys'  log_$t.txt | awk '{print $2}')
  eff=$(echo "scale=1; ($user+$sys)/$real" | bc)
  # RastQC --time breakdown lines (read/parse vs compute/QC)
  bd=$(grep -iE "read|parse|decompress|comput|qc|module|process|total|elapsed|time" log_$t.txt | grep -ivE "^real|^user|^sys" | tr '\n' ' | ' | cut -c1-90)
  printf "%-4s %-8s %-8s %-8s %-14s %s\n" "$t" "$real" "$user" "$sys" "$eff" "$bd"
done
rm -f f.fastq.gz f.fastq.gz.gzi
echo RQ_SCALING_DONE
