import polars_bio as pb

path = "min_example.fastq"

test = pb.cacl_base_seq_quality(path)
print(test)