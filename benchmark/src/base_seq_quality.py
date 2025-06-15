import polars_bio as pb
import pandas

path = "example.fastq"

print('Odczytan zawartość pliku', path)
fastq = pb.read_fastq(path).collect().head()
print(fastq)

print('Base sequence quality dla pliku', path)
test = pb.cacl_base_seq_quality(path, target_partitions=4, output_type='pandas.DataFrame')
print(test.head())