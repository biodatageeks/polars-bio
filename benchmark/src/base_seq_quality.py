import polars_bio as pb

path = "min_example.fastq"

print('Odczytan zawartość pliku', path)
fastq = pb.read_fastq(path).collect().head()
print(fastq)

print('Base sequence quality dla pliku', path)
test = pb.cacl_base_seq_quality(path)
#test = pb.read_fastq(path).collect().head()
print(test)