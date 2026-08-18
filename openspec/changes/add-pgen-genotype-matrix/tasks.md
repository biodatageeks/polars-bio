# Tasks

## 1. Provider support

- [x] 1.1 Decode a genotype field into a caller-owned slice, writing each
      variant at its own row index
- [x] 1.2 Expose the opened fileset as a reader answering shape, sample names
      and positions, so the companions are parsed once rather than once per
      question
- [x] 1.3 Test the matrix against the Arrow scan cell for cell, on the fixture
      holding every record representation, at one, two, four and eight threads

## 2. Binding

- [x] 2.1 Expose the reader to Python as a handle, since the caller must learn
      the shape before it can allocate
- [x] 2.2 Pass the destination as an address, the limited API having no buffer
      protocol, and check dtype, contiguity, writability and length in the
      Python wrapper before handing it over
- [x] 2.3 Release the GIL for the decode

## 3. Python API

- [x] 3.1 Add `read_pgen_matrix` returning values, positions and sample names
- [x] 3.2 Default `copy_threads` to `datafusion.execution.target_partitions`
- [x] 3.3 Reject fields without a dense form, pointing at `read_pgen`
- [x] 3.4 Import NumPy inside the function, so `import polars_bio` does not
      require it
- [x] 3.5 Test against the DataFrame path: values, missing sentinels, sample
      subsetting, field rejection, and partition independence

## 4. Documentation

- [x] 4.1 Add to the API reference and the PGEN feature page
- [x] 4.2 Record the measurements and the correctness gate in the benchmark
      writeup and the blog post
