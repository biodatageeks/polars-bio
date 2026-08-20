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
- [x] 2.2 Pass the destination array itself and check its type, dtype,
      contiguity, writability, alignment and length in Rust, so the checks sit
      on the boundary every caller crosses rather than in the Python wrapper a
      caller holding the reader goes around
- [x] 2.3 Hold the GIL for the decode, so no Python thread can resize or free
      the destination between the checks and the write. The buffer protocol
      would pin the export instead, at no cost to other Python threads, but
      `Py_buffer` reached the limited API in 3.11 and `datafusion-python`
      enables bare `abi3`, pinning this build's floor at 3.10
- [x] 2.4 Refuse an `ALT_COUNT` sentinel `int8` cannot hold at the cast as well
      as in the wrapper, the wrapper's copy earning its place by failing before
      the fileset is opened

## 3. Python API

- [x] 3.1 Add `read_pgen_matrix` returning values, positions and sample names
- [x] 3.2 Default `copy_threads` to `datafusion.execution.target_partitions`
- [x] 3.3 Reject fields without a dense form, pointing at `read_pgen`, and
      reject an `ALT_COUNT` sentinel `int8` cannot hold
- [x] 3.4 Import NumPy inside the function, so `import polars_bio` does not
      require it
- [x] 3.5 Test against the DataFrame path: values, missing sentinels, sample
      subsetting, field rejection, and partition independence

## 4. Documentation

- [x] 4.1 Add to the API reference and the PGEN feature page
- [x] 4.2 Record the measurements and the correctness gate in the benchmark
      writeup and the blog post
