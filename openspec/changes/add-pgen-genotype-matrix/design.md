# Design

## Why a handle rather than one call

Building a matrix is three steps in a fixed order: ask the file how big it is,
allocate, then fill. The caller owns the allocation, so it cannot happen until
the shape is known — and answering "how big is it" means opening the fileset and
parsing the PVAR, which on chromosome 22 is 108 MB of text.

Any arrangement of free functions parses it twice. `PgenMatrixReader` holds the
opened fileset across both steps, so it is parsed once. This is not a
micro-optimisation: an earlier revision did open it twice, which cost 18% of a
hardcall read and briefly made that workload slower than the DataFrame path it
replaced.

## Where the destination is checked

polars-bio builds against the CPython limited API (`abi3`), where PyO3's buffer
protocol is unavailable, so the reader cannot ask CPython for a validated
pointer. The alternatives were a NumPy build dependency for one function's
benefit, or checking the array from Rust through its Python attributes. The
second was chosen.

The first attempt put those checks in `read_pgen_matrix` and passed the address
across as an integer. That was wrong: the pyclass is registered on the
extension module, so `from polars_bio.polars_bio import PgenMatrixReader`
reaches a `read_into` that would decode into any address it was handed, and a
check the caller can walk around is not a check. The array itself is passed
now, and its type, dtype, C-contiguity, writability, alignment and length are
verified in Rust, on the boundary every caller crosses. The wrapper keeps only
the one check that earns a second copy: the `ALT_COUNT` sentinel, rejected
there before the fileset is opened as well as at the cast.

The decode holds the GIL. Keeping the array alive does not keep its allocation
still — another Python thread could resize it between the checks and the write
— and the export flag that `PyObject_GetBuffer` sets is what would prevent that
without blocking anyone. `Py_buffer` reached the limited API in 3.11, and this
package already requires 3.11, but `datafusion-python` enables bare `abi3`,
which drags the floor to 3.10 whatever this crate asks for. Until that changes
the GIL is the available guarantee, and its cost is other Python threads
waiting out the decode rather than any loss of decode parallelism: the
provider's threads never touch Python.

One residual is recorded rather than fixed. The trusted type comes from
`numpy.ndarray`, a rebindable module attribute; it is read once and cached,
which narrows the window to before the first matrix read rather than closing
it. Closing it needs the buffer protocol. Code that can rebind NumPy's
internals can already reach any address through `ctypes` without this reader,
so the checks are treated as catching accidents — a wrong dtype, a strided
view, a read-only or misaligned array — which they do.

## Why a sentinel and not a validity bitmap

Arrow carries validity separately, so the DataFrame path can distinguish a
missing genotype from a zero without spending a value. A NumPy matrix has no
such channel, so the caller picks a value that means missing: `-9` for
`ALT_COUNT`, matching PLINK's own convention, and NaN for `DS`.

The substitution happens in the decoder rather than in a pass over the result.
Doing it afterwards would mean materialising a validity mask, and an Arrow
integer array with nulls converts to `float64` on the way to NumPy — an
eightfold intermediate on a matrix this size, and an undefined narrowing cast
back for the null positions.

## Why row order is guaranteed here but not in `read_pgen`

A DataFrame scan emits batches as partitions finish them, so rows may interleave
above one partition. The matrix writes each variant at its own row index, which
is its position in the selection, so partitions own disjoint contiguous row
ranges and the order is the PVAR's regardless of how the work divided. That the
workers need no coordination to do this is a consequence of the same fact.

## Why `DS_STORED` is excluded

It is the stored dosage track *without* the hardcall fallback, so it is absent
wherever a record carries no dosage track. Every fast path in the decoder
derives its value from the hardcalls, which is exactly the fallback `DS_STORED`
is defined to omit, so it would have to go through the general decoder for every
record. Supporting it would mean a slow path advertised beside two fast ones,
and `read_pgen` already reads it correctly.
