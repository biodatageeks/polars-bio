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

## Why the destination is passed as an address

polars-bio builds against the CPython limited API (`abi3`), where PyO3's buffer
protocol is unavailable. The alternatives were to take a NumPy build dependency
or to pass the array's address explicitly. The address was chosen because the
dependency would apply to the whole package for one function's benefit.

The cost is that the checks the buffer protocol would have made are now the
Python wrapper's responsibility: dtype, C-contiguity, writability and length are
all verified before the address is handed over, and the decode re-checks the
length against the shape it derives from the fileset. Both `unsafe` blocks name
the check that makes them sound. `read_pgen_matrix` is the only supported caller
of the binding, and the binding says so.

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
