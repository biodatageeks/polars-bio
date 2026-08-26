"""Generate small cooler fixtures with the reference implementation.

Run from this directory (regenerates the fixtures in place):

    python generate_fixtures.py
"""

import cooler
import h5py
import numpy as np
import pandas as pd

rng = np.random.default_rng(42)

chromsizes = pd.Series({"chr1": 100_000, "chr2": 60_000})
binsize = 1_000
bins = cooler.binnify(chromsizes, binsize)
n_bins = len(bins)
print(f"bins: {n_bins}")

# random upper-triangle pixels
n = 5_000
b1 = rng.integers(0, n_bins, n)
b2 = rng.integers(0, n_bins, n)
lo, hi = np.minimum(b1, b2), np.maximum(b1, b2)
pixels = (
    pd.DataFrame({"bin1_id": lo, "bin2_id": hi, "count": rng.integers(1, 100, n)})
    .groupby(["bin1_id", "bin2_id"], as_index=False)["count"]
    .sum()
    .sort_values(["bin1_id", "bin2_id"])
)
print(f"pixels: {len(pixels)}")

cooler.create_cooler("test.cool", bins, pixels, assembly="toyGenome")

# multi-resolution + balancing weights
cooler.zoomify_cooler(
    "test.cool", "test.mcool", resolutions=[1000, 2000, 5000], chunksize=10_000
)
for res in (1000, 2000, 5000):
    clr = cooler.Cooler(f"test.mcool::/resolutions/{res}")
    cooler.balance_cooler(clr, store=True, min_nnz=2)

# Single-resolution .mcool exercises unambiguous automatic collection
# selection when no resolution or cooler URI group is supplied.
cooler.zoomify_cooler(
    "test.cool", "test_single_resolution.mcool", resolutions=[1000], chunksize=10_000
)

# float-count variant
fp = pixels.copy()
fp["count"] = fp["count"].astype("float64") * 0.5
cooler.create_cooler("test_float.cool", bins, fp, dtypes={"count": "float64"})

# int64-count variant with a value above i32::MAX, so any i32 truncation in a
# reader is caught by value comparison rather than passing silently.
ip = pixels.copy()
ip["count"] = ip["count"].astype("int64")
ip.loc[ip.index[0], "count"] = 5_000_000_000
cooler.create_cooler(
    "test_int64.cool", bins, ip, dtypes={"count": "int64"}, assembly="toyGenome"
)

# Unsigned-count variants exercise the ranges that do not fit the signed Arrow
# type of the same width.
u32p = pixels.copy()
u32p["count"] = u32p["count"].astype("uint32")
u32p.loc[u32p.index[0], "count"] = 3_000_000_000
cooler.create_cooler(
    "test_uint32.cool", bins, u32p, dtypes={"count": "uint32"}, assembly="toyGenome"
)

u64p = pixels.copy()
u64p["count"] = u64p["count"].astype("uint64")
u64p.loc[u64p.index[0], "count"] = 10_000_000_000_000_000_000
cooler.create_cooler(
    "test_uint64.cool", bins, u64p, dtypes={"count": "uint64"}, assembly="toyGenome"
)

# One exact integer sum above f64's 53-bit integer range. Metadata readers must
# not round it while accommodating fractional sums from float-count coolers.
exact_pixels = pixels.iloc[[0]].copy()
exact_pixels["count"] = np.array([9_007_199_254_740_993], dtype="int64")
cooler.create_cooler(
    "test_exact_sum.cool",
    bins,
    exact_pixels,
    dtypes={"count": "int64"},
    assembly="toyGenome",
)

# Coordinates can legally use int64 storage beyond the UInt32 range. The second
# bin begins above u32::MAX, catching narrowing in the joined Arrow output.
wide_bins = pd.DataFrame(
    {
        "chrom": ["chrWide", "chrWide"],
        "start": np.array([0, 5_000_000_000], dtype="int64"),
        "end": np.array([5_000_000_000, 6_000_000_000], dtype="int64"),
    }
)
wide_pixels = pd.DataFrame(
    {
        "bin1_id": np.array([1], dtype="int64"),
        "bin2_id": np.array([1], dtype="int64"),
        "count": np.array([1], dtype="int32"),
    }
)
cooler.create_cooler("test_wide_coords.cool", wide_bins, wide_pixels, ordered=True)
# cooler's writer currently coerces bin coordinates to int32. Replace these two
# datasets with the valid int64 storage used by other cooler-producing tools.
with h5py.File("test_wide_coords.cool", "r+") as h5:
    del h5["bins/start"]
    del h5["bins/end"]
    h5["bins"].create_dataset("start", data=np.array([0, 5_000_000_000], dtype="int64"))
    h5["bins"].create_dataset(
        "end", data=np.array([5_000_000_000, 6_000_000_000], dtype="int64")
    )

# cooler <=0.8.x wrote some numeric attrs as JSON strings; mimic that on the
# float fixture so readers keep tolerating string-typed numeric attributes.
with h5py.File("test_float.cool", "r+") as h5:
    h5.attrs["format-version"] = str(int(h5.attrs["format-version"]))

# Multi-resolution metadata may legitimately mix independent float- and
# integer-count collections. Keep one exact integer total above 2^53 beside a
# fractional float total to catch whole-file describe paths that coerce or
# reject the heterogeneous values.
with (
    h5py.File("test_mixed_sums.mcool", "w") as target_h5,
    h5py.File("test_float.cool", "r") as float_h5,
    h5py.File("test_exact_sum.cool", "r") as integer_h5,
):
    target_h5.attrs["format"] = "HDF5::MCOOL"
    target_h5.attrs["format-version"] = 2
    resolutions = target_h5.create_group("resolutions")
    for resolution, source in ((1000, float_h5), (2000, integer_h5)):
        target = resolutions.create_group(str(resolution))
        for name, value in source.attrs.items():
            target.attrs[name] = value
        for name in source:
            source.copy(name, target)

for uri in [
    "test.cool",
    "test.mcool::/resolutions/2000",
    "test_single_resolution.mcool::/resolutions/1000",
    "test_float.cool",
]:
    c = cooler.Cooler(uri)
    print(uri, c.info["nnz"], c.pixels()[:3].to_dict("list"))
print(
    "weights present:",
    "weight" in cooler.Cooler("test.mcool::/resolutions/1000").bins().columns,
)
