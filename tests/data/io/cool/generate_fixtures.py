"""Generate small cooler fixtures with the reference implementation (spike task 1.3)."""

import cooler
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

cooler.create_cooler("fixtures/test.cool", bins, pixels, assembly="toyGenome")

# multi-resolution + balancing weights
cooler.zoomify_cooler(
    "fixtures/test.cool",
    "fixtures/test.mcool",
    resolutions=[1000, 2000, 5000],
    chunksize=10_000,
)
for res in (1000, 2000, 5000):
    clr = cooler.Cooler(f"fixtures/test.mcool::/resolutions/{res}")
    cooler.balance_cooler(clr, store=True, min_nnz=2)

# float-count variant
fp = pixels.copy()
fp["count"] = fp["count"].astype("float64") * 0.5
cooler.create_cooler("fixtures/test_float.cool", bins, fp, dtypes={"count": "float64"})

# cooler <=0.8.x wrote some numeric attrs as JSON strings; mimic that on the
# float fixture so readers keep tolerating string-typed numeric attributes.
import h5py

with h5py.File("fixtures/test_float.cool", "r+") as h5:
    h5.attrs["format-version"] = str(int(h5.attrs["format-version"]))

for uri in [
    "fixtures/test.cool",
    "fixtures/test.mcool::/resolutions/2000",
    "fixtures/test_float.cool",
]:
    c = cooler.Cooler(uri)
    print(uri, c.info["nnz"], c.pixels()[:3].to_dict("list"))
print(
    "weights present:",
    "weight" in cooler.Cooler("fixtures/test.mcool::/resolutions/1000").bins().columns,
)
