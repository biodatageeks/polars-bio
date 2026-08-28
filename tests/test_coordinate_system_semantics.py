"""Value-level tests for every range operation on BOTH coordinate paths.

Why this file exists
--------------------
``tests/test_bioframe.py`` and ``TestCoverageNative`` pin every fixture to
``zero_based=True`` because bioframe -- their reference -- is 0-based half-open.
That is the right call for those tests, but it leaves the entire 1-based path
outside what they can assert. The partitioned-execution suites
(``test_lazyframe_partitioning.py``, ``test_partitioned_range_operation_regressions.py``)
do run 1-based, but they compare polars-bio against polars-bio, so a value that
is wrong-but-consistent passes.

Issue #450 fell straight through that gap: ``coverage()`` returned three
different answers on the 1-based path where all three had to agree, and nothing
in the suite could see it.

How the expectations are built
------------------------------
Every expectation here is *derived*, never transcribed from output. The
reference enumerates the actual base positions an interval occupies
(``_covered_bases``) and answers each operation as a set question. A test
therefore cannot agree with the implementation merely by having been generated
from it.

Under 1-based inclusive coordinates ``[start, end]`` both endpoints are covered,
so a range spans ``end - start + 1`` bases. Under 0-based half-open
``[start, end)`` it spans ``end - start``. Nearly every defect in this area is a
missing ``+ 1`` on one of those two paths, which is why each operation is
checked against both.

Defects this file pins
----------------------
Every case here passes. Five of them were failing when the file was written and
are now regression guards:

* ``coverage`` on the 1-based path -- polars-bio#450, upstream #220.
* ``count_overlaps`` and ``nearest`` silently missing 0-based intervals of
  length <= 1, and ``subtract``/``complement`` emitting half-open boundaries on
  the 1-based path -- upstream #221.
"""

import random

import polars as pl
import pytest

import polars_bio as pb

CHROM = "chr1"

ZERO_BASED = pytest.param(True, id="0-based")
ONE_BASED = pytest.param(False, id="1-based")


BOTH_SYSTEMS = [ZERO_BASED, ONE_BASED]


# --------------------------------------------------------------------------
# Reference model: an interval is the set of base positions it occupies.
# --------------------------------------------------------------------------


def _covered_bases(intervals, zero_based: bool) -> set[int]:
    covered: set[int] = set()
    for start, end in intervals:
        covered |= set(range(start, end if zero_based else end + 1))
    return covered


def _as_intervals(bases: set[int], zero_based: bool) -> list[tuple[int, int]]:
    """Collapse base positions back into maximal contiguous runs."""
    runs: list[list[int]] = []
    for position in sorted(bases):
        if runs and position == runs[-1][1] + 1:
            runs[-1][1] = position
        else:
            runs.append([position, position])
    return [(lo, hi + 1 if zero_based else hi) for lo, hi in runs]


def _shares_a_base(left, right, zero_based: bool) -> bool:
    return bool(
        _covered_bases([left], zero_based) & _covered_bases([right], zero_based)
    )


def _frame(intervals, zero_based: bool) -> pl.DataFrame:
    df = pl.DataFrame(
        {
            "chrom": [CHROM] * len(intervals),
            "start": [start for start, _ in intervals],
            "end": [end for _, end in intervals],
        }
    )
    df.config_meta.set(coordinate_system_zero_based=zero_based)
    return df


def _intervals_of(df: pl.DataFrame) -> list[tuple[int, int]]:
    return sorted((row[0], row[1]) for row in df.select("start", "end").rows())


# --------------------------------------------------------------------------
# Reference answers
# --------------------------------------------------------------------------


def _ref_coverage(query, targets, zero_based: bool) -> int:
    return len(
        _covered_bases([query], zero_based) & _covered_bases(targets, zero_based)
    )


def _ref_merge(intervals, zero_based: bool) -> list[tuple[int, int]]:
    """polars-bio joins intervals that SHARE a base, not merely adjacent ones.

    See ``test_merge_leaves_bookended_intervals_separate`` -- that policy is
    uniform across both coordinate systems, so the reference mirrors it rather
    than collapsing the union of bases.
    """
    components: list[tuple[int, int]] = []
    for interval in sorted(intervals):
        if components and _shares_a_base(components[-1], interval, zero_based):
            components[-1] = (
                components[-1][0],
                max(components[-1][1], interval[1]),
            )
        else:
            components.append(interval)
    return sorted(components)


def _ref_subtract(minuends, subtrahends, zero_based: bool) -> list[tuple[int, int]]:
    """subtract() works per input row, so rows may overlap in the output."""
    removed = _covered_bases(subtrahends, zero_based)
    return sorted(
        run
        for interval in minuends
        for run in _as_intervals(
            _covered_bases([interval], zero_based) - removed, zero_based
        )
    )


def _ref_interior_gaps(intervals, zero_based: bool) -> list[tuple[int, int]]:
    covered = _covered_bases(intervals, zero_based)
    span = set(range(min(covered), max(covered) + 1))
    return _as_intervals(span - covered, zero_based)


def _ref_distance(left, right, zero_based: bool) -> int:
    """Number of bases strictly between two non-overlapping intervals."""
    if _shares_a_base(left, right, zero_based):
        return 0
    left_bases = _covered_bases([left], zero_based)
    right_bases = _covered_bases([right], zero_based)
    if max(left_bases) < min(right_bases):
        return len(range(max(left_bases) + 1, min(right_bases)))
    return len(range(max(right_bases) + 1, min(left_bases)))


def _interior_of(result: pl.DataFrame, intervals, zero_based: bool):
    """complement() pads with sentinel flanks (0 and i64::MAX); drop them."""
    covered = _covered_bases(intervals, zero_based)
    lo, hi = min(covered), max(covered)
    return [
        interval
        for interval in _intervals_of(result)
        if interval[0] > lo and (interval[1] - 1 if zero_based else interval[1]) < hi
    ]


# --------------------------------------------------------------------------
# Randomised differential audit -- every operation, both coordinate systems.
# --------------------------------------------------------------------------

CASES = 60


def _random_cases(seed: int):
    rng = random.Random(seed)
    for _ in range(CASES):
        left = sorted({_rand(rng) for _ in range(rng.randint(1, 4))})
        right = sorted({_rand(rng) for _ in range(rng.randint(1, 3))})
        yield left, right


def _rand(rng) -> tuple[int, int]:
    start = rng.randint(0, 60)
    return start, start + rng.randint(1, 20)


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
def test_coverage_matches_reference(zero_based):
    for left, right in _random_cases(seed=11):
        query = left[0]
        result = pb.coverage(
            _frame([query], zero_based),
            _frame(right, zero_based),
            output_type="polars.DataFrame",
        )
        assert result["coverage"][0] == _ref_coverage(
            query, right, zero_based
        ), f"query={query} targets={right}"


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
def test_overlap_matches_reference(zero_based):
    # Same seed as test_count_overlaps_matches_reference: the two must agree
    # with the reference -- and so with each other -- on identical pairs.
    for left, right in _random_cases(seed=12):
        a, b = left[0], right[0]
        overlap = pb.overlap(
            _frame([a], zero_based),
            _frame([b], zero_based),
            output_type="polars.DataFrame",
        )
        assert (len(overlap) > 0) is _shares_a_base(a, b, zero_based), f"{a} vs {b}"


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
def test_count_overlaps_matches_reference(zero_based):
    # Same pairs as test_overlap_matches_reference -- see the note there.
    for left, right in _random_cases(seed=12):
        a, b = left[0], right[0]
        counts = pb.count_overlaps(
            _frame([a], zero_based),
            _frame([b], zero_based),
            output_type="polars.DataFrame",
        )
        assert (counts["count"][0] > 0) is _shares_a_base(
            a, b, zero_based
        ), f"{a} vs {b}"


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
def test_merge_matches_reference(zero_based):
    for left, _ in _random_cases(seed=13):
        result = pb.merge(_frame(left, zero_based), output_type="polars.DataFrame")
        assert _intervals_of(result) == _ref_merge(left, zero_based), f"input={left}"


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
def test_subtract_matches_reference(zero_based):
    for left, right in _random_cases(seed=14):
        result = pb.subtract(
            _frame(left, zero_based),
            _frame(right, zero_based),
            output_type="polars.DataFrame",
        )
        assert _intervals_of(result) == _ref_subtract(
            left, right, zero_based
        ), f"minuends={left} subtrahends={right}"


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
def test_complement_matches_reference(zero_based):
    for left, _ in _random_cases(seed=15):
        result = pb.complement(_frame(left, zero_based), output_type="polars.DataFrame")
        assert _interior_of(result, left, zero_based) == _ref_interior_gaps(
            left, zero_based
        ), f"input={left}"


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
def test_nearest_distance_matches_reference(zero_based):
    for left, right in _random_cases(seed=16):
        a, b = left[0], right[0]
        result = pb.nearest(
            _frame([a], zero_based),
            _frame([b], zero_based),
            output_type="polars.DataFrame",
        )
        assert result["distance"][0] == _ref_distance(a, b, zero_based), f"{a} vs {b}"


# --------------------------------------------------------------------------
# Explicit boundary geometry.
#
# The number of *clamped edges* is what separates a correct implementation from
# an off-by-one one, so these name each shape rather than relying on the random
# sweep to stumble into it. A fixture of only partially-overlapping pairs -- the
# one shape that was accidentally correct -- is exactly why #450 survived.
# --------------------------------------------------------------------------

QUERY = (100, 200)
SHAPES = [
    ((100, 200), "identical"),
    ((50, 250), "superset"),
    ((120, 130), "contained"),
    ((50, 150), "overhangs left"),
    ((180, 400), "overhangs right"),
    ((201, 300), "bookended right"),
    ((900, 950), "disjoint"),
]


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
@pytest.mark.parametrize("target,shape", SHAPES)
def test_coverage_boundary_shapes(zero_based, target, shape):
    result = pb.coverage(
        _frame([QUERY], zero_based),
        _frame([target], zero_based),
        output_type="polars.DataFrame",
    )
    assert result["coverage"][0] == _ref_coverage(QUERY, [target], zero_based), shape


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
def test_coverage_identical_and_superset_agree(zero_based):
    """Both must return the query's own length.

    This is the internal inconsistency from issue #450 -- judging it needs no
    cross-library reference, only the query's own span.
    """
    query_length = len(_covered_bases([QUERY], zero_based))
    values = [
        pb.coverage(
            _frame([QUERY], zero_based),
            _frame([target], zero_based),
            output_type="polars.DataFrame",
        )["coverage"][0]
        for target in [(100, 200), (50, 250), (0, 1000)]
    ]
    assert values == [query_length] * 3


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
def test_coverage_never_exceeds_query_length(zero_based):
    query_length = len(_covered_bases([QUERY], zero_based))
    for target, shape in SHAPES:
        value = pb.coverage(
            _frame([QUERY], zero_based),
            _frame([target], zero_based),
            output_type="polars.DataFrame",
        )["coverage"][0]
        assert (
            0 <= value <= query_length
        ), f"{shape}: {value} outside 0..={query_length}"


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
def test_coverage_of_degenerate_target_follows_the_convention(zero_based):
    """``[150,150]`` means different things in the two systems, so the answers differ.

    Half-open it is empty and covers nothing; 1-based inclusive it is a genuine
    single base. ``get_coverage`` used to floor each contribution at 1, so the
    0-based case reported one covered base.
    """
    result = pb.coverage(
        _frame([QUERY], zero_based),
        _frame([(150, 150)], zero_based),
        output_type="polars.DataFrame",
    )
    assert result["coverage"][0] == (0 if zero_based else 1)


# --------------------------------------------------------------------------
# Adjacency policy -- uniform across both systems, so not a coordinate defect.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "zero_based,neighbours",
    [(True, [(10, 20), (20, 30)]), (False, [(10, 20), (21, 30)])],
    ids=["0-based", "1-based"],
)
def test_merge_leaves_bookended_intervals_separate(zero_based, neighbours):
    """Characterisation, not a correctness claim.

    Both pairs are contiguous -- they leave no uncovered base between them --
    yet merge() keeps them apart in both coordinate systems. That makes it a
    consistent choice about what ``min_dist=0`` means rather than a
    coordinate-system defect. It does differ from ``bioframe.merge``, which
    joins bookended intervals.
    """
    result = pb.merge(_frame(neighbours, zero_based), output_type="polars.DataFrame")
    assert _intervals_of(result) == sorted(neighbours)


@pytest.mark.parametrize(
    "zero_based,neighbours",
    [(True, [(10, 21), (20, 30)]), (False, [(10, 20), (20, 30)])],
    ids=["0-based", "1-based"],
)
def test_merge_joins_intervals_sharing_one_base(zero_based, neighbours):
    result = pb.merge(_frame(neighbours, zero_based), output_type="polars.DataFrame")
    assert _intervals_of(result) == [(10, 30)]


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
def test_cluster_groups_overlapping_intervals(zero_based):
    intervals = [(10, 20), (15, 25), (40, 50)]
    result = pb.cluster(
        _frame(intervals, zero_based), output_type="polars.DataFrame"
    ).sort("start")
    assert result["cluster"].to_list() == [0, 0, 1]
    assert result["cluster_start"].to_list() == [10, 10, 40]
    assert result["cluster_end"].to_list() == [25, 25, 50]


# --------------------------------------------------------------------------
# Short intervals.
#
# A 0-based interval of length 1 -- a SNV, a point feature -- is narrowed to an
# empty range by the FilterOp::Strict pre-step, and operations that compare the
# narrowed bounds before measuring anything then treat it as matching nothing.
# The random sweep above only stumbles into this, so name it explicitly.
# --------------------------------------------------------------------------

SHORT_QUERY_TARGET = (9, 28)


@pytest.mark.parametrize(
    "query,length",
    [((17, 18), 1), ((17, 19), 2), ((15, 20), 5)],
    ids=["1-base", "2-base", "5-base"],
)
def test_zero_based_short_interval_ops_agree(query, length):
    """overlap(), count_overlaps() and coverage() must agree that a query
    strictly inside a target does overlap it, whatever its length."""
    args = (_frame([query], True), _frame([SHORT_QUERY_TARGET], True))
    overlap_rows = len(pb.overlap(*args, output_type="polars.DataFrame"))
    count = pb.count_overlaps(*args, output_type="polars.DataFrame")["count"][0]
    covered = pb.coverage(*args, output_type="polars.DataFrame")["coverage"][0]

    assert overlap_rows == 1
    assert covered == length
    assert count == 1


@pytest.mark.parametrize(
    "query,length",
    [((17, 18), 1), ((17, 19), 2), ((15, 20), 5)],
    ids=["1-base", "2-base", "5-base"],
)
def test_zero_based_short_interval_nearest_finds_neighbour(query, length):
    """nearest() must find the enclosing target regardless of query length."""
    result = pb.nearest(
        _frame([query], True),
        _frame([SHORT_QUERY_TARGET], True),
        output_type="polars.DataFrame",
    )
    assert result["start_2"][0] == SHORT_QUERY_TARGET[0]
    assert result["distance"][0] == 0


# --------------------------------------------------------------------------
# complement() against an explicit view.
#
# The randomised sweep above uses the default view, so it never exercises the
# clipping of an interval against a view boundary -- which is its own
# coordinate question: under inclusive coordinates an interval ending exactly
# at the view start still covers one base inside the view.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("zero_based", BOTH_SYSTEMS)
@pytest.mark.parametrize(
    "intervals",
    [
        [(0, 10)],  # touches the view start
        [(20, 30)],  # touches the view end
        [(0, 10), (20, 30)],  # both
        [(12, 15)],  # strictly inside
        [(0, 5)],  # entirely before the view
    ],
    ids=["touches-start", "touches-end", "both", "inside", "before"],
)
def test_complement_against_view_matches_reference(zero_based, intervals):
    view = (10, 20)
    covered = _covered_bases(intervals, zero_based) & _covered_bases([view], zero_based)
    expected = _as_intervals(_covered_bases([view], zero_based) - covered, zero_based)

    result = pb.complement(
        _frame(intervals, zero_based),
        view_df=_frame([view], zero_based),
        output_type="polars.DataFrame",
    )
    assert _intervals_of(result) == expected
