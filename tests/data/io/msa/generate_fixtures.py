#!/usr/bin/env python
"""Regenerate the MSA (A2M / A3M / Stockholm) test fixtures in this directory.

Run by a maintainer; the outputs are checked in so the test suite itself needs
only ``pyhmmer`` and ``biopython``.

Requirements at generation time:

* ``esl-reformat`` and ``esl-alistat`` (HMMER 3.x / Easel miniapps) on ``PATH``
* ``perl`` (hh-suite's ``reformat.pl`` is fetched into a temp dir; it is GPL-3
  and is deliberately **not** vendored into this Apache-2.0 repository)
* ``bgzip`` (htslib) for the ``.bgz`` variants
* ``pyhmmer`` importable (for the ``hmmalign``-generated ``#=GR PP`` fixture)
* network access to fetch the source alignments

The generator asserts the cross-implementation agreements documented in
``openspec/changes/add-msa-alignment-formats/design.md`` and refuses to write
anything if they do not hold: hh-suite and Easel must produce byte-identical
dotted A2M from the same A3M, Easel's canonical single-block Stockholm must
preserve every ``#=GS`` line and every ``#=GF`` feature except the ones Easel
parses and normalises (``GA``/``TC``/``NC``), and A3M must round-trip through
Stockholm and back unchanged.

Usage::

    PATH=/path/to/hmmer/easel/miniapps:$PATH python tests/data/io/msa/generate_fixtures.py
"""

from __future__ import annotations

import datetime as _dt
import gzip
import json
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent

SOURCES = {
    # Pfam seed alignment, single block, 49 #=GF lines with repeated DR/DC/WK/CC.
    "PF00001.sto": (
        "https://www.ebi.ac.uk/interpro/wwwapi/entry/pfam/PF00001/?annotation=alignment:seed",
        "gzip",
    ),
    # Rfam seed alignment, interleaved (two blocks per sequence), RNA, SS_cons.
    "RF00001.sto": ("https://rfam.org/family/RF00001/alignment/stockholm", None),
    # hh-suite example query MSA: 59 sequences, ragged, no header lines.
    "query.a3m": (
        "https://raw.githubusercontent.com/soedinglab/hh-suite/master/data/query.a3m",
        None,
    ),
    # hh-suite hhpred example: ss_dssp/ss_pred/ss_conf pseudo-sequences before the query.
    "_test_full.a3m": (
        "https://raw.githubusercontent.com/soedinglab/hh-suite/master/scripts/hhpred/example/test.a3m",
        None,
    ),
}
REFORMAT_PL = (
    "https://raw.githubusercontent.com/soedinglab/hh-suite/master/scripts/reformat.pl"
)

# #=GF features that Easel parses into typed fields and re-emits normalised
# (``30.50 30.50;`` becomes ``30.5 30.5``, and the three are reordered).
EASEL_NORMALISED_GF = {"GA", "TC", "NC"}

PSEUDO_PREFIXES = ("ss_", "sa_", "aa_")
TEST_HEAD_HOMOLOGS = 200


def die(msg: str) -> None:
    sys.exit(f"generate_fixtures: {msg}")


def fetch(url: str, decompress: str | None) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as resp:  # noqa: S310 - fixed URLs
        data = resp.read()
    if decompress == "gzip":
        data = gzip.decompress(data)
    return data


def run(*cmd: str, stdin: bytes | None = None) -> bytes:
    proc = subprocess.run(cmd, input=stdin, capture_output=True, check=False)
    if proc.returncode != 0:
        die(f"{' '.join(cmd)} failed:\n{proc.stderr.decode(errors='replace')}")
    return proc.stdout


def fasta_records(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    header, buf = None, []
    for line in text.splitlines():
        if line.startswith(">"):
            if header is not None:
                out.append((header, "".join(buf)))
            header, buf = line[1:].strip(), []
        elif line.strip() and header is not None:
            buf.append(line.strip())
    if header is not None:
        out.append((header, "".join(buf)))
    return out


def write_fasta(records: list[tuple[str, str]]) -> str:
    return "".join(f">{h}\n{s}\n" for h, s in records)


def sto_lines(text: str, prefix: str) -> list[str]:
    return [
        re.sub(r"\s+", " ", line).strip()
        for line in text.splitlines()
        if line.startswith(prefix)
    ]


def alistat(
    path: Path, informat: str | None = None, alphabet: str | None = None
) -> list[dict[str, int]]:
    cmd = ["esl-alistat"]
    if informat:
        cmd += ["--informat", informat]
    if alphabet:
        # Easel guesses the alphabet from residue counts; tiny fixtures need a hint.
        cmd += [f"--{alphabet}"]
    out = run(*cmd, str(path)).decode()
    alns: list[dict[str, int]] = []
    for line in out.splitlines():
        if line.startswith("Alignment number:"):
            alns.append({})
        elif line.startswith("Number of sequences:"):
            alns[-1]["n_sequences"] = int(line.split()[-1])
        elif line.startswith("Alignment length:"):
            alns[-1]["alignment_length"] = int(line.split()[-1])
    return alns


def check_tools() -> None:
    for tool in ("esl-reformat", "esl-alistat", "perl", "bgzip"):
        if shutil.which(tool) is None:
            die(f"{tool} not found on PATH")
    try:
        import pyhmmer  # noqa: F401
    except ImportError:
        die("pyhmmer is required (pip install pyhmmer)")


def main() -> None:
    check_tools()
    from pyhmmer.easel import MSAFile

    tmp = Path(tempfile.mkdtemp(prefix="msa-fixtures-"))
    today = _dt.date.today().isoformat()
    expected: dict[str, object] = {"generated": today, "stockholm": {}, "a3m": {}}

    # ---- 1. sources ---------------------------------------------------------
    raw: dict[str, str] = {}
    for name, (url, decompress) in SOURCES.items():
        raw[name] = fetch(url, decompress).decode()
    reformat_pl = tmp / "reformat.pl"
    reformat_pl.write_bytes(fetch(REFORMAT_PL, None))

    # Vendored as-is.
    for name in ("PF00001.sto", "RF00001.sto", "query.a3m"):
        (HERE / name).write_text(raw[name])

    # test.a3m is 2,547 records; keep the three pseudo-sequences plus the query
    # and the first homologs so the fixture stays small.
    full = fasta_records(raw["_test_full.a3m"])
    pseudo = [r for r in full if r[0].split()[0].startswith(PSEUDO_PREFIXES)]
    real = [r for r in full if not r[0].split()[0].startswith(PSEUDO_PREFIXES)]
    if len(pseudo) != 3 or real[0][0].split()[0] != "1a7j_A":
        die("unexpected layout of hh-suite test.a3m")
    head = pseudo + real[: 1 + TEST_HEAD_HOMOLOGS]
    (HERE / "test_head.a3m").write_text(write_fasta(head))

    # ---- 2. A3M -> dotted A2M: hh-suite and Easel must agree byte-for-byte --
    src = tmp / "query.a3m"
    src.write_text(raw["query.a3m"])
    hh_a2m = tmp / "query_hh.a2m"
    run("perl", str(reformat_pl), "a3m", "a2m", str(src), str(hh_a2m))
    hh_rows = fasta_records(hh_a2m.read_text())
    with MSAFile(str(src), format="a2m") as mf:
        easel = next(mf)
    easel_rows = list(zip(easel.names, easel.alignment))
    if len(hh_rows) != len(easel_rows):
        die("hh-suite and Easel disagree on record count for query.a3m")
    for (hn, hs), (en, es) in zip(hh_rows, easel_rows):
        if hn.split()[0] != en or hs != es:
            die(f"hh-suite and Easel dotted A2M differ for {en}")
    (HERE / "query_dotted.a2m").write_text(write_fasta([(h, s) for h, s in hh_rows]))

    # ---- 3. A3M -> Stockholm -> A3M round trip via hh-suite -----------------
    hh_sto = tmp / "query_hh.sto"
    run("perl", str(reformat_pl), "a3m", "sto", str(src), str(hh_sto))
    rt = tmp / "query_rt.a3m"
    run("perl", str(reformat_pl), "sto", "a3m", str(hh_sto), str(rt))
    a, b = fasta_records(raw["query.a3m"]), fasta_records(rt.read_text())
    if [x[1] for x in a] != [y[1] for y in b]:
        die("a3m -> sto -> a3m did not round-trip sequences")
    if [x[0].split()[0] for x in a] != [y[0].split()[0] for y in b]:
        die("a3m -> sto -> a3m did not round-trip names")
    (HERE / "query_hh.sto").write_text(hh_sto.read_text())

    # ---- 4. Easel canonical single-block Stockholm --------------------------
    for name in ("PF00001.sto", "RF00001.sto"):
        original = raw[name]
        canon = run("esl-reformat", "pfam", str(HERE / name)).decode()
        if sto_lines(original, "#=GS") != sto_lines(canon, "#=GS"):
            die(f"esl-reformat pfam changed #=GS lines of {name}")
        gf_in = [
            line
            for line in sto_lines(original, "#=GF")
            if line.split()[1] not in EASEL_NORMALISED_GF
        ]
        gf_out = [
            line
            for line in sto_lines(canon, "#=GF")
            if line.split()[1] not in EASEL_NORMALISED_GF
        ]
        if gf_in != gf_out:
            die(f"esl-reformat pfam changed non-normalised #=GF lines of {name}")
        (HERE / name.replace(".sto", "_pfam.sto")).write_text(canon)

    # ---- 5. hmmalign-generated #=GR PP / #=GC PP_cons fixture ---------------
    import pyhmmer
    from pyhmmer.easel import Alphabet

    abc = Alphabet.amino()
    with MSAFile(str(HERE / "PF00001.sto"), digital=True, alphabet=abc) as mf:
        dmsa = next(mf)
    dmsa.name = b"7tm_1"
    hmm, _, _ = pyhmmer.plan7.Builder(abc).build_msa(
        dmsa, pyhmmer.plan7.Background(abc)
    )
    aligned = pyhmmer.hmmer.hmmalign(hmm, list(dmsa.sequences), trim=False)
    with open(HERE / "PF00001_hmmalign.sto", "wb") as fh:
        aligned.write(fh, "stockholm")
    text = (HERE / "PF00001_hmmalign.sto").read_text()
    if not sto_lines(text, "#=GR") or "PP_cons" not in text:
        die("hmmalign fixture lacks #=GR PP / #=GC PP_cons")

    # ---- 6. multi-alignment file: two valid Stockholm files concatenated ---
    (HERE / "multi.sto").write_text(raw["PF00001.sto"] + raw["RF00001.sto"])

    # ---- 7. A3M with #A3M# header and comment lines -------------------------
    (HERE / "hdr.a3m").write_text(
        "#A3M#\n# generated by generate_fixtures.py\n" + raw["query.a3m"]
    )

    # ---- 8. hand-written edge cases -----------------------------------------
    body = raw["PF00001.sto"].rstrip()
    if not body.endswith("//"):
        die("PF00001.sto does not end with //")
    (HERE / "missing_terminator.sto").write_text(body[: -len("//")].rstrip() + "\n")
    (HERE / "wrong_header.sto").write_text(">seq1\nACGT\n")
    (HERE / "empty.sto").write_text("")
    (HERE / "empty.a3m").write_text("")
    (HERE / "single.a3m").write_text(">only description here\nMKVLaaGG-\n")
    (HERE / "single.sto").write_text(
        "# STOCKHOLM 1.0\n#=GF ID single\nonly ACDE.F-G\n//\n"
    )
    (HERE / "no_desc.a3m").write_text(">nodesc\nACDEFG\n>with desc\nACDEFG\n")

    # ---- 9. compressed variants ---------------------------------------------
    for name in ("PF00001.sto", "query.a3m", "RF00001.sto"):
        data = (HERE / name).read_bytes()
        with gzip.open(HERE / f"{name}.gz", "wb") as fh:
            fh.write(data)
        (HERE / f"{name}.bgz").write_bytes(run("bgzip", "-c", str(HERE / name)))

    # ---- 10. expected values from esl-alistat --------------------------------
    for name in (
        "PF00001.sto",
        "RF00001.sto",
        "PF00001_pfam.sto",
        "RF00001_pfam.sto",
        "PF00001_hmmalign.sto",
        "multi.sto",
        "query_hh.sto",
    ):
        expected["stockholm"][name] = alistat(HERE / name)  # type: ignore[index]
    expected["stockholm"]["single.sto"] = alistat(HERE / "single.sto", alphabet="amino")  # type: ignore[index]
    for name in ("query.a3m", "query_dotted.a2m"):
        expected["a3m"][name] = alistat(HERE / name, informat="a2m")  # type: ignore[index]
    # Easel rejects the ``#A3M#`` header line ("at or near line 1"), so hdr.a3m has
    # no Easel oracle; Biopython's ``fasta-blast`` parser is its reference and the
    # record content is identical to query.a3m.
    expected["a3m"]["hdr.a3m"] = {"same_records_as": "query.a3m"}  # type: ignore[index]
    # Easel rejects ss_conf digits, so the pseudo-sequences are stripped first.
    stripped = tmp / "test_head_nopseudo.a3m"
    stripped.write_text(write_fasta(head[3:]))
    expected["a3m"]["test_head.a3m"] = {  # type: ignore[index]
        "with_pseudo_records": len(head),
        "pseudo_names": [h.split()[0] for h, _ in head[:3]],
        "without_pseudo": alistat(stripped, informat="a2m"),
    }
    (HERE / "expected.json").write_text(json.dumps(expected, indent=2) + "\n")

    # ---- 11. README ----------------------------------------------------------
    (HERE / "README.md").write_text(
        f"""# MSA fixtures (A2M / A3M / Stockholm)

Generated on {today} by `generate_fixtures.py` in this directory. Do not edit
by hand; re-run the generator (see its docstring for the required tools).

## Sources

| File | Source |
|---|---|
| `PF00001.sto` | Pfam seed, InterPro API `{SOURCES['PF00001.sto'][0]}` (gunzipped) |
| `RF00001.sto` | Rfam seed, `{SOURCES['RF00001.sto'][0]}` (interleaved, two blocks) |
| `query.a3m` | hh-suite `data/query.a3m` |
| `test_head.a3m` | hh-suite `scripts/hhpred/example/test.a3m`: the three `ss_*` pseudo-sequences, the query and the first {TEST_HEAD_HOMOLOGS} homologs |

## Derived (oracle-generated)

| File | Produced by | Cross-check |
|---|---|---|
| `query_dotted.a2m` | hh-suite `reformat.pl a3m a2m` | byte-identical to Easel's A2M reader reconstruction |
| `query_hh.sto` | hh-suite `reformat.pl a3m sto` | `sto -> a3m` round-trips names and sequences |
| `PF00001_pfam.sto`, `RF00001_pfam.sto` | `esl-reformat pfam` (canonical single-block) | `#=GS` and all `#=GF` except `GA`/`TC`/`NC` line-identical to the source |
| `PF00001_hmmalign.sto` | `pyhmmer.hmmer.hmmalign` on an HMM built from the seed | carries `#=GR PP`, `#=GC PP_cons`/`RF`, 200-column blocks |
| `multi.sto` | `PF00001.sto` + `RF00001.sto` concatenated | `esl-alistat` reports two alignments |
| `hdr.a3m` | `query.a3m` with `#A3M#` and a `#` comment line prepended | |
| `expected.json` | `esl-alistat` per-alignment `n_sequences` / `alignment_length` | |

## Hand-written edge cases

`missing_terminator.sto`, `wrong_header.sto`, `empty.sto`, `empty.a3m`,
`single.a3m`, `single.sto`, `no_desc.a3m`, plus `.gz` / `.bgz` variants of
`PF00001.sto`, `RF00001.sto` and `query.a3m`.
"""
    )
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"fixtures written to {HERE}")


if __name__ == "__main__":
    main()
