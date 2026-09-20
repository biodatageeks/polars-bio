# MSA fixtures (A2M / A3M / Stockholm)

Generated on 2026-09-08 by `generate_fixtures.py` in this directory. Do not edit
by hand; re-run the generator (see its docstring for the required tools).

## Sources

| File | Source |
|---|---|
| `PF00001.sto` | Pfam seed, InterPro API `https://www.ebi.ac.uk/interpro/wwwapi/entry/pfam/PF00001/?annotation=alignment:seed` (gunzipped) |
| `RF00001.sto` | Rfam seed, `https://rfam.org/family/RF00001/alignment/stockholm` (interleaved, two blocks) |
| `query.a3m` | hh-suite `data/query.a3m` |
| `test_head.a3m` | hh-suite `scripts/hhpred/example/test.a3m`: the three `ss_*` pseudo-sequences, the query and the first 200 homologs |

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
