# cy-convert

This program is used to convert the weight system files and polytope info files obtained for the paper [All Weight Systems for Calabi-Yau Fourfolds from Reflexive Polyhedra](https://arxiv.org/abs/1808.02422) into Parquet format.

The data will be made available at https://huggingface.co/cy-data.

```
Usage: cy-convert [OPTIONS]

Options:
      --ws-in <FILE>
      --polytope-info-in <FILE>
      --parquet-in <FILE>
      --ws-out <FILE>
      --polytope-info-out <FILE>
      --parquet-non-ip-out <FILE>
      --parquet-non-reflexive-out <FILE>
      --parquet-reflexive-out <FILE>
  -i, --include-derived-quantities
      --limit <LIMIT>
  -h, --help                              Print help
  -V, --version                           Print version
```
