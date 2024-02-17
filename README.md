# cy-convert

This program is used to convert the weight system files and polytope info files obtained for the paper [All Weight Systems for Calabi-Yau Fourfolds from Reflexive Polyhedra](https://arxiv.org/abs/1808.02422) into Parquet format.

The data is available at: https://huggingface.co/datasets/calabi-yau-data/ws-5d

```
Usage: cy-convert ipws [OPTIONS]

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
```

It is also used to convert the polyhedron data on reflexive polyhedra in four dimensions [Complete classification of reflexive ]polyhedra in four dimensions(https://arxiv.org/abs/hep-th/0002240).

```
Usage: cy-convert palp --palp-in <FILE> --parquet-out <FILE>

Options:
      --palp-in <FILE>
      --parquet-out <FILE>
  -h, --help                Print help
```

# Parquet tools

Parquet files can be inspected using the tools from the [parquet crate](https://crates.io/crates/parquet).

```
cargo install parquet --features=cli
```
