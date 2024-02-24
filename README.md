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

It is also used to convert the polyhedron data on reflexive polyhedra in four dimensions [Complete classification of reflexive polyhedra in four dimensions](https://arxiv.org/abs/hep-th/0002240).

The data is available at: https://huggingface.co/datasets/calabi-yau-data/polytopes-4d

```
Usage: cy-convert palp [OPTIONS]

Options:
      --palp-in <FILE>
      --palp-out <FILE>
      --parquet-in <FILE>
      --parquet-out <FILE>
  -h, --help                Print help
```

## Parquet tools

Parquet files can be inspected using the tools from the [parquet crate](https://crates.io/crates/parquet).

```
cargo install parquet --features=cli
```

## Lessons

- The Rust parquet library is very low level and requires quite detailed knowledge of the
  Parquet format. It might be easier to use Arrow on top of Parquet to read and write
  data.
- The Hugging Face dataset viewer does not support large row groups. While five million
  records per row group was okay for the 4d weight system dataset, this is too much for
  the 4d polytope dataset, presumably because each record contains more data.
