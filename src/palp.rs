use std::cmp::{max, min};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use regex::Regex;

use crate::parquet_utils::{
    build_parquet_int_field, build_parquet_int_list_of_lists_field, write_parquet_int_column,
    write_repeated_parquet_int_column, ROW_GROUP_SIZE,
};
use crate::PalpArgs;

#[derive(Default)]
struct PolytopeInfo {
    dimension: usize,
    coordinate_list: Vec<i32>,
    vertex_count_list: Vec<i32>,
    facet_count_list: Vec<i32>,
    point_count_list: Vec<i32>,
    dual_point_count_list: Vec<i32>,
    euler_characteristic_list: Vec<i32>,
    hodge_number_lists: Vec<Vec<i32>>,
}

impl PolytopeInfo {
    fn resize(&mut self, dimension: usize) {
        self.dimension = dimension;
        self.hodge_number_lists.resize(dimension - 2, Vec::new());
    }
}

struct PalpHeader {
    rows: usize,
    columns: usize,
    point_count: i32,
    dual_point_count: i32,
    vertex_count: i32,
    facet_count: i32,
    hodge_numbers: Vec<i32>,
    euler_characteristic: i32,
}

fn parse_header(input: &str) -> Result<PalpHeader> {
    use once_cell::sync::Lazy;

    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(
            r"(?x)
            ^\s* (?<rows> [0-9]+)
            \s+ (?<columns> [0-9]+)
            \s+ M: (?<point_count> [0-9]+)
            \s+ (?<vertex_count> [0-9]+)
            \s+ N: (?<dual_point_count> [0-9]+)
            \s+ (?<facet_count> [0-9]+)
            \s+ H:(?<hodge_numbers> [0-9]+ (, [0-9]+)*)
            \s+ \[ (?<euler_characteristic> -?[0-9]+) \]",
        )
        .unwrap()
    });

    let c = RE
        .captures(input)
        .context(format!("invalid header: {}", input))?;

    let hodge_numbers: Result<Vec<_>, _> =
        c["hodge_numbers"].split(",").map(|x| x.parse()).collect();

    Ok(PalpHeader {
        rows: c["rows"].parse()?,
        columns: c["columns"].parse()?,
        point_count: c["point_count"].parse()?,
        vertex_count: c["vertex_count"].parse()?,
        dual_point_count: c["dual_point_count"].parse()?,
        facet_count: c["facet_count"].parse()?,
        euler_characteristic: c["euler_characteristic"].parse()?,
        hodge_numbers: hodge_numbers?,
    })
}

fn parse_coordinates(header: &PalpHeader, lines: &mut std::str::Lines) -> Result<Vec<Vec<i32>>> {
    let mut ret = Vec::with_capacity(header.rows);

    for _ in 0..header.rows {
        let line = lines.next().context("incomplete input")?;
        let values: Result<Vec<i32>, _> = line
            .split(" ")
            .filter(|x| !x.is_empty())
            .map(|x| x.parse())
            .collect();
        let values = values?;

        if values.len() != header.columns {
            bail!("invalid coordinate count");
        }

        ret.push(values);
    }

    Ok(ret)
}

fn parse(input: &str) -> Result<PolytopeInfo> {
    let mut ret = PolytopeInfo::default();

    let mut lines = input.lines();

    while let Some(line) = lines.next() {
        match line.chars().find(|c| !c.is_whitespace()) {
            Some(c) if c.is_numeric() => {}
            _ => continue,
        };

        let header = parse_header(line)?;
        let coordinates = parse_coordinates(&header, &mut lines)?;
        let dimension = min(header.rows, header.columns);
        let vertex_count = max(header.rows, header.columns);

        if ret.dimension == 0 {
            ret.resize(dimension);
        } else {
            if ret.dimension != dimension {
                bail!("varing dimension");
            }
        }

        ret.vertex_count_list.push(header.vertex_count);
        ret.facet_count_list.push(header.facet_count);
        ret.point_count_list.push(header.point_count);
        ret.dual_point_count_list.push(header.dual_point_count);
        ret.euler_characteristic_list
            .push(header.euler_characteristic);

        for (i, h) in header.hodge_numbers.into_iter().enumerate() {
            ret.hodge_number_lists[i].push(h);
        }

        if header.rows < header.columns {
            for i in 0..vertex_count {
                for j in 0..dimension {
                    ret.coordinate_list.push(coordinates[j][i]);
                }
            }
        } else {
            unimplemented!("vertices can only be read when specified column-wise");
        };

        if header.vertex_count as usize != vertex_count {
            bail!("invalid vertex count");
        }
    }

    if ret.dimension == 0 {
        bail!("no polytopes read");
    }
    Ok(ret)
}

fn write_parquet<P: AsRef<Path>>(info: PolytopeInfo, path: P) -> Result<()> {
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type as SchemaType;

    let writer_props = Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(5)?))
            .build(),
    );

    let vertices_field = build_parquet_int_list_of_lists_field("vertices")?;

    let mut hodge_number_fields = Vec::new();
    for i in 0..info.dimension - 2 {
        hodge_number_fields.push(build_parquet_int_field(&format!("h1{}", i + 1))?);
    }

    let vertex_count_field = build_parquet_int_field("vertex_count")?;
    let facet_count_field = build_parquet_int_field("facet_count")?;
    let point_count_field = build_parquet_int_field("point_count")?;
    let dual_point_count_field = build_parquet_int_field("dual_point_count")?;
    let euler_characteristic_field = build_parquet_int_field("euler_characteristic")?;

    let mut fields = vec![
        vertices_field,
        vertex_count_field,
        facet_count_field,
        point_count_field,
        dual_point_count_field,
    ];
    fields.append(&mut hodge_number_fields.clone());
    fields.push(euler_characteristic_field.clone());

    let schema = SchemaType::group_type_builder("schema")
        .with_fields(fields)
        .build()?;

    let file = fs::File::create(path)?;

    let row_count = info.vertex_count_list.len();
    let row_group_count = (row_count + ROW_GROUP_SIZE - 1) / ROW_GROUP_SIZE;

    let mut writer = SerializedFileWriter::new(file, Arc::new(schema), writer_props.clone())?;

    let mut coordinate_end = 0;

    for g in 0..row_group_count {
        let start = g * ROW_GROUP_SIZE;
        let end = min(start + ROW_GROUP_SIZE, row_count);

        let mut row_group_writer = writer.next_row_group()?;

        let coordinate_start = coordinate_end;
        let mut coordinate_repetition_levels = Vec::new();
        for &count in &info.vertex_count_list[start..end] {
            coordinate_end += count as usize * info.dimension;
            for v in 0..count {
                for i in 0..info.dimension {
                    let value = if v == 0 && i == 0 {
                        0
                    } else if i == 0 {
                        1
                    } else {
                        2
                    };
                    coordinate_repetition_levels.push(value);
                }
            }
        }
        let coordinate_definition_levels = vec![2; coordinate_end - coordinate_start];

        let count = write_repeated_parquet_int_column(
            &mut row_group_writer,
            &info.coordinate_list[coordinate_start..coordinate_end],
            &coordinate_definition_levels,
            &coordinate_repetition_levels,
        )?;
        assert_eq!(count, coordinate_end - coordinate_start);

        write_parquet_int_column(&mut row_group_writer, &info.vertex_count_list[start..end])?;
        write_parquet_int_column(&mut row_group_writer, &info.facet_count_list[start..end])?;
        write_parquet_int_column(&mut row_group_writer, &info.point_count_list[start..end])?;
        write_parquet_int_column(
            &mut row_group_writer,
            &info.dual_point_count_list[start..end],
        )?;

        for h in &info.hodge_number_lists {
            write_parquet_int_column(&mut row_group_writer, &h[start..end])?;
        }

        write_parquet_int_column(
            &mut row_group_writer,
            &info.euler_characteristic_list[start..end],
        )?;

        row_group_writer.close()?;
    }

    writer.close()?;

    Ok(())
}

pub fn run(args: PalpArgs) -> Result<()> {
    let input = std::fs::read_to_string(args.palp_in)?;
    let polytope_info = parse(&input)?;

    write_parquet(polytope_info, args.parquet_out)?;

    Ok(())
}
