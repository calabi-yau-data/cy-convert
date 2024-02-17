use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut};
use clap::Parser;
use parquet::file::metadata::KeyValue;
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::schema::types::Type as ParquetType;
use regex::Regex;
use std::cmp::{min, Ordering};
use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

const ROW_GROUP_SIZE: usize = 5_000_000;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(long, value_name = "FILE")]
    ws_in: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    polytope_info_in: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    parquet_in: Vec<PathBuf>,

    #[arg(long, value_name = "FILE")]
    ws_out: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    polytope_info_out: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    parquet_non_ip_out: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    parquet_non_reflexive_out: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    parquet_reflexive_out: Option<PathBuf>,

    #[arg(short, long)]
    include_derived_quantities: bool,

    #[arg(long)]
    limit: Option<usize>,
}

#[derive(Default)]
struct NonIpPolytopeInfo {
    dimension: usize,
    weight_lists: Vec<Vec<i32>>,
}

#[derive(Default)]
struct NonReflexivePolytopeInfo {
    dimension: usize,
    weight_lists: Vec<Vec<i32>>,
    vertex_count_list: Vec<i32>,
    facet_count_list: Vec<i32>,
    point_count_list: Vec<i32>,
}

#[derive(Default)]
struct ReflexivePolytopeInfo {
    dimension: usize,
    weight_lists: Vec<Vec<i32>>,
    vertex_count_list: Vec<i32>,
    facet_count_list: Vec<i32>,
    point_count_list: Vec<i32>,
    dual_point_count_list: Vec<i32>,
    hodge_number_lists: Vec<Vec<i32>>,
    euler_characteristic_list: Vec<i32>,
}

impl NonIpPolytopeInfo {
    fn new(dimension: usize) -> NonIpPolytopeInfo {
        let mut ret = NonIpPolytopeInfo::default();
        ret.resize(dimension);
        ret
    }

    fn resize(&mut self, dimension: usize) {
        self.dimension = dimension;
        self.weight_lists.resize(dimension, Vec::new());
    }
}

impl NonReflexivePolytopeInfo {
    fn new(dimension: usize) -> NonReflexivePolytopeInfo {
        let mut ret = NonReflexivePolytopeInfo::default();
        ret.resize(dimension);
        ret
    }

    fn resize(&mut self, dimension: usize) {
        self.dimension = dimension;
        self.weight_lists.resize(dimension, Vec::new());
    }
}

impl ReflexivePolytopeInfo {
    fn new(dimension: usize, include_derived_quantities: bool) -> ReflexivePolytopeInfo {
        let mut ret = ReflexivePolytopeInfo::default();
        ret.resize(dimension, include_derived_quantities);
        ret
    }

    fn resize(&mut self, dimension: usize, include_derived_quantities: bool) {
        // one more for six dimensions to store h22 in addition to h11, h12, and h13
        let hodge_number_lists_count = dimension - 3
            + if dimension == 6 && include_derived_quantities {
                1
            } else {
                0
            };

        self.dimension = dimension;
        self.weight_lists.resize(dimension, Vec::new());
        self.hodge_number_lists
            .resize(hodge_number_lists_count, Vec::new());
    }
}

fn euler_characteristic(h11: i32, h12: i32, h13: i32) -> i32 {
    return 48 + 6 * (h11 - h12 + h13);
}

fn hodge_number_h22(h11: i32, h12: i32, h13: i32) -> i32 {
    return 44 + 4 * h11 + 4 * h13 - 2 * h12;
}

fn read_varint<T: Buf>(data: &mut T) -> u32 {
    let mut ret = 0;
    let mut pos = 0;

    loop {
        let v = data.get_u8();
        let w = u32::from(v) & 127;

        ret |= w << pos;

        if ret.checked_shr(pos) != Some(w) {
            panic!("varint read error");
        }

        if (v & 128) == 0 {
            break;
        }

        pos += 7;
    }

    return ret;
}

fn write_varint<T: BufMut>(data: &mut T, mut value: u32) {
    while value > 127 {
        data.put_u8(value as u8 | 128);
        value >>= 7;
    }

    data.put_u8(value as u8);
}

fn read_weights<P: AsRef<Path>>(path: P, limit: usize) -> Result<(usize, String, Vec<i32>)> {
    let data = fs::read(path)?;
    let mut buf = Cursor::new(data);

    let dimension = buf.get_u32() as usize;
    let numerator = buf.get_u32();
    let denominator = buf.get_u32();
    let ws_count = min(buf.get_u64() as usize, limit);

    let index = if denominator == 1 {
        format!("{}", numerator)
    } else {
        format!("{}/{}", numerator, denominator)
    };

    println!("dimension: {}", dimension);
    println!("index: {}", index);
    println!("weight system count: {}", ws_count);

    let mut weights = Vec::with_capacity(ws_count * dimension);

    for _ in 0..ws_count * dimension {
        let w = read_varint(&mut buf);
        weights.push(w.try_into()?);
    }

    Ok((dimension, index, weights))
}

fn compare_weight_systems(a: &[i32], b: &[i32]) -> Ordering {
    let a_empty = a.is_empty();
    let b_empty = b.is_empty();

    match a_empty.cmp(&b_empty) {
        Ordering::Equal => (),
        x => return x,
    };

    a.cmp(b)
}

fn collect_weights_into(dest: &mut Vec<i32>, weight_lists: &[Vec<i32>], pos: usize) {
    dest.extend(weight_lists.iter().map(|wl| wl[pos]));
}

fn write_weights<P: AsRef<Path>>(
    dimension: usize,
    numerator: i32,
    denominator: i32,
    ws_path: Option<P>,
    polytope_info_path: Option<P>,
    non_ip: &NonIpPolytopeInfo,
    non_reflexive: &NonReflexivePolytopeInfo,
    reflexive: &ReflexivePolytopeInfo,
) -> Result<()> {
    let mut non_ip_pos = 0;
    let mut non_reflexive_pos = 0;
    let mut reflexive_pos = 0;

    let non_ip_wl = &non_ip.weight_lists;
    let non_reflexive_wl = &non_reflexive.weight_lists;
    let reflexive_wl = &reflexive.weight_lists;

    let non_ip_count = non_ip_wl[0].len();
    let non_reflexive_count = non_reflexive_wl[0].len();
    let reflexive_count = reflexive_wl[0].len();

    let mut non_ip_weights = Vec::with_capacity(dimension);
    let mut non_reflexive_weights = Vec::with_capacity(dimension);
    let mut reflexive_weights = Vec::with_capacity(dimension);

    println!("dimension: {}", dimension);
    println!("index: {}/{}", numerator, denominator);
    println!("non-IP weight system count: {}", non_ip_count);
    println!("non-reflexive weight system count: {}", non_reflexive_count);
    println!("reflexive weight system count: {}", reflexive_count);

    let mut ws_buf = Vec::new();
    let mut pi_buf = Vec::new();

    ws_buf.put_u32(dimension as u32);
    ws_buf.put_u32(numerator as u32);
    ws_buf.put_u32(denominator as u32);
    ws_buf.put_u64((non_ip_count + non_reflexive_count + reflexive_count) as u64);

    if non_ip_count > 0 {
        collect_weights_into(&mut non_ip_weights, non_ip_wl, 0);
    }

    if non_reflexive_count > 0 {
        collect_weights_into(&mut non_reflexive_weights, non_reflexive_wl, 0);
    }

    if reflexive_count > 0 {
        collect_weights_into(&mut reflexive_weights, reflexive_wl, 0);
    }

    while !non_ip_weights.is_empty()
        || !non_reflexive_weights.is_empty()
        || !reflexive_weights.is_empty()
    {
        while !non_ip_weights.is_empty()
            && compare_weight_systems(&non_ip_weights, &non_reflexive_weights).is_lt()
            && compare_weight_systems(&non_ip_weights, &reflexive_weights).is_lt()
        {
            for &w in &non_ip_weights {
                write_varint(&mut ws_buf, w as u32);
            }

            pi_buf.put_u8(0); // not IP

            non_ip_pos += 1;
            non_ip_weights.clear();
            if non_ip_pos < non_ip_count {
                collect_weights_into(&mut non_ip_weights, non_ip_wl, non_ip_pos);
            }
        }

        while !non_reflexive_weights.is_empty()
            && compare_weight_systems(&non_reflexive_weights, &non_ip_weights).is_lt()
            && compare_weight_systems(&non_reflexive_weights, &reflexive_weights).is_lt()
        {
            for &w in &non_reflexive_weights {
                write_varint(&mut ws_buf, w as u32);
            }

            pi_buf.put_u8(1); // not reflexive
            write_varint(
                &mut pi_buf,
                non_reflexive.vertex_count_list[non_reflexive_pos] as u32,
            );
            write_varint(
                &mut pi_buf,
                non_reflexive.facet_count_list[non_reflexive_pos] as u32,
            );
            write_varint(
                &mut pi_buf,
                non_reflexive.point_count_list[non_reflexive_pos] as u32,
            );

            non_reflexive_pos += 1;
            non_reflexive_weights.clear();
            if non_reflexive_pos < non_reflexive_count {
                collect_weights_into(
                    &mut non_reflexive_weights,
                    non_reflexive_wl,
                    non_reflexive_pos,
                );
            }
        }

        while !reflexive_weights.is_empty()
            && compare_weight_systems(&reflexive_weights, &non_ip_weights).is_lt()
            && compare_weight_systems(&reflexive_weights, &non_reflexive_weights).is_lt()
        {
            for &w in &reflexive_weights {
                write_varint(&mut ws_buf, w as u32);
            }

            pi_buf.put_u8(2); // reflexive
            write_varint(
                &mut pi_buf,
                reflexive.vertex_count_list[reflexive_pos] as u32,
            );
            write_varint(
                &mut pi_buf,
                reflexive.facet_count_list[reflexive_pos] as u32,
            );
            write_varint(
                &mut pi_buf,
                reflexive.point_count_list[reflexive_pos] as u32,
            );
            write_varint(
                &mut pi_buf,
                reflexive.dual_point_count_list[reflexive_pos] as u32,
            );
            for i in 0..dimension - 3 {
                write_varint(
                    &mut pi_buf,
                    reflexive.hodge_number_lists[i][reflexive_pos] as u32,
                );
            }

            reflexive_pos += 1;
            reflexive_weights.clear();
            if reflexive_pos < reflexive_count {
                collect_weights_into(&mut reflexive_weights, reflexive_wl, reflexive_pos);
            }
        }
    }

    if let Some(path) = ws_path {
        fs::write(path, &ws_buf)?;
    }

    if let Some(path) = polytope_info_path {
        fs::write(path, &pi_buf)?;
    }

    Ok(())
}

fn append_weight_system(weight_lists: &mut Vec<Vec<i32>>, weight_system: &[i32]) {
    for (i, &w) in weight_system.iter().enumerate() {
        weight_lists[i].push(w);
    }
}

fn read_polytope_info<P: AsRef<Path>>(
    dimension: usize,
    weights: &[i32],
    calculate_derived_quantities: bool,
    path: P,
) -> Result<(
    NonIpPolytopeInfo,
    NonReflexivePolytopeInfo,
    ReflexivePolytopeInfo,
)> {
    let derived6 = calculate_derived_quantities && dimension == 6;

    let data = fs::read(path)?;
    let mut cursor = Cursor::new(data);

    let mut non_ip = NonIpPolytopeInfo::new(dimension);
    let mut non_reflexive = NonReflexivePolytopeInfo::new(dimension);
    let mut reflexive = ReflexivePolytopeInfo::new(dimension, calculate_derived_quantities);

    for ws in weights.chunks(dimension) {
        let polytope_type = cursor.get_u8();

        /* not IP */
        if polytope_type == 0 {
            append_weight_system(&mut non_ip.weight_lists, ws);
            continue;
        }

        let vertex_count = read_varint(&mut cursor).try_into()?;
        let facet_count = read_varint(&mut cursor).try_into()?;
        let point_count = read_varint(&mut cursor).try_into()?;

        /* non reflexive */
        if polytope_type == 1 {
            append_weight_system(&mut non_reflexive.weight_lists, ws);
            non_reflexive.vertex_count_list.push(vertex_count);
            non_reflexive.facet_count_list.push(facet_count);
            non_reflexive.point_count_list.push(point_count);
            continue;
        }

        /* reflexive */
        if polytope_type == 2 {
            append_weight_system(&mut reflexive.weight_lists, ws);
            reflexive.vertex_count_list.push(vertex_count);
            reflexive.facet_count_list.push(facet_count);
            reflexive.point_count_list.push(point_count);

            reflexive
                .dual_point_count_list
                .push(read_varint(&mut cursor).try_into()?);

            for i in 0..dimension - 3 {
                let h = read_varint(&mut cursor);
                reflexive.hodge_number_lists[i].push(h.try_into()?);
            }

            if derived6 {
                let i = reflexive.hodge_number_lists[3].len();

                let h11 = reflexive.hodge_number_lists[0][i];
                let h12 = reflexive.hodge_number_lists[1][i];
                let h13 = reflexive.hodge_number_lists[2][i];

                reflexive.hodge_number_lists[3].push(hodge_number_h22(h11, h12, h13));
                reflexive
                    .euler_characteristic_list
                    .push(euler_characteristic(h11, h12, h13));
            }

            continue;
        }

        bail!("invalid polytope type");
    }

    Ok((non_ip, non_reflexive, reflexive))
}

fn build_parquet_int_field(name: &str) -> Result<Arc<ParquetType>> {
    use parquet::basic::{Repetition, Type as PhysicalType};

    Ok(Arc::new(
        ParquetType::primitive_type_builder(name, PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .build()?,
    ))
}

fn append_metadata<W: Write + Send>(
    writer: &mut SerializedFileWriter<W>,
    ip: bool,
    reflexive: bool,
    dimension: usize,
    index: &str,
) {
    writer.append_key_value_metadata(KeyValue::new("ip".to_owned(), ip.to_string()));
    writer.append_key_value_metadata(KeyValue::new("reflexive".to_owned(), reflexive.to_string()));
    writer.append_key_value_metadata(KeyValue::new("dimension".to_owned(), dimension.to_string()));
    writer.append_key_value_metadata(KeyValue::new("index".to_owned(), index.to_owned()));
}

fn write_parquet_int_column<W: Write + Send>(
    row_group_writer: &mut SerializedRowGroupWriter<W>,
    data: &[i32],
) -> Result<()> {
    use parquet::data_type::Int32Type;

    let mut col_writer = row_group_writer.next_column()?.expect("column");

    col_writer
        .typed::<Int32Type>()
        .write_batch(data, None, None)?;
    col_writer.close()?;

    Ok(())
}

fn write_parquet<P: AsRef<Path>>(
    dimension: usize,
    index: &str,
    write_derived_quantities: bool,
    non_ip: NonIpPolytopeInfo,
    non_reflexive: NonReflexivePolytopeInfo,
    reflexive: ReflexivePolytopeInfo,
    non_ip_path: Option<P>,
    non_reflexive_path: Option<P>,
    reflexive_path: Option<P>,
) -> Result<()> {
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::schema::types::Type;

    let writer_props = Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(5)?))
            .build(),
    );

    let mut weight_fields = Vec::new();
    for i in 0..dimension {
        weight_fields.push(build_parquet_int_field(&format!("weight{}", i))?);
    }

    let mut hodge_number_fields = Vec::new();
    for i in 0..dimension - 3 {
        hodge_number_fields.push(build_parquet_int_field(&format!("h1{}", i + 1))?);
    }

    if write_derived_quantities && dimension == 6 {
        hodge_number_fields.push(build_parquet_int_field("h22")?);
    }

    let vertex_count_field = build_parquet_int_field("vertex_count")?;
    let facet_count_field = build_parquet_int_field("facet_count")?;
    let point_count_field = build_parquet_int_field("point_count")?;
    let dual_point_count_field = build_parquet_int_field("dual_point_count")?;
    let euler_characteristic_field = build_parquet_int_field("euler_characteristic")?;

    if let Some(non_ip_path) = non_ip_path {
        let non_ip_schema = Type::group_type_builder("schema")
            .with_fields(weight_fields.clone())
            .build()?;

        let file = fs::File::create(non_ip_path)?;

        let row_count = non_ip.weight_lists[0].len();
        let row_group_count = (row_count + ROW_GROUP_SIZE - 1) / ROW_GROUP_SIZE;

        let mut writer =
            SerializedFileWriter::new(file, Arc::new(non_ip_schema), writer_props.clone())?;

        append_metadata(&mut writer, false, false, dimension, index);

        for g in 0..row_group_count {
            let start = g * ROW_GROUP_SIZE;
            let end = min(start + ROW_GROUP_SIZE, row_count);
            println!("{} {}", start, end);

            let mut row_group_writer = writer.next_row_group()?;

            for weights in &non_ip.weight_lists {
                write_parquet_int_column(&mut row_group_writer, &weights[start..end])?;
            }

            row_group_writer.close()?;
        }

        writer.close()?;
    }

    if let Some(non_reflexive_path) = non_reflexive_path {
        let mut non_reflexive_fields = weight_fields.clone();
        non_reflexive_fields.push(vertex_count_field.clone());
        non_reflexive_fields.push(facet_count_field.clone());
        non_reflexive_fields.push(point_count_field.clone());

        let non_reflexive_schema = Type::group_type_builder("schema")
            .with_fields(non_reflexive_fields)
            .build()?;

        let file = fs::File::create(non_reflexive_path)?;

        let row_count = non_reflexive.weight_lists[0].len();
        let row_group_count = (row_count + ROW_GROUP_SIZE - 1) / ROW_GROUP_SIZE;

        let mut writer =
            SerializedFileWriter::new(file, Arc::new(non_reflexive_schema), writer_props.clone())?;

        append_metadata(&mut writer, true, false, dimension, index);

        for g in 0..row_group_count {
            let start = g * ROW_GROUP_SIZE;
            let end = min(start + ROW_GROUP_SIZE, row_count);
            println!("{} {}", start, end);

            let mut row_group_writer = writer.next_row_group()?;

            for weights in &non_reflexive.weight_lists {
                write_parquet_int_column(&mut row_group_writer, &weights[start..end])?;
            }

            write_parquet_int_column(
                &mut row_group_writer,
                &non_reflexive.vertex_count_list[start..end],
            )?;
            write_parquet_int_column(
                &mut row_group_writer,
                &non_reflexive.facet_count_list[start..end],
            )?;
            write_parquet_int_column(
                &mut row_group_writer,
                &non_reflexive.point_count_list[start..end],
            )?;

            row_group_writer.close()?;
        }

        writer.close()?;
    }

    if let Some(reflexive_path) = reflexive_path {
        let mut reflexive_fields = weight_fields.clone();
        reflexive_fields.push(vertex_count_field.clone());
        reflexive_fields.push(facet_count_field.clone());
        reflexive_fields.push(point_count_field.clone());
        reflexive_fields.push(dual_point_count_field.clone());
        reflexive_fields.append(&mut hodge_number_fields.clone());
        if write_derived_quantities && dimension == 6 {
            reflexive_fields.push(euler_characteristic_field.clone());
        }

        let reflexive_schema = Type::group_type_builder("schema")
            .with_fields(reflexive_fields)
            .build()?;

        let file = fs::File::create(reflexive_path)?;

        let row_count = reflexive.weight_lists[0].len();
        let row_group_count = (row_count + ROW_GROUP_SIZE - 1) / ROW_GROUP_SIZE;

        let mut writer =
            SerializedFileWriter::new(file, Arc::new(reflexive_schema), writer_props.clone())?;

        append_metadata(&mut writer, true, true, dimension, index);

        for g in 0..row_group_count {
            let start = g * ROW_GROUP_SIZE;
            let end = min(start + ROW_GROUP_SIZE, row_count);
            println!("{} {}", start, end);

            let mut row_group_writer = writer.next_row_group()?;

            for weights in &reflexive.weight_lists {
                write_parquet_int_column(&mut row_group_writer, &weights[start..end])?;
            }

            write_parquet_int_column(
                &mut row_group_writer,
                &reflexive.vertex_count_list[start..end],
            )?;
            write_parquet_int_column(
                &mut row_group_writer,
                &reflexive.facet_count_list[start..end],
            )?;
            write_parquet_int_column(
                &mut row_group_writer,
                &reflexive.point_count_list[start..end],
            )?;
            write_parquet_int_column(
                &mut row_group_writer,
                &reflexive.dual_point_count_list[start..end],
            )?;

            for h in &reflexive.hodge_number_lists {
                write_parquet_int_column(&mut row_group_writer, &h[start..end])?;
            }

            if write_derived_quantities && dimension == 6 {
                write_parquet_int_column(
                    &mut row_group_writer,
                    &reflexive.euler_characteristic_list[start..end],
                )?;
            }

            row_group_writer.close()?;
        }

        writer.close()?;
    }

    Ok(())
}

fn parse_parquet_metadata(metadata: &[KeyValue]) -> Result<(bool, bool, usize, i32, i32)> {
    let mut ip: Option<bool> = None;
    let mut reflexive: Option<bool> = None;
    let mut dimension: Option<usize> = None;
    let mut index: Option<String> = None;

    for kv in metadata {
        if let Some(value) = &kv.value {
            match kv.key.as_str() {
                "ip" => ip = FromStr::from_str(value).ok(),
                "reflexive" => reflexive = FromStr::from_str(value).ok(),
                "dimension" => dimension = FromStr::from_str(value).ok(),
                "index" => index = FromStr::from_str(value).ok(),
                _ => {}
            };
        }
    }

    let (Some(ip), Some(reflexive), Some(dimension), Some(index)) =
        (ip, reflexive, dimension, index)
    else {
        bail!("missing Parquet file metadata");
    };

    let re = Regex::new(r"^([0-9]+)/([0-9]+)$").unwrap();

    let (numerator, denominator): (i32, i32) = if let Some(c) = re.captures(&index) {
        (
            FromStr::from_str(c.get(1).expect("capture").as_str()).context("parse index")?,
            FromStr::from_str(c.get(2).expect("capture").as_str()).context("parse index")?,
        )
    } else {
        (FromStr::from_str(&index)?, 1)
    };

    Ok((ip, reflexive, dimension, numerator, denominator))
}

fn read_parquet<P: AsRef<Path>>(
    path: P,
    non_ip: &mut NonIpPolytopeInfo,
    non_reflexive: &mut NonReflexivePolytopeInfo,
    reflexive: &mut ReflexivePolytopeInfo,
    limit: usize,
) -> Result<(usize, i32, i32)> {
    use parquet::column::reader::ColumnReader;
    use parquet::file::reader::FileReader as _;
    use parquet::file::serialized_reader::SerializedFileReader;

    let file = fs::File::open(&path)?;
    let reader = SerializedFileReader::new(file)?;

    let metadata = reader.metadata();
    let kv_metadata = metadata
        .file_metadata()
        .key_value_metadata()
        .context("no Parquet file metadata")?;

    let (is_ip, is_reflexive, dimension, numerator, denominator) =
        parse_parquet_metadata(&kv_metadata)?;

    let num_columns = match (is_ip, is_reflexive) {
        (false, false) => dimension,
        (true, false) => dimension + 3,
        (true, true) => 2 * dimension + 1,
        _ => bail!("invalid metadata"),
    };

    non_ip.resize(dimension);
    non_reflexive.resize(dimension);
    reflexive.resize(dimension, false);

    let row_count = min(metadata.file_metadata().num_rows() as usize, limit);

    let mut values = vec![vec![0; row_count]; num_columns];
    let mut pos = 0;

    for g in 0..metadata.num_row_groups() {
        let row_group_reader = reader.get_row_group(g)?;
        let row_group_metadata = metadata.row_group(g);

        if num_columns > row_group_metadata.num_columns() {
            bail!("columns missing");
        }

        let to_read = min(row_group_metadata.num_rows() as usize, row_count - pos);

        for c in 0..num_columns {
            let mut column_reader = row_group_reader.get_column_reader(c)?;

            match column_reader {
                ColumnReader::Int32ColumnReader(ref mut typed_reader) => {
                    let (count, _, _) =
                        typed_reader.read_records(to_read, None, None, &mut values[c][pos..])?;

                    assert_eq!(count, to_read);
                }
                _ => bail!("invalid Parquet column type"),
            }
        }

        pos += to_read;
        if pos >= row_count {
            break;
        }
    }

    assert_eq!(pos, row_count);

    if !is_ip {
        non_ip.weight_lists = values;
    } else if !is_reflexive {
        non_reflexive.weight_lists = values.drain(0..dimension).collect();
        non_reflexive.vertex_count_list = values.remove(0);
        non_reflexive.facet_count_list = values.remove(0);
        non_reflexive.point_count_list = values.remove(0);
    } else {
        reflexive.weight_lists = values.drain(0..dimension).collect();
        reflexive.vertex_count_list = values.remove(0);
        reflexive.facet_count_list = values.remove(0);
        reflexive.point_count_list = values.remove(0);
        reflexive.dual_point_count_list = values.remove(0);
        reflexive.hodge_number_lists = values.drain(0..dimension - 3).collect();
        reflexive.euler_characteristic_list = Vec::new();
    }

    Ok((dimension, numerator, denominator))
}

fn main() -> Result<()> {
    let args = Args::parse();

    let limit = args.limit.unwrap_or(usize::MAX);

    if let (Some(ws_in), Some(polytope_info_in)) = (args.ws_in, args.polytope_info_in) {
        println!("Reading weights...");
        let (dimension, index, weights) = read_weights(ws_in, limit)?;

        println!("Reading polytope info...");
        let (non_ip, non_reflexive, reflexive) = read_polytope_info(
            dimension,
            &weights,
            args.include_derived_quantities,
            polytope_info_in,
        )?;

        println!("Writing Parquet...");
        write_parquet(
            dimension,
            &index,
            args.include_derived_quantities,
            non_ip,
            non_reflexive,
            reflexive,
            args.parquet_non_ip_out,
            args.parquet_non_reflexive_out,
            args.parquet_reflexive_out,
        )?;
    } else if !args.parquet_in.is_empty() {
        println!("Reading Parquet...");

        let mut non_ip = NonIpPolytopeInfo::default();
        let mut non_reflexive = NonReflexivePolytopeInfo::default();
        let mut reflexive = ReflexivePolytopeInfo::default();

        let mut dimension = 0;
        let mut numerator = 0;
        let mut denominator = 0;

        for path in args.parquet_in {
            (dimension, numerator, denominator) =
                read_parquet(path, &mut non_ip, &mut non_reflexive, &mut reflexive, limit)?;
        }

        println!("Writing weights and polytope info...");
        write_weights(
            dimension,
            numerator,
            denominator,
            args.ws_out,
            args.polytope_info_out,
            &non_ip,
            &non_reflexive,
            &reflexive,
        )?;
    } else {
        println!("Nothing to do.");
    }

    Ok(())
}
