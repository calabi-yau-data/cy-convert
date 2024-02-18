use anyhow::Result;
use parquet::basic::{LogicalType, Repetition, Type as PhysicalType};
use parquet::file::writer::SerializedRowGroupWriter;
use parquet::schema::types::Type as SchemaType;
use std::io::Write;
use std::sync::Arc;

pub const ROW_GROUP_SIZE: usize = 5_000_000;

pub fn build_parquet_int_field(name: &str) -> Result<Arc<SchemaType>> {
    Ok(Arc::new(
        SchemaType::primitive_type_builder(name, PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .build()?,
    ))
}

// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
pub fn build_parquet_int_list_of_lists_field(name: &str) -> Result<Arc<SchemaType>> {
    let inner_element = SchemaType::primitive_type_builder("element", PhysicalType::INT32)
        .with_repetition(Repetition::REQUIRED)
        .build()?;

    let inner_list = SchemaType::group_type_builder("list")
        .with_repetition(Repetition::REPEATED)
        .with_fields([Arc::new(inner_element)].to_vec())
        .build()?;

    let outer_element = SchemaType::group_type_builder("element")
        .with_logical_type(Some(LogicalType::List))
        .with_repetition(Repetition::REQUIRED)
        .with_fields([Arc::new(inner_list)].to_vec())
        .build()?;

    let outer_list = SchemaType::group_type_builder("list")
        .with_repetition(Repetition::REPEATED)
        .with_fields([Arc::new(outer_element)].to_vec())
        .build()?;

    let field = SchemaType::group_type_builder(name)
        .with_logical_type(Some(LogicalType::List))
        .with_repetition(Repetition::REQUIRED)
        .with_fields([Arc::new(outer_list)].to_vec())
        .build()?;

    Ok(Arc::new(field))
}

pub fn write_parquet_int_column<W: Write + Send>(
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

pub fn write_repeated_parquet_int_column<W: Write + Send>(
    row_group_writer: &mut SerializedRowGroupWriter<W>,
    data: &[i32],
    definition_levels: &[i16],
    repetition_levels: &[i16],
) -> Result<usize> {
    use parquet::data_type::Int32Type;

    let mut col_writer = row_group_writer.next_column()?.expect("column");

    let count = col_writer.typed::<Int32Type>().write_batch(
        data,
        Some(definition_levels),
        Some(repetition_levels),
    )?;
    col_writer.close()?;

    Ok(count)
}
