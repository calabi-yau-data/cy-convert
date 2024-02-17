use anyhow::Result;
use parquet::basic::{Repetition, Type as PhysicalType};
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
