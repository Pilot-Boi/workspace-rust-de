use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    let table_path = ListingTableUrl::parse("data/")?;
    let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    ctx.register_listing_table("taxi_trips", table_path, options, None, None)
        .await?;

    // Aggregation 1: Trips and revenue by month
    let sql_month = r#"
        SELECT
            date_trunc('month', tpep_pickup_datetime) AS pickup_month,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS total_revenue,
            AVG(fare_amount) AS avg_fare
        FROM taxi_trips
        GROUP BY date_trunc('month', tpep_pickup_datetime)
        ORDER BY pickup_month ASC
    "#;

    println!("Aggregation 1: Trips and revenue by month");
    let df_month = ctx.sql(sql_month).await?;
    let result_month = df_month.collect().await?;
    for batch in result_month {
        println!(
            "{}",
            datafusion::arrow::util::pretty::pretty_format_batches(&[batch])?
        );
    }

    println!();

    // Aggregation 2: Tip behavior by payment type
    let sql_payment = r#"
        SELECT
            payment_type,
            COUNT(*) AS trip_count,
            AVG(tip_amount) AS avg_tip_amount,
            SUM(tip_amount) / SUM(total_amount) AS tip_rate
        FROM taxi_trips
        GROUP BY payment_type
        ORDER BY trip_count DESC
    "#;

    println!("Aggregation 2: Tip behavior by payment type");
    let df_payment = ctx.sql(sql_payment).await?;
    let result_payment = df_payment.collect().await?;
    for batch in result_payment {
        println!(
            "{}",
            datafusion::arrow::util::pretty::pretty_format_batches(&[batch])?
        );
    }

    Ok(())
}