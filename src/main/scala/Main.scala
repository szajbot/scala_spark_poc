package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, not, when}

object Main {
  def main(args: Array[String]): Unit = {
    val resources_path = "src/main/resources/"
    val employees_file_name = "employees.csv"
    val departments_file_name = "departments.csv"
    val transaction_file_name = "transaction.csv"

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    val df_employees = spark.read
      .option("delimiter", ";")
      .option("header", value = true)
      .csv(resources_path + employees_file_name)
      .filter(not(col("position").isin("junior")))

    val df_departments = spark.read
      .option("delimiter", ";")
      .option("header", value = true)
      .csv(resources_path + departments_file_name)

    val df_transaction = spark.read
      .option("delimiter", ";")
      .option("header", value = true)
      .csv(resources_path + transaction_file_name)
      .filter(not(col("type").isin("intermediary sale")))

    val df_1 = df_transaction.as("dt")
      .join(df_employees.as("de"), df_employees("id") === df_transaction("employee"), "inner")
      .join(df_departments.as("dd"), df_departments("id") === df_employees("department"), "outer")
      .select(
        col("dt.id").as("transaction"),
        col("value").cast("int"),
        col("type"),
        col("employee"),
        col("position"),
        col("department"),
        col("dd.name")
      )
//    Wyfiltorwani zostali pracownicy bez transakcji ale został department bez pracownika

    val df_with_employee_count = df_1.groupBy(col("department")).agg(countDistinct(col("employee")))

    val df_with_summed_values = df_1
      .select(
        col("name"),
        when(col("type") === "contract", col("value") * 6).otherwise(col("value")).as("normalized value"),
        col("department")
      ).groupBy(col("name"), col("department")).sum("normalized value")

    val df_sums_and_employees = df_with_summed_values
      .join(df_with_employee_count, df_with_summed_values.col("department") === df_with_employee_count.col("department"), "left_outer")

    val df_report = df_sums_and_employees.select(
      col("name"),
      when(col("sum(normalized value)").isNull.or(col("count(DISTINCT employee)") === 0), 0).otherwise(
        col("sum(normalized value)") / col("count(DISTINCT employee)")
      ).as("average revenue per employee")
    ).sort(col("average revenue per employee").desc)

    df_report.show()
  }
}