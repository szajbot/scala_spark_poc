package org.example

import org.apache.spark.sql.{Dataset, Row, SparkSession}
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

    val df_transaction = readFileFromPath(spark, resources_path + transaction_file_name)

    val df_employees = readFileFromPath(spark, resources_path + employees_file_name)

    val df_departments = readFileFromPath(spark, resources_path + departments_file_name)

    val df_final = generateReport(df_departments, df_employees, df_transaction)

    df_final.show()
  }

  def generateReport(df_departments: Dataset[Row], df_employees: Dataset[Row], df_transaction: Dataset[Row]): Dataset[Row] = {
    // Filter some selected transaction type and employee position
    val df_transaction_filtered = df_transaction.filter(not(col("type").isin("intermediary sale")))
    val df_employees_filtered = df_employees.filter(not(col("position").isin("junior")))

    val df_joined_sets = df_transaction_filtered.as("dt")
      .join(df_employees_filtered.as("de"), df_employees_filtered("id") === df_transaction_filtered("employee"), "inner")
      .join(df_departments.as("dd"), df_departments("id") === df_employees_filtered("department"), "outer")
      .select(
        col("dt.id").as("transaction"),
        col("value").cast("int"),
        col("type"),
        col("employee"),
        col("position"),
        col("department"),
        col("dd.name")
      )
    //Joined set is filtered from employees withour transaction but not from departments without employees

    val df_with_employee_count = df_joined_sets.groupBy(col("department")).agg(countDistinct(col("employee")))

    val df_with_summed_values = df_joined_sets
      .select(
        col("name"),
        when(col("type") === "contract", col("value") * 6).otherwise(col("value")).as("normalized value"),
        col("department")
      ).groupBy(col("name"), col("department")).sum("normalized value")

    val df_sums_and_employees = df_with_summed_values
      .join(df_with_employee_count, df_with_summed_values.col("department") === df_with_employee_count.col("department"), "left_outer")

    df_sums_and_employees.select(
      col("name"),
      when(col("sum(normalized value)").isNull.or(col("count(DISTINCT employee)") === 0), 0).otherwise(
        col("sum(normalized value)") / col("count(DISTINCT employee)")
      ).as("average revenue per employee")
    ).sort(col("average revenue per employee").desc)
  }

  def readFileFromPath(spark: SparkSession, filePath: String): Dataset[Row] = {
    spark.read
      .option("delimiter", ";")
      .option("header", value = true)
      .csv(filePath)
  }
}