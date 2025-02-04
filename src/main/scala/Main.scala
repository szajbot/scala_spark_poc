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
      .select(col("id"), col("department"))
      .filter(not(col("position").isin("junior")))

    val df_departments = spark.read
      .option("delimiter", ";")
      .option("header", value = true)
      .csv(resources_path + departments_file_name)

    // Departments join employee to get employee count
    val df_departments_prepared = df_departments.as("dd")
      .join(df_employees.as("de"), df_departments("id") === df_employees("department"), "outer")
      .groupBy(col("dd.id"), col("dd.name"))
      .agg(countDistinct("de.id").as("employee_count"))

    val df_transaction = spark.read
      .option("delimiter", ";")
      .option("header", value = true)
      .csv(resources_path + transaction_file_name)
      .filter(not(col("type").isin("intermediary sale")))

    val transaction_employees_join = df_transaction(
      "employee"
    ) === df_employees("id")

    val df_joined = df_transaction.as("dt")
      .join(df_employees.as("de"), transaction_employees_join, "inner")
      .select(col("value"), col("type"), col("employee"), col("dt.id").as("transaction"), col("department"))

    val employees_department_join = df_joined(
      "department"
    ) === df_departments_prepared("id")

    val df_with_employee_count = df_joined.join(df_departments_prepared, employees_department_join, "outer")
      .select(col("value").cast("int"), col("type"), col("employee"), col("name"), col("department"), col("employee_count"))

    val df_with_normalized_value = df_with_employee_count.withColumn(
      "normalized_value",
      when(col("type") === "contract", col("value") * 3).otherwise(col("value"))
    )

    val df_for_report = df_with_normalized_value
      .groupBy(col("department"), col("name"), col("employee_count"))
      .sum("normalized_value")
      .select(
        col("name").as("Department name"),
        when(col("sum(normalized_value)").isNotNull, col("sum(normalized_value)")).otherwise(0).as("average revenue per employee")
      ).sort(col("average revenue per employee").desc)

    df_for_report.show()
  }
}