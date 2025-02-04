package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, not}

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

    //  Skip records from transaction dataframe
    val expression = not(col("type").isin("intermediary sale"))
    val df_transaction_filtered = df_transaction.filter(expression)

    val transaction_employees_join = df_transaction_filtered(
      "employee"
    ) === df_employees("id")

    val df_joined = df_transaction_filtered.as("dt")
      .join(df_employees.as("de"), transaction_employees_join, "inner")
      .select(col("value"), col("type"), col("employee"), col("dt.id").as("transaction"), col("department"))

    val employees_department_join = df_joined(
      "department"
    ) === df_departments_prepared("id")

    val df_joined2 = df_joined.join(df_departments_prepared, employees_department_join, "outer")
      .select(col("value").cast("int"), col("type"), col("employee"), col("name"), col("department"), col("employee_count"))

    df_joined2.groupBy(col("department"), col("name"), col("employee_count")).sum("value").show()
//
//    df_joined2.show()
  }
}