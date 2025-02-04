package org.example

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val resourcesPath = "src/main/resources/"
    val employeesFileName = "employees.csv"
    val departmentsFileName = "departments.csv"
    val transactionFileName = "transaction.csv"

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    val df_departments = spark.read
      .option("delimiter", ";")
      .option("header", value = true)
      .csv(resourcesPath + departmentsFileName)

    val df_employees = spark.read
      .option("delimiter", ";")
      .option("header", value = true)
      .csv(resourcesPath + employeesFileName)

    val df_transaction = spark.read
      .option("delimiter", ";")
      .option("header", value = true)
      .csv(resourcesPath + transactionFileName)

    df_departments.show()
    df_employees.show()
    df_transaction.show()

  }
}