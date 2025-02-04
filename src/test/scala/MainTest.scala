package org.example

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class MainAppTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark Test")
    .master("local[*]")
    .getOrCreate()

  test("Should properly calculate values") {
    val df_departments = spark.createDataFrame(Seq(
      (1, "First test department"),
      (2, "Second test department")
    )).toDF("id", "name")

    val df_employees = spark.createDataFrame(Seq(
      (1, "Adam", "Abacki", "senior", 1),
      (2, "Adam", "Cabacki", "mid", 2)
    )).toDF("id", "name", "surname", "position", "department")

    val df_transaction = spark.createDataFrame(Seq(
      (1, 100, "contract", 1),
      (2, 100, "sale", 2),
      (3, 100,"sale", 2)
    )).toDF("id","value","type","employee")

    val result = Main.generateReport(df_departments, df_employees, df_transaction)

    assert(result.filter(col("name") === "First test department").select(col("average revenue per employee")).head().get(0) === 600)
    assert(result.filter(col("name") === "Second test department").select(col("average revenue per employee")).head().get(0) === 200)
  }

  test("Should include empty department") {
    val df_departments = spark.createDataFrame(Seq(
      (1, "Empty department"),
      (2, "Not empty department")
    )).toDF("id", "name")

    val df_employees = spark.createDataFrame(Seq(
      (1, "Adam", "Abacki", "senior", 2),
      (2, "Adam", "Cabacki", "mid", 2)
    )).toDF("id", "name", "surname", "position", "department")

    val df_transaction = spark.createDataFrame(Seq(
      (1, 100, "contract", 1),
      (2, 100, "sale", 2),
      (3, 100,"sale", 2)
    )).toDF("id","value","type","employee")

    val result = Main.generateReport(df_departments, df_employees, df_transaction)

    assert(result.filter(col("name") === "Empty department").select(col("average revenue per employee")).head().get(0) === 0)
  }

  test("Should filter employee without transaction") {
    val df_departments = spark.createDataFrame(Seq(
      (1, "First department")
    )).toDF("id", "name")

    val df_employees = spark.createDataFrame(Seq(
      (1, "Adam", "Abacki", "senior", 1),
      (2, "Adam", "Cabacki", "mid", 1)
    )).toDF("id", "name", "surname", "position", "department")

    val df_transaction = spark.createDataFrame(Seq(
      (2, 100, "sale", 2),
      (3, 100,"sale", 2)
    )).toDF("id","value","type","employee")

    val result = Main.generateReport(df_departments, df_employees, df_transaction)

    assert(result.filter(col("name") === "First department").select(col("average revenue per employee")).head().get(0) === 200)
  }

  test("Should filter selected transaction type") {
    val df_departments = spark.createDataFrame(Seq(
      (1, "First department")
    )).toDF("id", "name")

    val df_employees = spark.createDataFrame(Seq(
      (1, "Adam", "Abacki", "senior", 1),
      (2, "Adam", "Cabacki", "mid", 1)
    )).toDF("id", "name", "surname", "position", "department")

    val df_transaction = spark.createDataFrame(Seq(
      (2, 100, "sale", 2),
      (3, 100,"sale", 2)
    )).toDF("id","value","type","employee")

    val result = Main.generateReport(df_departments, df_employees, df_transaction)

    assert(result.filter(col("name") === "First department").select(col("average revenue per employee")).head().get(0) === 200)
  }

  //TODO test na filtrowanie (2)

  //TODO filtorwanie pracownika ktorego transakcje ostaly odciecte prze filter
}
