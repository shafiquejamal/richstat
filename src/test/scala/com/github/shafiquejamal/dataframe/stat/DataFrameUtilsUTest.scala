package com.github.shafiquejamal.dataframe.stat

import DataFrameUtils._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpecLike, Matchers}

class DataFrameUtilsUTest extends FlatSpecLike with Matchers with DataFrameSuiteBase {

  case class TestCC(index: Int, age: Int, group: String)

  trait Fixture {
    import spark.implicits._

    val data = Seq((1, 25, "Big Accra"), (2, 26, "Accra"), (3, 5, "Western"), (4, 80, "Upper West Region"),
      (5, 45, "Eastern Region"), (6, 34, "Upper East"), (7, 36, "Tema"), (8, 38, "Cairo"),
      (9, 19, "Greater Accra Region"), (10, 51, "Greater Accra"))
    val columnNames = Seq("index", "age", "group")
    val df = sc.parallelize(data).toDF(columnNames: _*)
  }

  "Creating a categorical variable from a numeric column" should "insert values in the columns that are the correct" +
  "category for the corresponding value in the column to be categorized" in new Fixture {
    val newColumnName = "age (grouped)"
    val groups: Seq[Double] = Seq(0, 10, 24, 33, 45, 45.001, 46, 51.001, 81, 1000)
    val expected = Seq(
      TestCC(1, 25, "foo 24k0 mid 33k0 bar"), TestCC(2, 26, "foo 24k0 mid 33k0 bar"),
      TestCC(3, 5, "foo 0k0 mid 10k0 bar"), TestCC(4, 80, "foo 51k001 mid 81k0 bar"),
      TestCC(5, 45, "foo 45k0 mid 45k001 bar"), TestCC(6, 34, "foo 33k0 mid 45k0 bar"),
      TestCC(7, 36, "foo 33k0 mid 45k0 bar"), TestCC(8, 38, "foo 33k0 mid 45k0 bar"),
      TestCC(9, 19, "foo 10k0 mid 24k0 bar"), TestCC(10, 51, "foo 46k0 mid 51k001 bar"))

    val actual =
      df.drop("group")
        .withColumn(newColumnName, generateCategoricalVariableFrom(col("age"), groups, "foo ", " mid ", " bar", "k"))
        .collect().toSeq.map { row => TestCC(row.getInt(0), row.getInt(1), row.getString(2)) }

    actual should contain theSameElementsAs expected
  }

  "Regrouping a categorical variable by case insensitive exact match" should "regroup, putting unmatched values into " +
  "an 'other' category" in new Fixture {
    import spark.implicits._

    val mapping = Map[String, Seq[String]](
      "Greater Accra" -> Seq("accra", "big accra", "greater accra region"),
      "Eastern" -> Seq("Eastern Region"),
      "Western" -> Seq(),
      "upper weSt" -> Seq("Upper West region"),
      "Upper East" -> Seq(),
      "Tema" -> Seq("temA")
    )
    val newColumnName = "groUped"
    val iDsWithGroupKey = sc.parallelize(Seq(
      (1, "Greater Accra"), (2, "Greater Accra"), (3, "Western"), (4, "upper weSt"), (5, "Eastern"), (6, "Upper East"),
      (7, "Tema"), (8, "Some oth"), (9,"Greater Accra"), (10, "Greater Accra")
    )).toDF("index", newColumnName)
    val expected = df.join(iDsWithGroupKey, "index")
     .orderBy("index")

    val actual = df
      .withColumn(newColumnName, regroupCategoricalContainsExactly(col("group"), mapping, "Some oth")).orderBy("index")
      .withColumn(newColumnName, when(col(newColumnName).isNotNull, col(newColumnName)).otherwise(lit(null)))

    assertDataFrameEquals(expected, actual)
  }

  "Regrouping a categorical variable by case insensitive partial match" should "regroup, putting unmatched values " +
  "into an 'other' category" in new Fixture {
    import spark.implicits._

    val mappingEast = Map[Boolean, Seq[String]](true -> Seq("EaST"))
    val newColGroupedEast = "Contains_EAST"

    val newColGroupedRegion = "grouped_region"
    val mappingRegion = Map[String, Seq[String]](
      "accrA" -> Seq("aCcRA"), "East" -> Seq("east"), "WEST" -> Seq("WEST"), "teMa" -> Seq())

    val actual = df
      .withColumn(newColGroupedRegion,
        regroupCategoricalContainsWithin(col("group"), mappingRegion, lit("udder"))).orderBy("index")
      .withColumn(newColGroupedRegion, when(col(newColGroupedRegion).isNotNull, col(newColGroupedRegion))
        .otherwise(lit(null)))
      .withColumn(newColGroupedEast,
        regroupCategoricalContainsWithin(col("group"), mappingEast, lit(false))).orderBy("index")
      .withColumn(newColGroupedEast, when(col(newColGroupedEast).isNotNull, col(newColGroupedEast))
        .otherwise(lit(null)))
      .orderBy("index")

    val iDsWithGroupKey = sc.parallelize(Seq(
      (1, "accrA", false), (2, "accrA", false), (3, "WEST", false), (4, "WEST", false),
      (5, "East", true), (6, "East", true), (7, "teMa", false), (8, "udder", false),
      (9, "accrA", false), (10, "accrA", false)
    )).toDF("index", newColGroupedRegion, newColGroupedEast)
      .withColumn(newColGroupedEast, when(col(newColGroupedEast).isNotNull, col(newColGroupedEast))
        .otherwise(lit(null)))

    val expected = df.join(iDsWithGroupKey, "index").orderBy("index")

    assertDataFrameEquals(expected, actual)
  }

}
