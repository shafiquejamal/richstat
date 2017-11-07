package com.github.shafiquejamal.dataframe.stat

import com.github.shafiquejamal.dataframe.stat.RichStat.TabOptions._
import com.github.shafiquejamal.dataframe.stat.RichStat._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpecLike, Matchers}

class StatisticianUTest extends FlatSpecLike with Matchers with DataFrameSuiteBase {
  
  import spark.implicits._
  
  val guts =
    Seq(("male", 5, "US"), ("female", 4, "CA"), ("male", 4, "US"), ("female", 8, "CA"), ("male", 15, "CA"),
        ("male", 8, "CA"), ("male", 2, "CA"), ("female", 6, "US"), ("", 20, "US"), ("female", 3, null))
  val header = Seq("gender", "age", "country")
  val tol = 0.00000001
  
  trait Data { val data = sc.parallelize(guts).toDF(header: _*) }
  
  trait HistogramData {
    val histogramData:Seq[(Double, String, String)] = Seq(List.fill(1)(0d), List.fill(3)(1.1), List.fill(30)(2d),
      List.fill(8)(3.9), List.fill(4)(4.4), List.fill(40)(5d), List.fill(2)(6.9), List.fill(5)(7.7), List.fill(4)(8d),
      List.fill(6)(9d), List.fill(8)(11d), List.fill(10)(12.8), List.fill(9)(12.9), List.fill(17)(14d), List.fill(2)(15d),
      List.fill(12)(16d), List.fill(11)(17d), List.fill(7)(18d), List.fill(13)(19d), List.fill(35)(20d)).flatten
      .zipWithIndex.map { case (age, index) =>
      val gender = if (index % 2 == 0 && age < 8) "MALE" else if (index % 2 != 0 && age > 10) "FEMALE" else null
      val country = if (index % 3 == 0) "" else if (index % 2 == 0 && age % 2 == 0) "US" else "CA"
      (age, gender, country)
    }
    
    val newRowsNullAge:Seq[(Option[Double], Option[String], String)] =
      Seq((None, None, "CA"), (None, Some("MALE"), "US"), (None, None, ""))
    val data: DataFrame = sc.parallelize(histogramData).toDF("age", "gender", "country").union(newRowsNullAge.toDF)
    
    val dataWithoutNullAge = data.filter(!col("age").isNull).cache()
    val countCountryMissing = dataWithoutNullAge.filter(col("country").isNull || trim(col("country")) === "").count()
    val countCA = dataWithoutNullAge.filter(col("country") === "CA").count()
    val countUS = dataWithoutNullAge.filter(col("country") === "US").count()
    val countGenderMissing = dataWithoutNullAge.filter(col("gender").isNull || trim(col("gender")) === "").count()
    val countM = dataWithoutNullAge.filter(col("gender") === "MALE").count()
    val countF = dataWithoutNullAge.filter(col("gender") === "FEMALE").count()
    
    val totalCounts: Double = data.filter( row => Option(row.get(0)).nonEmpty ).count().toDouble
    val expectedProps: Seq[Double] = Seq(42, 51, 18, 38, 78).map(_/totalCounts)
    val expectedDataFrameData =
      Seq((0d, 4d, 2d, 2d, 42.toLong, 42/totalCounts),
          (4d, 8d, 6d, 2d, 51.toLong, 51/totalCounts),
          (8d, 12d, 10d, 2d, 18.toLong, 18/totalCounts),
          (12d, 16d, 14d, 2d, 38.toLong, 38/totalCounts),
          (16d, 20d, 18d, 2d, 78.toLong, 78/totalCounts))
  }
  
  "The statistician" should "be able to tabulate one categorical variable, yielding percentages - blank values for " +
  "categories omitted" in new Data {
    val expected = sc.parallelize(Seq(("female", Some(4), Some(4/9d)), ("male", Some(5), Some(5/9d))))
      .toDF("gender", "count", "proportion").withColumn("count", col("count").cast("long"))
    val expectedPercent = sc.parallelize(Seq(("female", Some(4), Some(100*4/9d)), ("male", Some(5), Some(100*5/9d))))
      .toDF("gender", "count", "percent").withColumn("count", col("count").cast("long"))
    val tabulated = data.tab("gender").toDF
    val tabulatedPercent = data.tab("gender", Percent).toDF
    
    assertDataFrameEquals(tabulated, expected)
    assertDataFrameEquals(tabulatedPercent, expectedPercent)
  }
  
  it should "be able to tabulate one categorical variable, yielding percentages - blank values for categories " +
  "included" in new Data {
    val expected =
      sc.parallelize(Seq(("female", Some(4), Some(0.4)), ("male", Some(5), Some(0.5)), ("", Some(1), Some(0.1))))
        .toDF("gender", "count", "proportion").withColumn("count", col("count").cast("long"))
    val expectedPercent =
      sc.parallelize(Seq(("female", Some(4), Some(100*0.4)), ("male", Some(5), Some(100*0.5)), ("", Some(1), Some(100*0.1))))
        .toDF("gender", "count", "percent").withColumn("count", col("count").cast("long"))
    val tabulated = data.tab("gender", IncludeMissing).toDF
    val tabulatedPercent = data.tab("gender", IncludeMissing, Percent).toDF
    
    assertDataFrameEquals(tabulated, expected)
    assertDataFrameEquals(tabulatedPercent, expectedPercent)
  }
  
  it should "be able to cross tabulate to yield column percentages" in new Data {
    val expected =
      sc.parallelize(Seq(("male", Some(0.6d), Some(2/3d)), ("female", Some(0.4), Some(1/3d))))
        .toDF("gender_country", "country___CA", "country___US")
    val tabulated = data.crossTab("gender", "country", ByColumn).toDF
  
    val expectedPercent =
      sc.parallelize(Seq(("male", Some(100*0.6d), Some(100*2/3d)), ("female", Some(100*0.4), Some(100*1/3d))))
        .toDF("gender_country", "country___CA", "country___US")
    val tabulatedPercent = data.crossTab("gender", "country", ByColumn, Percent).toDF
    
    assertDataFrameEquals(expected, tabulated)
    assertDataFrameEquals(expectedPercent, tabulatedPercent)
  }

  it should "be able to cross tabulate to yield row percentages" in new Data {
    val expected =
      sc.parallelize(Seq(("male", Some(0.6), Some(0.4)), ("female", Some(2/3d), Some(1/3d))))
        .toDF("gender_country", "country___CA", "country___US")
    val tabulated = data.crossTab("gender", "country", ByRow).toDF
  
    val expectedPercent =
      sc.parallelize(Seq(("male", Some(100*0.6), Some(100*0.4)), ("female", Some(100*2/3d), Some(100*1/3d))))
        .toDF("gender_country", "country___CA", "country___US")
    val tabulatedPercent = data.crossTab("gender", "country", ByRow, Percent).toDF
    
    assertDataFrameEquals(expected, tabulated)
    assertDataFrameEquals(expectedPercent, tabulatedPercent)
  }

  it should "give a crosstab of counts if neither column nor row option is specified" in new Data {
    val expected = data.stat.crosstab("gender", "country")
    val tabulated = data.crossTab("gender", "country", IncludeMissing)
    
    assertDataFrameEquals(expected, expected)
  }
  
  it should "give a crosstab of counts and percent if neither column nor row option is specified, but the Percent " +
  "option is specified" in new Data {
    val expected =
      sc.parallelize(Seq(("male", Some(100*0.6d), Some(100*2/3d)), ("female", Some(100*0.4), Some(100*1/3d))))
        .toDF("gender_country", "country___CA", "country___US")
    val tabulated = data.crossTab("gender", "country", Percent)
    
    assertDataFrameEquals(expected, expected)
  }
  
  "For historgrams, the Statistician" should "return a histogram of counts and proportions" in new HistogramData {
    val expectedHistogram =
      sc.parallelize(expectedDataFrameData).toDF("age", "", "_midpoint_", "_rangeHalfWidth_", "count", "proportion")
    val actualHistogram = data.histogram("age", 5)
  
    val expectedDataFrameDataPercent = expectedDataFrameData.map { case (z, b, c, d, e, f) => (z, b, c, d, e, f*100d) }
    val expectedHistogramPercent =
      sc.parallelize(expectedDataFrameDataPercent).toDF("age", "", "_midpoint_", "_rangeHalfWidth_", "count", "percent")
    val actualHistogramPercent = data.histogramPercent("age", 5)
    
    assertDataFrameEquals(expectedHistogram, actualHistogram)
    assertDataFrameEquals(expectedHistogramPercent, actualHistogramPercent)
  }
  
  it should "return counts and proportions for categories with the specified variables" in new HistogramData {
    val expected = sc.parallelize(Seq[(Double, Double, Double, Double, Long, Double, Double, Double, Double, Double, Double, Double)](
      (0d, 4d, 2d, 2d, 42, 42d/227, 18d/countCA, 10d/countUS, 14d/countCountryMissing, 21d/countGenderMissing, 21d/countM, 0d/countF),
      (4d, 8d, 6d, 2d, 51, 51d/227, 34d/countCA, 0d/countUS, 17d/countCountryMissing, 25d/countGenderMissing, 26d/countM, 0d/countF),
      (8d, 12d, 10d, 2d, 18, 18d/227, 11d/countCA, 1d/countUS, 6d/countCountryMissing, 14d/countGenderMissing, 0d/countM, 4d/countF),
      (12d, 16d, 14d, 2d, 38, 38d/227, 19d/countCA, 6d/countUS, 13d/countCountryMissing, 19d/countGenderMissing, 0d/countM, 19d/countF),
      (16d, 20d, 18d, 2d, 78, 78d/227, 33d/countCA, 19d/countUS, 26d/countCountryMissing, 39d/countGenderMissing, 0d/countM, 39d/countF)
    )).toDF("age", "", "_midpoint_", "_rangeHalfWidth_", "count", "proportion", "country___CA", "country___US",
      "country___(country missing)", "gender___(gender missing)", "gender___MALE", "gender___FEMALE")
    val actual = data.histogram("age", 5, "country", "gender")
    
    val expectedPercent = sc.parallelize(Seq[(Double, Double, Double, Double, Long, Double, Double, Double, Double, Double, Double, Double)](
      (0d, 4d, 2d, 2d, 42, 100*42d/227, 100*18d/countCA, 100*10d/countUS, 100*14d/countCountryMissing, 100*21d/countGenderMissing, 100*21d/countM, 100*0d/countF),
      (4d, 8d, 6d, 2d, 51, 100*51d/227, 100*34d/countCA, 100*0d/countUS, 100*17d/countCountryMissing, 100*25d/countGenderMissing, 100*26d/countM, 100*0d/countF),
      (8d, 12d, 10d, 2d, 18, 100*18d/227, 100*11d/countCA, 100*1d/countUS, 100*6d/countCountryMissing, 100*14d/countGenderMissing, 100*0d/countM, 100*4d/countF),
      (12d, 16d, 14d, 2d, 38, 100*38d/227, 100*19d/countCA, 100*6d/countUS, 100*13d/countCountryMissing, 100*19d/countGenderMissing, 100*0d/countM, 100*19d/countF),
      (16d, 20d, 18d, 2d, 78, 100*78d/227, 100*33d/countCA, 100*19d/countUS, 100*26d/countCountryMissing, 100*39d/countGenderMissing, 100*0d/countM, 100*39d/countF)
    )).toDF("age", "", "_midpoint_", "_rangeHalfWidth_", "count", "percent", "country___CA", "country___US",
      "country___(country missing)", "gender___(gender missing)", "gender___MALE", "gender___FEMALE")
    val actualPercent = data.histogramPercent("age", 5, "country", "gender")
    
    assertDataFrameEquals(expected, actual)
    assertDataFrameApproximateEquals(expectedPercent, actualPercent, tol)
  }
  
  it should "return counts and proportions for categories with the specified variables when given the bucket " +
  "demarcations" in new HistogramData {
    val bucketDemarcations = Array(0d, 8d, 16d, 20d)
    
    val expected = sc.parallelize(Seq[(Double, Double, Double, Double, Long, Double, Double, Double, Double, Double, Double, Double)](
      (0d, 8d, 4d, 4d, 42 + 51,  (42d + 51d)/227, (18d + 34d)/countCA, (10d + 0d)/countUS, (14d + 17d)/countCountryMissing, (21d + 25d)/countGenderMissing, (21d + 26d)/countM,         0d/countF),
      (8d, 16d, 12d, 4d, 18 + 38, (18d + 38d)/227, (11d + 19d)/countCA,  (1d + 6f)/countUS,  (6d + 13d)/countCountryMissing, (14d + 19d)/countGenderMissing,          0d/countM, (4d + 19d)/countF),
      (16d, 20d, 18d, 2d, 78,             78d/227,         33d/countCA,        19d/countUS,         26d/countCountryMissing,         39d/countGenderMissing,          0d/countM,        39d/countF)
    )).toDF("age", "", "_midpoint_", "_rangeHalfWidth_", "count", "proportion", "country___CA", "country___US",
      "country___(country missing)", "gender___(gender missing)", "gender___MALE", "gender___FEMALE")
    val actual = data.histogram("age", bucketDemarcations, "country", "gender")
  
    val expectedPercent = sc.parallelize(Seq[(Double, Double, Double, Double, Long, Double, Double, Double, Double, Double, Double, Double)](
      (0d, 8d, 4d, 4d, 42 + 51,  100*(42d + 51d)/227, 100*(18d + 34d)/countCA, 100*(10d + 0d)/countUS, 100*(14d + 17d)/countCountryMissing, 100*(21d + 25d)/countGenderMissing, 100*(21d + 26d)/countM,         100*0d/countF),
      (8d, 16d, 12d, 4d, 18 + 38, 100*(18d + 38d)/227, 100*(11d + 19d)/countCA,  100*(1d + 6f)/countUS,  100*(6d + 13d)/countCountryMissing, 100*(14d + 19d)/countGenderMissing,          100*0d/countM, 100*(4d + 19d)/countF),
      (16d, 20d, 18d, 2d, 78,             100*78d/227,         100*33d/countCA,        100*19d/countUS,         100*26d/countCountryMissing,         100*39d/countGenderMissing,          100*0d/countM,        100*39d/countF)
    )).toDF("age", "", "_midpoint_", "_rangeHalfWidth_", "count", "percent", "country___CA", "country___US",
      "country___(country missing)", "gender___(gender missing)", "gender___MALE", "gender___FEMALE")
    val actualPercent = data.histogramPercent("age", bucketDemarcations, "country", "gender")
    
    assertDataFrameEquals(expected, actual)
    assertDataFrameApproximateEquals(expectedPercent, actualPercent, tol)
  }
  
  "The statistician writer" should "call the writer for each row of the dataframe" in new Data {
    var rowsWrittenOut = Seq[Seq[String]]()
    def writer(row: Seq[String]): Unit = { rowsWrittenOut = rowsWrittenOut :+ row }
  
    val inputDF = sc.parallelize(Seq(("female", 4, 4/9d), ("male", 5, 5/9d)))
      .toDF("gender", "count", "proportion").withColumn("count", col("count").cast("long"))
    val expected = Seq(
      Seq("gender", "count", "proportion"),
      Seq("female", "4", (4/9d).toString),
      Seq("male", "5", (5/9d).toString))
    
    inputDF.writeOut(writer, isFollowWithBlank = false)
    rowsWrittenOut should contain theSameElementsInOrderAs expected
  
    rowsWrittenOut = Seq[Seq[String]]()
    inputDF.writeOut(writer, isIncludeHeader = false)
    rowsWrittenOut should contain theSameElementsInOrderAs expected.tail :+ Seq("")
  }
  
  it should "be able to convert a dataframe for writing to CSV" in new Data {
    val expectedGuts = guts.map { case (gender, age, country) =>
      Seq[String](gender, age.toString, Option(country).fold(""){_.toString}) }
    data.forCSV().map(_.toList) should contain theSameElementsInOrderAs header +: expectedGuts
    data.forCSV(isIncludeHeader = false) should contain theSameElementsInOrderAs expectedGuts
  }
  
}
