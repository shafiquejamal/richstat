package com.github.shafiquejamal.dataframe.stat

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import DataFrameUtils.generateCategoricalVariableFrom
import org.apache.spark.sql.types.DoubleType

import scala.util.Try


object WeightedHistogram {

  private def bucketDemarcationsFrom(nBuckets: Int, df: DataFrame, column: String): Seq[Double] = {
    val maybeMax: Option[Double] = toMaybeDoubleFrom(df.groupBy().max(column).collect()(0))
    val maybeMin: Option[Double] = toMaybeDoubleFrom(df.groupBy().min(column).collect()(0))
    (for {
      max <- maybeMax
      min <- maybeMin
    } yield {
      val delta: Double = (max - min)/nBuckets
      min.to(max, delta)
    }).toSeq.flatten
  }

  private def toMaybeDoubleFrom(row: Row): Option[Double] =
   Try(row.getDouble(0))
     .orElse(Try(row.getDecimal(0).doubleValue()))
     .orElse(Try(row.getInt(0).toDouble))
     .orElse(Try(row.getLong(0).toDouble))
     .toOption

  private def toMaybeDoubleFrom(row: Row, column: String): Option[Double] =
    Try(row.getAs[Double](column))
      .orElse(Try(row.getAs[BigDecimal](column).doubleValue()))
      .orElse(Try(row.getAs[Int](column).toDouble))
      .orElse(Try(row.getAs[Long](column).toDouble))
      .toOption

  def maybeHistogram(df: DataFrame, column: String, weightColumn: String, nBuckets: Int):
  Option[DataFrame] = {
    val bucketDemarcations: Seq[Double] = bucketDemarcationsFrom(nBuckets, df, column)
    maybeHistogram(df, column, weightColumn, bucketDemarcations)
  }

  def maybeHistogram(df: DataFrame, column: String, weightColumn: String, bucketDemarcations: Seq[Double]):
  Option[DataFrame] = {
    import df.sparkSession.implicits._
    val pattern = "^_(.*)_to_(.*)_".r
    val hist = df.withColumn("_grouped_", generateCategoricalVariableFrom(df(column), bucketDemarcations, "_", "_to_", "_"))
        .groupBy("_grouped_")
        .agg(count(column), sum(weightColumn))
        .filter(col("_grouped_") =!= lit("__other__"))
    val maybeCount: Option[Double] = toMaybeDoubleFrom(hist.select(sum(s"count($column)")).collect()(0))
    val maybeSumOfWeights: Option[Double] = toMaybeDoubleFrom(hist.select(sum(s"sum($weightColumn)")).collect()(0))
    for {
     count <- maybeCount
     sumOfWeights <- maybeSumOfWeights
    } yield {
      hist
        .withColumn(s"${column}_prop", col(s"count($column)") / lit(count))
        .withColumn(s"${weightColumn}_prop", col(s"sum($weightColumn)") / lit(sumOfWeights))
        .withColumn(s"${column}_lower_bound", regexp_extract(col("_grouped_"), "^_(.*)_to_(.*)_", 1))
        .withColumn(s"${column}_lower_bound", translate(col(s"${column}_lower_bound"), "p", "."))
        .withColumn(s"${column}_lower_bound", col(s"${column}_lower_bound").cast(DoubleType))
        .withColumn(s"${column}_upper_bound", regexp_extract(col("_grouped_"), "^_(.*)_to_(.*)_", 2))
        .withColumn(s"${column}_upper_bound", translate(col(s"${column}_upper_bound"), "p", "."))
        .withColumn(s"${column}_upper_bound", col(s"${column}_upper_bound").cast(DoubleType))
        .withColumn(s"${column}_midpoint",
          col(s"${column}_lower_bound") + (col(s"${column}_upper_bound") - col(s"${column}_lower_bound")) / 2)
        .withColumn(s"${column}_range", col(s"${column}_upper_bound") - col(s"${column}_lower_bound"))
    }
  }

}
