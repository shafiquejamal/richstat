package com.github.shafiquejamal.dataframe.stat

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, trim}


object HistogramSpecialist {
  
  def histogram(df:DataFrame, variable: String, bucketDemarcations: Array[Double], crossTabVariables: String*):
  DataFrame = {
    val (factor, columnName) = (1d, "proportion")
    val dsNonNullVariable = df.filter(!col(variable).isNull)
    val baseHistogram = singleVariableHistogram(dsNonNullVariable, variable, bucketDemarcations, factor, columnName)
    histogramGivenUnconditional(dsNonNullVariable, variable, bucketDemarcations, baseHistogram, factor, columnName, crossTabVariables: _*)
  }
  
  def histogram(df:DataFrame, variable: String, nBuckets: Int, crossTabVariables: String*): DataFrame = {
    val (factor, columnName) = (1d, "proportion")
    val dsNonNullVariable = df.filter(!col(variable).isNull)
    val (baseHistogram, bucketDemarcations) = singleVariableHistogram(dsNonNullVariable, variable, nBuckets, factor, columnName)
    histogramGivenUnconditional(dsNonNullVariable, variable, bucketDemarcations, baseHistogram, factor, columnName, crossTabVariables: _*)
  }
  
  def histogramPercent(df:DataFrame, variable: String, bucketDemarcations: Array[Double], crossTabVariables: String*):
  DataFrame = {
    val (factor, columnName) = (100d, "percent")
    val dsNonNullVariable = df.filter(!col(variable).isNull)
    val baseHistogram = singleVariableHistogram(dsNonNullVariable, variable, bucketDemarcations, factor, columnName)
    histogramGivenUnconditional(
      dsNonNullVariable, variable, bucketDemarcations, baseHistogram, factor, columnName, crossTabVariables: _*)
  }
  
  def histogramPercent(df:DataFrame, variable: String, nBuckets: Int, crossTabVariables: String*): DataFrame = {
    val (factor, columnName) = (100d, "percent")
    val dsNonNullVariable = df.filter(!col(variable).isNull)
    val (baseHistogram, bucketDemarcations) = singleVariableHistogram(dsNonNullVariable, variable, nBuckets, factor, columnName)
    histogramGivenUnconditional(
      dsNonNullVariable, variable, bucketDemarcations, baseHistogram, factor, columnName, crossTabVariables: _*)
  }
  
  private def histogramGivenUnconditional(
      df:DataFrame, variable: String, bucketDemarcations: Array[Double], baseHistogram: DataFrame, factor: Double,
      columnName: String, crossTabVariables: String*):
  DataFrame = {
    val dataFrameWithMergeColumnsOnly = baseHistogram.select(variable, "")
    crossTabVariables.foldLeft(baseHistogram){ case (accumulatedHistogram, crossTabVariable) =>
    
      val crossTabVariableCategories: List[String] =
        df.groupBy(col(crossTabVariable)).count().select(crossTabVariable).collect().toList.map { row =>
          Option(row.get(0)).fold(""){_.toString}
        }
    
      val newHistogram = crossTabVariableCategories.foldLeft(dataFrameWithMergeColumnsOnly){
        case (accumulatedHistogramForCategory, category) =>
          val categoryNameEmptyReplacedWithText = crossTabVariable + "___" +
            (if (category.trim.isEmpty) s"($crossTabVariable missing)" else category)
          val dataFrameWithOnlyThisCategory = df.filter(col(crossTabVariable).isNull && category.trim.isEmpty ||
            trim(col(crossTabVariable)) === category.trim).cache()
        
          val histogramWithColumnToAdd =
            singleVariableHistogram(dataFrameWithOnlyThisCategory, variable, bucketDemarcations, factor, columnName)
            .withColumnRenamed(columnName, categoryNameEmptyReplacedWithText).drop("count",columnName)
        
          accumulatedHistogramForCategory
          .join(histogramWithColumnToAdd.select(variable, "",  categoryNameEmptyReplacedWithText), Seq(variable, ""))
      }.drop("count",columnName)
    
      accumulatedHistogram.join(newHistogram, Seq(variable, "")).orderBy(variable)
    }
  }
  
  private def singleVariableHistogram(df:DataFrame, column: String, buckets: Array[Double], factor: Double, columnName: String):
  DataFrame = {
    import df.sparkSession.implicits._
    val counts =
      df.select(column).map(value => value.get(0).toString.toDouble).rdd.histogram(buckets)
    histogram(df, column, buckets, counts, factor, columnName)
  }
  
  private def singleVariableHistogram(df:DataFrame, column: String, nBuckets: Int, factor: Double, columnName: String):
  (DataFrame, Array[Double]) = {
    import df.sparkSession.implicits._
    val (bucketDemarcations, counts) =
      df.select(column).map(value => value.get(0).toString.toDouble).rdd.histogram(nBuckets)
    (histogram(df, column, bucketDemarcations, counts, factor, columnName), bucketDemarcations)
  }
  
  private def histogram(
    df:DataFrame, column: String, bucketDemarcations: Array[Double], counts: Array[Long], factor: Double, columnName: String):
  DataFrame = {
    import df.sparkSession.implicits._
    import df.sparkSession.sparkContext
    val bucketBoundaries =
      bucketDemarcations.zipWithIndex.tail.foldLeft(Seq[(Double, Double)]()){
        case (previousBoundaries, (currentDemarcation, index)) =>
          val endOfLastBoundary = bucketDemarcations(index - 1)
          previousBoundaries :+ (endOfLastBoundary, currentDemarcation)
      }
    val bucketBoundariesWithMidpointAndHalfRangeWidth = bucketBoundaries.map { case (start, end) =>
      (start, end, (end+start)/2, (end-start)/2) }
    val totalCounts = counts.sum.toDouble
    val proportions = counts.map(_/totalCounts)
    val dataFrameData = bucketBoundariesWithMidpointAndHalfRangeWidth.zip(counts).zip(proportions).map {
      case (((start, end, midpoint, rangeHalfWidth), count), proportion) =>
        (start, end, midpoint, rangeHalfWidth, count, proportion)
    }
    sparkContext.parallelize(dataFrameData).toDF(column, "", "_midpoint_", "_rangeHalfWidth_", "count", columnName)
    .withColumn(columnName, lit(factor) * col(columnName))
  }
  
}
