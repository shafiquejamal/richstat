package com.github.shafiquejamal.dataframe.stat

import org.apache.spark.sql.DataFrame

object RichStat {

  sealed trait TabOptions

  object TabOptions {
    case object NoOptions extends TabOptions
    case object IncludeMissing extends TabOptions
    case object ByRow extends TabOptions
    case object ByColumn extends TabOptions
    case object Percent extends TabOptions
  }

  implicit class Statistician(df: DataFrame) {
    
    def tab(variable: String, tabOptions: TabOptions*): DataFrame = {
      TabulationSpecialist.tab(df, variable, tabOptions: _*)
    }

    def crossTab(variable1: String, variable2: String, tabOptions: TabOptions*): DataFrame  = {
      TabulationSpecialist.crossTab(df, variable1, variable2, tabOptions: _*)
    }
  
    def histogram(variable: String, nBuckets: Int, crossTabVariables: String*): DataFrame = {
      HistogramSpecialist.histogram(df, variable, nBuckets, crossTabVariables: _*)
    }
  
    def histogramPercent(variable: String, nBuckets: Int, crossTabVariables: String*): DataFrame = {
      HistogramSpecialist.histogramPercent(df, variable, nBuckets, crossTabVariables: _*)
    }
  
    def histogram(variable: String, bucketDemarcations: Array[Double], crossTabVariables: String*): DataFrame = {
      HistogramSpecialist.histogram(df, variable, bucketDemarcations, crossTabVariables: _*)
    }
  
    def histogramPercent(variable: String, bucketDemarcations: Array[Double], crossTabVariables: String*): DataFrame = {
      HistogramSpecialist.histogramPercent(df, variable, bucketDemarcations, crossTabVariables: _*)
    }

    def weightedHistogram(variable: String, weightVariable: String, bucketDemarcations: Seq[Double]): Option[DataFrame] = {
      WeightedHistogram.maybeHistogram(df, variable, weightVariable, bucketDemarcations)
    }

    def weightedHistogram(variable: String, weightVariable: String, nBuckets: Int): Option[DataFrame] = {
      WeightedHistogram.maybeHistogram(df, variable, weightVariable, nBuckets)
    }
  }
  
  implicit class StatWriter[T](dataFrame: DataFrame) {
  
    import dataFrame.sparkSession.implicits._
    
    def forCSV(isIncludeHeader: Boolean = true): Seq[Seq[String]] = {
      val columns = dataFrame.columns.toSeq
      val data = dataFrame.toDF().map { row =>
        columns.zipWithIndex.map { case (_, index) => Option(row.get(index)).fold(""){ _.toString } }
      }.collect().toVector
      if (isIncludeHeader) columns +: data else data
    }

    def writeOut(
        writeRow: Seq[String] => Unit, isIncludeHeader: Boolean = true, isFollowWithBlank: Boolean = true): Unit = {
      forCSV(isIncludeHeader).foreach(writeRow)
      if (isFollowWithBlank) writeRow(Seq(""))
    }
  }

}
