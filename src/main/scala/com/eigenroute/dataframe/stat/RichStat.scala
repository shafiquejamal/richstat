package com.eigenroute.dataframe.stat

import org.apache.spark.sql.{DataFrame, Dataset}

object RichStat {

  sealed trait TabOptions

  object TabOptions {
    case object NoOptions extends TabOptions
    case object IncludeMissing extends TabOptions
    case object ByRow extends TabOptions
    case object ByColumn extends TabOptions
    case object Percent extends TabOptions
  }

  implicit class Statistician[T](dataset: Dataset[T]) {
    
    def tab(variable: String, tabOptions: TabOptions*): Dataset[_] = {
      TabulationSpecialist.tab(dataset, variable, tabOptions: _*)
    }

    def crossTab(variable1: String, variable2: String, tabOptions: TabOptions*): DataFrame  = {
      TabulationSpecialist.crossTab(dataset, variable1, variable2, tabOptions: _*)
    }
  
    def histogram(variable: String, nBuckets: Int, crossTabVariables: String*): DataFrame = {
      HistogramSpecialist.histogram(dataset, variable, nBuckets, crossTabVariables: _*)
    }
  
    def histogramPercent(variable: String, nBuckets: Int, crossTabVariables: String*): DataFrame = {
      HistogramSpecialist.histogramPercent(dataset, variable, nBuckets, crossTabVariables: _*)
    }
  
    def histogram(variable: String, bucketDemarcations: Array[Double], crossTabVariables: String*): DataFrame = {
      HistogramSpecialist.histogram(dataset, variable, bucketDemarcations, crossTabVariables: _*)
    }
  
    def histogramPercent(variable: String, bucketDemarcations: Array[Double], crossTabVariables: String*): DataFrame = {
      HistogramSpecialist.histogramPercent(dataset, variable, bucketDemarcations, crossTabVariables: _*)
    }
    
  }
  
  implicit class StatWriter[T](dataset: Dataset[T]) {
  
    import dataset.sparkSession.implicits._
    
    def forCSV(isIncludeHeader: Boolean = true): Seq[Seq[String]] = {
      val columns = dataset.columns.toSeq
      val data = dataset.toDF().map { row =>
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
