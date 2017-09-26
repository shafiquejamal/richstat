package com.github.shafiquejamal.dataframe.stat

import com.github.shafiquejamal.dataframe.stat.RichStat.TabOptions
import com.github.shafiquejamal.dataframe.stat.RichStat.TabOptions.{ByColumn, ByRow, IncludeMissing, Percent}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}


object TabulationSpecialist {
    
    def tab(ds: Dataset[_], variable: String, tabOptions: TabOptions*): Dataset[_] = {
      val filteredDataset =
        if (tabOptions contains IncludeMissing) {
          ds.toDF
        } else {
          ds.filter(trim(col(variable)) =!= "")
        }
      val (factor, columnName) = if (tabOptions contains Percent) (100d, "percent") else (1d, "proportion")
      val total = filteredDataset.count().toDouble
      filteredDataset.groupBy(variable).count().withColumn(columnName, lit(factor) * col("count") / lit(total))
      .withColumn("count", when(col("count") === col("count"), col("count")).otherwise(null))
    }
  
    def crossTab(ds: Dataset[_], variable1: String, variable2: String, tabOptions: TabOptions*): DataFrame  = {
      val datasetToUse =
        if (tabOptions contains IncludeMissing) {
          ds
        } else {
          ds.filter(trim(col(variable1)) =!= "").filter(trim(col(variable2)) =!= "")
        }
      val factor = if (tabOptions contains Percent) 100d else 1d
      val crossTabCounts = datasetToUse.stat.crosstab(variable1, variable2).cache()
      val columnNames = crossTabCounts.columns.tail

      if (tabOptions contains ByRow) {
        crossTabRowPercent(crossTabCounts, columnNames, variable1, variable2, factor)
      } else if (tabOptions contains ByColumn) {
        crossTabColumnPercent(crossTabCounts, columnNames, variable1, variable2, factor)
      } else {
        crossTabCounts
      }
    }
    
    private def crossTabColumnPercent(
        crossTabCounts: DataFrame, columnNames: Array[String], column1: String, column2: String, factor: Double):
    DataFrame = {
      val columnSumsDF = crossTabCounts.groupBy().sum().cache()
      val columnSumsVector = columnSumsDF.first.toSeq.map(_.toString.toDouble)
      columnNames.zipWithIndex.foldLeft(crossTabCounts){ case (crossTab, (columnName, index)) =>
        val total = columnSumsVector(index)
        if (total == 0) {
          crossTab
        } else {
          crossTab.withColumn(columnName, lit(factor) * col(columnName) / lit(total)).cache()
        }
      }
    }

    private def crossTabRowPercent(
        crossTabCounts: DataFrame, columnNames: Array[String], column1: String, column2: String, factor: Double):
    DataFrame = {
      val crossTabCountsWithRowTotals =
        crossTabCounts.withColumn("sum", columnNames.map(col).reduce((c1, c2) => c1 + c2)).cache()

      columnNames.zipWithIndex.foldLeft(crossTabCountsWithRowTotals){ case (crossTab, (columnName, index)) =>
          crossTab.withColumn(columnName, lit(factor) * col(columnName) / col("sum")).cache()
      }.drop("sum")
    }
  
}
