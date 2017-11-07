package com.github.shafiquejamal.dataframe.stat

import com.github.shafiquejamal.dataframe.stat.RichStat.TabOptions
import com.github.shafiquejamal.dataframe.stat.RichStat.TabOptions.{ByColumn, ByRow, IncludeMissing, Percent}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object TabulationSpecialist {
    
    def tab(df: DataFrame, variable: String, tabOptions: TabOptions*): DataFrame = {
      val filteredDataFame =
        if (tabOptions contains IncludeMissing) {
          df.toDF
        } else {
          df.filter(trim(col(variable)) =!= "")
        }
      val (factor, columnName) = if (tabOptions contains Percent) (100d, "percent") else (1d, "proportion")
      val total = filteredDataFame.count().toDouble
      filteredDataFame.groupBy(variable).count().withColumn(columnName, lit(factor) * col("count") / lit(total))
      .withColumn("count", when(col("count") === col("count"), col("count")).otherwise(null))
    }
  
    def crossTab(df: DataFrame, variable1: String, variable2: String, tabOptions: TabOptions*): DataFrame  = {
      val dataFrameToUse =
        if (tabOptions contains IncludeMissing) {
          df
        } else {
          df.filter(trim(col(variable1)) =!= "").filter(trim(col(variable2)) =!= "")
        }
      val factor = if (tabOptions contains Percent) 100d else 1d
      val crossTabCounts = dataFrameToUse.stat.crosstab(variable1, variable2).cache()
      val columnNames = crossTabCounts.columns.tail

      if (tabOptions contains ByRow) {
        crossTabRowPercent(crossTabCounts, columnNames, variable2, factor)
      } else if (tabOptions contains ByColumn) {
        crossTabColumnPercent(crossTabCounts, columnNames, variable2, factor)
      } else {
        if (tabOptions contains Percent) {
          crossTabColumnPercent(crossTabCounts, columnNames, variable2, factor)
        } else {
          crossTabCounts
        }
      }
    }
    
    private def crossTabColumnPercent(
        crossTabCounts: DataFrame, columnNames: Array[String], column2: String, factor: Double):
    DataFrame = {
      val columnSumsDF = crossTabCounts.groupBy().sum().cache()
      val columnSumsVector = columnSumsDF.first.toSeq.map(_.toString.toDouble)
      columnNames.zipWithIndex.foldLeft(crossTabCounts){ case (crossTab, (columnName, index)) =>
        val total = columnSumsVector(index)
        if (total == 0) {
          crossTab
        } else {
          crossTab.withColumn(columnName, col(columnName) * factor / total).cache()
            .withColumnRenamed(columnName, s"${column2}___$columnName")
        }
      }
    }

    private def crossTabRowPercent(
        crossTabCounts: DataFrame, columnNames: Array[String], column2: String, factor: Double):
    DataFrame = {
      val crossTabCountsWithRowTotals =
        crossTabCounts.withColumn("sum", columnNames.map(col).reduce((c1, c2) => c1 + c2)).cache()

      columnNames.zipWithIndex.foldLeft(crossTabCountsWithRowTotals){ case (crossTab, (columnName, index)) =>
          crossTab.withColumn(columnName, lit(factor) * col(columnName) / col("sum")).cache()
            .withColumnRenamed(columnName, s"${column2}___$columnName")
      }.drop("sum")
    }
  
}
