package com.github.shafiquejamal.dataframe.stat

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object DataFrameUtils {

  def generateCategoricalVariableFrom(
    source: Column, groups: Seq[Double], preText: String = "", midText: String = " to ", postText: String = "",
    replaceDecimalWith: String = "p"
  ): Column = {

    def grouper(groups: Seq[Double], previousGroupDemarcation: Double): Column = {
      if (groups.isEmpty) {
        lit("__other__")
      } else {
        val currentDemarcation = groups.head
        val List(lowerValue, upperValue) =
          List(previousGroupDemarcation, currentDemarcation).map(_.toString.replaceAll("""\.""", replaceDecimalWith))
        val columnValue = s"$preText$lowerValue$midText$upperValue$postText"
        if (groups.tail.nonEmpty) {
          when(source >= previousGroupDemarcation && source < currentDemarcation, lit(columnValue))
            .otherwise(grouper(groups.tail, currentDemarcation))
        } else {
          when(source >= previousGroupDemarcation && source <= currentDemarcation, lit(columnValue))
            .otherwise(grouper(groups.tail, currentDemarcation))
        }
      }
    }

    grouper(groups.tail, groups.head)
  }

  def regroupCategoricalContainsExactly(
      existingCol: Column,  mapping: Map[String, Seq[String]], valueForNonMatch: String = "Other"): Column = {

    def grouper(keys: Iterable[String]): Column = {
      if (keys.isEmpty) {
        lit(valueForNonMatch)
      } else {
        val key = keys.head
        val keyLowerCase = key.toLowerCase
        val candidatesForMatch = mapping(key).map(_.toLowerCase)
        val existingColumnValue = lower(existingCol)
        when(existingColumnValue.isin(candidatesForMatch:_ *) || existingColumnValue === keyLowerCase, lit(key))
          .otherwise(grouper(keys.tail))
      }
    }

    grouper(mapping.keys)
  }

  def regroupCategoricalContainsWithin[T](
    existingCol: Column,  mapping: Map[T, Seq[String]], valueForNonMatch: Column): Column = {

    def containsAnyOf(remainingItemsToCheck: Seq[String]): Column = {
      if (remainingItemsToCheck.isEmpty) {
        lit(false)
      } else {
        val itemToCheck = remainingItemsToCheck.head
        locate(itemToCheck, lower(existingCol)) > 0 || containsAnyOf(remainingItemsToCheck.tail)
      }
    }

    def grouper(keys: Iterable[T]): Column = {
      if (keys.isEmpty) {
        lit(valueForNonMatch)
      } else {
        val key = keys.head
        val candidatesForMatch = mapping(key).map(_.toLowerCase)
        val existingColumnValue = lower(existingCol)
        when(containsAnyOf(candidatesForMatch) || existingColumnValue === lower(lit(key)), lit(key))
        .otherwise(grouper(keys.tail))
      }
    }

    grouper(mapping.keys)
  }

}
