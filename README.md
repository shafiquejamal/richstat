# richstat
Library for computing tables (tabulations and cross-tabulations) and histogram data in a format amenable for plotting

## Installation

To add to your SBT project, add the following to libraryDependencies:

```
"com.github.shafiquejamal" % "richstat_2.11" % "0.0.4"
```

## Examples - Tabulations

### Simple tabulation
```
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import com.github.shafiquejamal.dataframe.stat.RichStat.TabOptions._
import com.github.shafiquejamal.dataframe.stat.RichStat._

implicit val spark: SparkSession =
  SparkSession
  .builder()
  .enableHiveSupport()
  .appName("Some project")
  .config("spark.master", "local")
  .getOrCreate()
val sc: SparkContext = spark.sparkContext
import spark.implicits._

val guts =
  Seq(("male", 5, "US"), ("female", 4, "CA"), ("male", 4, "US"), ("female", 8, "CA"), ("male", 15, "CA"),
      ("male", 8, "CA"), ("male", 2, "CA"), ("female", 6, "US"), ("", 20, "US"), ("female", 3, null))
val header = Seq("gender", "age", "country")
val data = sc.parallelize(guts).toDF(header: _*)

data.tab("gender").show()
```
this will give you a tabulation of counts:
```
+------+-----+------------------+
|gender|count|        proportion|
+------+-----+------------------+
|female|    4|0.4444444444444444|
|  male|    5|0.5555555555555556|
+------+-----+------------------+
```
Want percent instead of proportion? Try this:
```
data.tab("gender", Percent).show()
```
...and you'll get:
```
+------+-----+-----------------+
|gender|count|          percent|
+------+-----+-----------------+
|female|    4|44.44444444444444|
|  male|    5|55.55555555555556|
+------+-----+-----------------+
```
By default, rows in the DataFrame whose value for the variable of interest is null or emty are omitted. But you can include them by specifying the `IncludeMissing` option:
```
data.crossTab("gender", "country", IncludeMissing).show()
```
...and voila:
```
+--------------+------------+------------+--------------+
|gender_country|country___CA|country___US|country___null|
+--------------+------------+------------+--------------+
|          male|           3|           2|             0|
|        female|           2|           1|             1|
|              |           0|           1|             0|
+--------------+------------+------------+--------------+
```

### Cross-tabulation (contingency table)
```
data.crossTab("gender", "country", ByColumn).show()
```
...gives a break down of gender composition by country, where the columns sum to unity:
```
+--------------+------------+------------------+
|gender_country|country___CA|      country___US|
+--------------+------------+------------------+
|          male|         0.6|0.6666666666666666|
|        female|         0.4|0.3333333333333333|
+--------------+------------+------------------+
```
You can get this in percent too:
```
data.crossTab("gender", "country", ByColumn, Percent).show()
```
...yields:
```
+--------------+------------+------------------+
|gender_country|country___CA|      country___US|
+--------------+------------+------------------+
|          male|        60.0| 66.66666666666667|
|        female|        40.0|33.333333333333336|
+--------------+------------+------------------+
```
What about row percent, i.e. the rows sum to unity or percent? Try this:
```
data.crossTab("gender", "country", ByRow).show()
data.crossTab("gender", "country", ByRow, Percent).show()
```
...and the results (respectively):
```
+--------------+------------------+------------------+
|gender_country|      country___CA|      country___US|
+--------------+------------------+------------------+
|          male|               0.6|               0.4|
|        female|0.6666666666666666|0.3333333333333333|
+--------------+------------------+------------------+

+--------------+-----------------+------------------+
|gender_country|     country___CA|      country___US|
+--------------+-----------------+------------------+
|          male|             60.0|              40.0|
|        female|66.66666666666667|33.333333333333336|
+--------------+-----------------+------------------+
```

## Examples - Producing data for histograms

Use this test data instead:
```
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
```
run the following commands:
```
data.histogram("age", 5).show()
data.histogramPercent("age", 5).show()
data.crossTab("gender", "country", ByColumn, Percent).toDF.show()
```
to get the following output, respectively:
```
+----+----+----------+----------------+-----+-------------------+
| age|    |_midpoint_|_rangeHalfWidth_|count|         proportion|
+----+----+----------+----------------+-----+-------------------+
| 0.0| 4.0|       2.0|             2.0|   42|0.18502202643171806|
| 4.0| 8.0|       6.0|             2.0|   51|0.22466960352422907|
| 8.0|12.0|      10.0|             2.0|   18|0.07929515418502203|
|12.0|16.0|      14.0|             2.0|   38|0.16740088105726872|
|16.0|20.0|      18.0|             2.0|   78| 0.3436123348017621|
+----+----+----------+----------------+-----+-------------------+

+----+----+----------+----------------+-----+------------------+
| age|    |_midpoint_|_rangeHalfWidth_|count|           percent|
+----+----+----------+----------------+-----+------------------+
| 0.0| 4.0|       2.0|             2.0|   42|18.502202643171806|
| 4.0| 8.0|       6.0|             2.0|   51| 22.46696035242291|
| 8.0|12.0|      10.0|             2.0|   18| 7.929515418502203|
|12.0|16.0|      14.0|             2.0|   38|16.740088105726873|
|16.0|20.0|      18.0|             2.0|   78| 34.36123348017621|
+----+----+----------+----------------+-----+------------------+

+--------------+-----------------+------------+
|gender_country|     country___CA|country___US|
+--------------+-----------------+------------+
|          MALE|33.87096774193548|       100.0|
|        FEMALE|66.12903225806451|         0.0|
+--------------+-----------------+------------+
```
You can also get a breakdown of the variable of interest by multiple categorical variables:
```
data.histogram("age", 5, "country", "gender").show()
data.histogramPercent("age", 5, "country", "gender").show()
```
the output is:
```
+----+----+----------+----------------+-----+-------------------+-------------------+--------------------+---------------------------+-------------------------+-------------------+-------------------+
| age|    |_midpoint_|_rangeHalfWidth_|count|         proportion|       country___CA|        country___US|country___(country missing)|gender___(gender missing)|      gender___MALE|    gender___FEMALE|
+----+----+----------+----------------+-----+-------------------+-------------------+--------------------+---------------------------+-------------------------+-------------------+-------------------+
| 0.0| 4.0|       2.0|             2.0|   42|0.18502202643171806| 0.1565217391304348|  0.2777777777777778|        0.18421052631578946|      0.17796610169491525|0.44680851063829785|                0.0|
| 4.0| 8.0|       6.0|             2.0|   51|0.22466960352422907| 0.2956521739130435|                 0.0|         0.2236842105263158|        0.211864406779661| 0.5531914893617021|                0.0|
| 8.0|12.0|      10.0|             2.0|   18|0.07929515418502203|0.09565217391304348|0.027777777777777776|        0.07894736842105263|      0.11864406779661017|                0.0|0.06451612903225806|
|12.0|16.0|      14.0|             2.0|   38|0.16740088105726872|0.16521739130434782| 0.16666666666666666|        0.17105263157894737|      0.16101694915254236|                0.0| 0.3064516129032258|
|16.0|20.0|      18.0|             2.0|   78| 0.3436123348017621|0.28695652173913044|  0.5277777777777778|        0.34210526315789475|       0.3305084745762712|                0.0| 0.6290322580645161|
+----+----+----------+----------------+-----+-------------------+-------------------+--------------------+---------------------------+-------------------------+-------------------+-------------------+

+----+----+----------+----------------+-----+------------------+------------------+------------------+---------------------------+-------------------------+------------------+------------------+
| age|    |_midpoint_|_rangeHalfWidth_|count|           percent|      country___CA|      country___US|country___(country missing)|gender___(gender missing)|     gender___MALE|   gender___FEMALE|
+----+----+----------+----------------+-----+------------------+------------------+------------------+---------------------------+-------------------------+------------------+------------------+
| 0.0| 4.0|       2.0|             2.0|   42|18.502202643171806| 15.65217391304348| 27.77777777777778|         18.421052631578945|       17.796610169491526|44.680851063829785|               0.0|
| 4.0| 8.0|       6.0|             2.0|   51| 22.46696035242291|29.565217391304348|               0.0|          22.36842105263158|         21.1864406779661|55.319148936170215|               0.0|
| 8.0|12.0|      10.0|             2.0|   18| 7.929515418502203| 9.565217391304348|2.7777777777777777|          7.894736842105263|       11.864406779661017|               0.0| 6.451612903225806|
|12.0|16.0|      14.0|             2.0|   38|16.740088105726873| 16.52173913043478|16.666666666666664|         17.105263157894736|       16.101694915254235|               0.0| 30.64516129032258|
|16.0|20.0|      18.0|             2.0|   78| 34.36123348017621|28.695652173913043| 52.77777777777778|          34.21052631578947|        33.05084745762712|               0.0|62.903225806451616|
+----+----+----------+----------------+-----+------------------+------------------+------------------+---------------------------+-------------------------+------------------+------------------+```

