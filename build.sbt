import Dependencies._

val sparkVersion = "2.1.0"
val sparkTestingVersion = sparkVersion + "_0.7.4"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.github.shafiquejamal",
      scalaVersion := "2.11.11",
      version      := "0.0.1"
    )),
    name := "RichStat",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-hive" % sparkVersion,
      "com.holdenkarau" %% "spark-testing-base" % sparkTestingVersion % Test,
      scalaTest % Test
    )
  )