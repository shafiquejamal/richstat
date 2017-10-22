import Dependencies._
import ReleaseTransformations._

releaseCrossBuild := false
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommand("publishSigned"),
  setNextVersion,
  commitNextVersion,
  releaseStepCommand("sonatypeReleaseAll"),
  pushChanges
)

val sparkVersion = "2.1.0"
val sparkTestingVersion = sparkVersion + "_0.7.4"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.github.shafiquejamal",
      scalaVersion := "2.11.11",
      version      := "0.0.2"
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

useGpg := true
pomIncludeRepository := { _ => false }

licenses := Seq("BSD-style" -> url("http://www.opensource.org/licenses/bsd-license.php"))

homepage := Some(url("https://github.com/shafiquejamal/richstat"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/shafiquejamal/richstat"),
    "scm:git@github.com:shafiquejamal/richstat.git"
  )
)

developers := List(
  Developer(
    id    = "shafiquejamal",
    name  = "Shafique Jamal",
    email = "admin@eigenroute.com",
    url   = url("http://eigenroute.com")
  )
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

