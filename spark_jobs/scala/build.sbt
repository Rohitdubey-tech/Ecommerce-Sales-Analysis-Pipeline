name := "olist-etl-scala"

version := "0.1.0"

scalaVersion := "2.13.18"

val sparkVersion = "4.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

