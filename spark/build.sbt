name := "spark-samples"

organization := "com.streaming"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-streaming" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0"
//  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"
)

resolvers += Resolver.mavenLocal