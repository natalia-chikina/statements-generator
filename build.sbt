name := "statements-generator"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "1.4.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0",
  "com.typesafe" % "config" % "1.3.0"
)

mainClass := Some("com.poc.statement.Generator")

assemblyJarName in assembly := "statements-generator.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}