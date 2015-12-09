# statements-generator

Project generates fake bank transactions data for testing money laundering detection algorithms

# To build

You will need sbt tool to be installed.

sbt assembly
Job jar will be located in target/scala-2.10/statements-generator.jar

# To run

To run locally with spark, in the spark distro folder:

bin/spark-submit.cmd --master "local[*]" /statements-generator.jar
