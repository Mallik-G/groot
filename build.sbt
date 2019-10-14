name := "Groot"

version := "0.1"

scalaVersion := "2.12.9"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

// https://mvnrepository.com/artifact/io.delta/delta-core
libraryDependencies += "io.delta" %% "delta-core" % "0.3.0"

// https://mvnrepository.com/artifact/org.yaml/snakeyaml
libraryDependencies += "org.yaml" % "snakeyaml" % "1.25"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.3"