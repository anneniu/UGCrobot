name := "dispatcher"

version := "1.0"

scalaVersion := "2.10.4"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2" exclude("org.apache.spark", "spark-streaming_2.10")

libraryDependencies += "org.json" % "json" % "20140107"
