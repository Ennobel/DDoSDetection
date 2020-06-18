name := "ddos-online-detector"
version := "0.1"
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.2"
libraryDependencies += "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.3.2"
 
  
assemblyMergeStrategy in assembly := {
  {
    case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}