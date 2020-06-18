import extract._
import java.sql.Timestamp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.{VectorAssembler, VectorSizeHint}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_json, from_unixtime, 
                  to_utc_timestamp, lit, to_timestamp}

import com.mongodb.spark.config.WriteConfig
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector

import scala.collection.mutable
import org.bson.Document
import scala.collection.JavaConverters._

case class ConnCountObj(
  timestamp: Timestamp,
  uid: String,
  idOrigH: String,
  idOrigP: Integer,
  idRespH: String,
  idRespP: Integer,
  proto: String,
  service: String,
  duration: Double,
  orig_bytes: Integer,
  resp_bytes: Integer,
  connState: String,
  localOrig: Boolean,
  localResp: Boolean,
  missedBytes: Integer,
  history: String,
  origPkts: Integer,
  origIpBytes: Integer,
  respPkts: Integer,
  respIpBytes: Integer
)

case class ClassificationObj(
  timestamp: Timestamp,
  uid: String,
  idOrigH: String,
  idOrigP: Integer,
  idRespH: String,
  idRespP: Integer,
  label: String
)

object Stream{
	def main(args: Array[String]): Unit = {
		new Stream("192.168.0.20:9092").go()
	}
}

class Stream(brokers: String){
	def go(): Unit = {
     val conf = new SparkConf()
      .setAppName("DDoS-stream-detector")
  		.setMaster("spark://PC:7077")
      .set("spark.driver.allowMultipleContexts", "true")
      
		val spark = SparkSession.builder()
      .config(conf)
		  .getOrCreate()
		  
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    
		val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", "bisa")
      .load()

		val connSchema : StructType = StructType(
      Seq(StructField ("conn", StructType(
        Seq(
          StructField("ts",DoubleType,true),
          StructField("uid", StringType, true),
          StructField("id.orig_h", StringType, true),
          StructField("id.orig_p", IntegerType, true),
          StructField("id.resp_h", StringType, true),
          StructField("id.resp_p", IntegerType, true),
          StructField("proto", StringType, true),
          StructField("service", StringType, true),
          StructField("duration", DoubleType, true),
          StructField("orig_bytes", IntegerType, true),
          StructField("resp_bytes", IntegerType, true),
          StructField("conn_state", StringType, true),
          StructField("local_orig", BooleanType, true),
          StructField("local_resp", BooleanType, true),
          StructField("missed_bytes", IntegerType, true),
          StructField("history", StringType, true),
          StructField("orig_pkts", IntegerType, true),
          StructField("orig_ip_bytes", IntegerType, true),
          StructField("resp_pkts", IntegerType, true),
          StructField("resp_ip_bytes", IntegerType, true),
          StructField("tunnel_parents", ArrayType(StringType, true))
        )
      )))
    )
		
		val parsedLogData = df.select(
			col("value").cast(StringType).as("col")
        ).select(from_json(
      col("col"), connSchema).getField("conn")
        .alias("conn")
      ).select("conn.*")

		val parsedRawDf = parsedLogData.withColumn(
			 "ts",to_utc_timestamp(
				  from_unixtime(col("ts")),"GMT"
			  ).alias("ts").cast(TimestampType))

		val connDf = parsedRawDf
      .map((r:Row) => new ConnCountObj(
        r.getAs[Timestamp](0), r.getAs[String](1),
        r.getAs[String](2), r.getAs[Integer](3),
        r.getAs[String](4), r.getAs[Integer](5),
        r.getAs[String](6), r.getAs[String](7),
        r.getAs[Double](8), r.getAs[Integer](9),
        r.getAs[Integer](10), r.getAs[String](11),
        r.getAs[Boolean](12), r.getAs[Boolean](13),
        r.getAs[Integer](14), r.getAs[String](15),
        r.getAs[Integer](16), r.getAs[Integer](17),
        r.getAs[Integer](18), r.getAs[Integer](19)
      ))

      // parsedRawDf.select("*").writeStream
      // .outputMode("append")
      // .format("console")
      // .start()

    val PX  = connDf.withColumn("PX", FeatureExtraction.px(col("origPkts").cast("int"), col("respPkts").cast("int")))
    val NNP = PX.withColumn("NNP", FeatureExtraction.nnp(col("PX").cast("int")))
    val NSP = NNP.withColumn("NSP", FeatureExtraction.nsp(col("PX").cast("int")))
    val PSP = NSP.withColumn("PSP", FeatureExtraction.psp(col("NSP").cast("double"), col("PX").cast("double")))
    val IOPR = PSP.withColumn("IOPR", FeatureExtraction.iopr(col("origPkts").cast("int"), col("respPkts").cast("int")))
    val Reconnect = IOPR.withColumn("Reconnect", lit(0))
    val FPS = Reconnect.withColumn("FPS", FeatureExtraction.fps(col("origIpBytes").cast("int"), col("respPkts").cast("int")))
    val TBT = FPS.withColumn("TBT", FeatureExtraction.tbt(col("origIpBytes").cast("int"), col("respIpBytes").cast("int")))
    val APL = TBT.withColumn("APL", FeatureExtraction.apl(col("PX").cast("int"), col("origIpBytes").cast("int"), col("respIpBytes").cast("int")))
    val PPS = APL.withColumn("PPS", FeatureExtraction.pps(col("duration").cast("double"), col("FPS").cast("double")))
    val transDF = PPS.withColumn("missedBytesTmp", lit(0))

    val classificationDf = transDF

    // classificationDf.printSchema()

    val connModel = PipelineModel.load("hdfs://localhost:9000/dataset/dataset-model")

    val assembler = new VectorAssembler()
        .setInputCols(Array(
            "duration",
            "orig_bytes",
            "resp_bytes",
            "missedBytesTmp",
            "origPkts",
            "origIpBytes",
            "respPkts",
            "respIpBytes",
            "PX",
            "NNP",
            "NSP",
            "PSP",
            "IOPR",
            "Reconnect",
            "FPS",
            "TBT",
            "APL",
            "PPS"
          ))
        .setOutputCol("features")

        val filtered  = classificationDf.filter(
          $"idOrigP".isNotNull &&
          $"idRespP".isNotNull &&
          $"duration".isNotNull &&
          $"orig_bytes".isNotNull &&
          $"resp_bytes".isNotNull &&
          $"missedBytes".isNotNull &&
          $"origPkts".isNotNull &&
          $"origIpBytes".isNotNull &&
          $"respPkts".isNotNull &&
          $"respIpBytes".isNotNull &&
          $"PX".isNotNull &&
          $"NNP".isNotNull &&
          $"NSP".isNotNull &&
          $"PSP".isNotNull &&
          $"IOPR".isNotNull &&
          $"Reconnect".isNotNull &&
          $"FPS".isNotNull &&
          $"TBT".isNotNull &&
          $"APL".isNotNull &&
          $"PPS".isNotNull
        )

        val output = assembler.transform(filtered)

        // output.writeStream
        // .format("console")
        // .outputMode("append")
        // .start()

        val classifier = connModel.transform(output)

        val outputstream = classifier.select(
          col("timestamp"),
          col("uid"),
          col("idOrigH"),
          col("idOrigP"),
          col("idRespH"),
          col("idRespP"),
          col("predictedLabel").as("label")
        )

        outputstream.writeStream
        .outputMode("append")
        .format("console")
        .start()

        val outputtabel = outputstream
          .map((r:Row) => ClassificationObj(
            r.getAs[Timestamp](0),
            r.getAs[String](1),
            r.getAs[String](2),
            r.getAs[Integer](3),
            r.getAs[String](4),
            r.getAs[Integer](5),
            r.getAs[String](6)
          ))  

        val query = outputtabel.writeStream.outputMode("append")
        .foreach(new ForeachWriter[ClassificationObj]{

           val writeConfig: WriteConfig = WriteConfig(
             Map("uri" -> "mongodb://localhost/DDoS.report")
            )
          var mongoConnector: MongoConnector = _
          var ConnCounts: mutable.ArrayBuffer[ClassificationObj] = _

          override def process(value: ClassificationObj): Unit = {
            ConnCounts.append(value)
          }
          override def close(errorOrNull: Throwable): Unit = {
            if (ConnCounts.nonEmpty) {
              mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
                collection.insertMany(ConnCounts.map(sc => {
                  var doc = new Document()
                  doc.put("ts", sc.timestamp)
                  doc.put("uid", sc.uid)
                  doc.put("orig_h", sc.idOrigH)
                  doc.put("orig_p", sc.idOrigP)
                  doc.put("resp_h", sc.idRespH)
                  doc.put("resp_p", sc.idRespP)
                  doc.put("label", sc.label)
                  doc
                }).asJava)
              })
            }
          }
          
          override def open(partitionId: Long, version: Long): Boolean = {
            mongoConnector = MongoConnector(writeConfig.asOptions)
            ConnCounts = new mutable.ArrayBuffer[ClassificationObj]()
            true
          }
        }).start()
        
        query.awaitTermination()
		spark.streams.awaitAnyTermination()
	}
}

