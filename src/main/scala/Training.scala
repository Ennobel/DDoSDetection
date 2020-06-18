import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator


object Training {
  def main(args: Array[String]): Unit = {
	  new Training("localhost:9092").process()
	}
}

class Training(broker: String){
  def process(): Unit = {
		val spark = SparkSession.builder()
  		.appName("ddos-training-dataset")
  		.master("spark://PC:7077")
		  .getOrCreate()
		  
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.read.format("libsvm").
               load("hdfs://localhost:9000/dataset/dataset.data")

    val labelIndexer = new StringIndexer().setInputCol("label")
                      .setOutputCol("indexedLabel").fit(data)

    val featureIndexer = new VectorIndexer().setInputCol("features")
                        .setOutputCol("indexedFeatures").setMaxCategories(4)
                        .fit(data)
    
    val Array(trainingData, testData) = data.randomSplit(Array(0.9, 0.1))

    val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel")
            .setFeaturesCol("indexedFeatures")
    
    val labelConverter = new IndexToString().setInputCol("prediction")
                        .setOutputCol("predictedLabel")
                        .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
                  .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)
    predictions.select("predictedLabel", "label", "features").show(100)

    val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"akurasi = ${(accuracy)}")
    println(s"Test Error = ${(1.0 - accuracy)}")

    model.write.overwrite().save("hdfs://localhost:9000/dataset/dataset-model")

  }
}
