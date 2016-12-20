import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

// loading and parsing data
val glassData = sc.textFile("input/assignment3/glass.data")
val parsedData = glassData.map { line =>
	val parts = line.split(',')
  	LabeledPoint(parts(10).toDouble, Vectors.dense(parts(0).toDouble,parts(1).toDouble,parts(2).toDouble,parts(3).toDouble,parts(4).toDouble,parts(5).toDouble,parts(6).toDouble,parts(7).toDouble,parts(8).toDouble,parts(9).toDouble))
}

//splitting the data into training and tests data
val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
val trainingData = splits(0)
val testData = splits(1)

//modelling data
val nbModel = NaiveBayes.train(trainingData, lambda = 1.0)

//prediction
val predictionAndLabel = testData.map(pl => (nbModel.predict(pl.features), pl.label))

// calculating accuracy
val acc = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testData.count()

println("Naive Bayes Algorithm Accuracy : " + (acc*100)+"%")
