import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.util.MLUtils

// variable 
val nClass = 10
val depth = 7
val bins = 32
val impurity = "gini"
val categoricalFeatures = Map[Int, Int]()

//load and parse data
val glassData = sc.textFile("input/assignment3/glass.data")
val parsedData = glassData.map { line =>
val parts = line.split(',')
LabeledPoint(parts(10).toDouble, Vectors.dense(parts(0).toDouble,parts(1).toDouble,parts(2).toDouble,parts(3).toDouble,parts(4).toDouble,parts(5).toDouble,parts(6).toDouble,parts(7).toDouble,parts(8).toDouble,parts(9).toDouble))
}

// splitting the training and test data by 60% and 40%
val splitData = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
val trainingData = splitData(0)
val testData = splitData(1)

// creating model
val dtModel = DecisionTree.trainClassifier(trainingData, nClass, categoricalFeatures, impurity, depth, bins)

// labeling and predicting
val labelAndPreds = testData.map { p =>
val predictionData = dtModel.predict(p.features)
(p.label, predictionData)
}

//calculating accuracy and printing
val acc =  1.0 *labelAndPreds.filter(r => r._1 == r._2).count.toDouble / testData.count
println("Decision Tree Accuracy : " + (acc*100)+"%")
