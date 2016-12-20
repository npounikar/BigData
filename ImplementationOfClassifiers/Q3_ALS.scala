import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

// Build the recommendation model using ALS
val rank = 10
val nIterations = 15

// Load and parse the Ratings data
val ratingsData = sc.textFile("input/assignment3/ratings.dat")
val parsedRating = ratingsData.map(_.split("::") match { case Array(userId, movieId, rating, timestamp) => Rating(userId.toInt, movieId.toInt, rating.toInt) })


//splitting data into 60 to 40 percent of training and test data
val splits = parsedRating.randomSplit(Array(0.6, 0.4), seed = 11L)
val trainingData = splits(0)
val testData = splits(1)

// ALS Modelling
val alsModel = ALS.train(trainingData, rank, nIterations, 0.01)

// Evaluate the model on rating data
val parsedMovieRating = parsedRating.map { case Rating(user, movieId, rate) =>
  (user, movieId)
}

// Predicting data
val predictionData =
  alsModel.predict(parsedMovieRating).map { case Rating(user, movie, rate) =>
    ((user, movie), rate)
  }
  
// Joining data
val ratesAndPredsData = testData.map { case Rating(user, movie, rate) =>
  ((user, movie), rate)
}.join(predictionData)

// calculating MSE error and finding error
val MSError = ratesAndPredsData.map { case ((user, movie), (um1, um2)) =>
  val e = (um1 - um2)
  e * e
}.mean()

println("ALS Algo Accuracy : " + (MSError*100)+"%")
