import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

// variables needed for kMeans
val nClusters = 10
val nIterations = 15

//reading and parsing the input file
val itemusermatData = sc.textFile("input/assignment3/itemusermat")
val parsedItemusermatData = itemusermatData.map(s => Vectors.dense(s.split(' ').drop(1).map(_.toDouble))).cache()

// modelling the parsed input data using kMeans
val kMeansModel = KMeans.train(parsedItemusermatData, nClusters, nIterations)

//prediction
val predictionModel = itemusermatData.map{ line =>	
val parts = line.split(' ')	
(parts(0), kMeansModel.predict(Vectors.dense(parts.tail.map(_.toDouble))))
}

// resding and parsing movies file
val moviesData = sc.textFile("input/assignment3/movies.dat")
val  parsedMoviesData = moviesData.map{ line=>	
val parts = line.split("::")
(parts(0),(parts(1)+" , "+parts(2)))
}

//joining data
val joinedData = predictionModel.join(parsedMoviesData)
val shuffledJoinedData= joinedData.map(s=>(s._2._1,(s._1,s._2._2)))

// grouping and taking the 5 entries
val result = shuffledJoinedData.groupByKey().map(g=>(g._1,g._2.toList)).map(s=>(s._1,s._2.take(5))).collect

//printing output from every cluster
result.foreach(p=>println("Cluster(ID)- "+ p._1+"  :  "+p._2.mkString(":::")))
