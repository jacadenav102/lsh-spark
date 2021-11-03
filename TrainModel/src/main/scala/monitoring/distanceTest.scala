package monitoring

import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.Row
import play.api.libs.json._  


object measureDistanceTools {


    def getVectorsTable(trainedData: DataFrame,
        lshTrained: BucketedRandomProjectionLSHModel,
        executionConfig: JsValue): DataFrame = {
        
val maxDistance: Double = (executionConfig\"lsh_configuracion"\"maximun_distance").as[Double]
val idKey: String = (executionConfig\"lsh_configuracion"\"column_id").as[String]
val dataSetsSplits: Array[Dataset[Row]] = trainedData.randomSplit(Array(0.25, 0.75))
val trainData: Dataset[Row] = dataSetsSplits(0)
val testData: Dataset[Row] = dataSetsSplits(1).sample(1.0).limit(5)
        
        
        val similaratyResult: DataFrame =  (
            lshTrained.approxSimilarityJoin(trainData, testData, maxDistance, "EuclideanDistance")
            .select(
                col("datasetA.scaledFeatures").alias("features_a"), 
                col("datasetB.scaledFeatures").alias("features_b"), 
                col("EuclideanDistance")
                )
        )



        return similaratyResult
    }


    
    def getTestDistances(distancesData: DataFrame): DataFrame = {



        val vectorDistance =  (x: Vector, y: Vector) => {
            Math.sqrt(Vectors.sqdist(x, y))
        }


        val vectorDistanceUdf = udf(vectorDistance)

        val calculatedDistance: DataFrame = (
            distancesData
            .select(vectorDistanceUdf(col("features_b"), col("features_a")).alias("testDistance"), col("EuclideanDistance"))
        )


        return calculatedDistance

    } 



    def calculateMapeDifference(calculatedDistance: DataFrame): Double = {



        val absoluteDifferencePer: DataFrame = (
            calculatedDistance
            .withColumn("apeDistance", abs((col("testDistance") - col("EuclideanDistance"))/col("testDistance")))
        )
    
    
        val arrayResult: Array[Double] = absoluteDifferencePer.select(avg(col("apeDistance"))).rdd.map(r => r(0).asInstanceOf[Double]).collect()
    


        val mapeResult: Double = arrayResult(0).asInstanceOf[Double]

        return mapeResult

    }


    def getMapeTraining(trainData: DataFrame,
        lshTrained: BucketedRandomProjectionLSHModel,
        executionConfig: JsValue): Double = {





            val similaratyResult: DataFrame = getVectorsTable(trainData, lshTrained, executionConfig)
            val calculatedDistance: DataFrame = getTestDistances(similaratyResult)
            val mapeResult: Double =    calculateMapeDifference(calculatedDistance)
            
            return mapeResult



        }

}