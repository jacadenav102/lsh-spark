package similarityCalculation
    

import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import scala.util.matching.Regex
import play.api.libs.json._  


object similarityTools {



    def getSimilarityTable(
        spark: SparkSession,
        executionConfig: JsValue,
        lshModel: BucketedRandomProjectionLSHModel, 
        transformationPipeline: PipelineModel,
        executionKind: String) : Unit = {
        
        if (executionKind == "automatic") {

            val idKey: String = (executionConfig\"lsh_configuracion"\"column_id").as[String]
            val trainTable: String =  (executionConfig\"lsh_configuracion"\"queries"\ "train_table").as[String]
            val originalVariables: String = (executionConfig\"lsh_configuracion"\"variables_tocompare").as[String]
            val queryCompare: String = "SELECT " +  originalVariables + " FROM " + (executionConfig\"lsh_configuracion"\ "automatic_comprable_table").as[String]
            val queryOriginal: String = "SELECT " + originalVariables + " FROM " + (executionConfig\"lsh_configuracion"\ "not_anomaly_table").as[String]
            val toCompareData: DataFrame = spark.sql(queryCompare)
            val originalData: DataFrame = spark.sql(queryOriginal)
            val maxDistance: Double = (executionConfig\"lsh_configuracion"\"maximun_distance").as[Double]
            val toCompareTransformed: DataFrame = transformationPipeline.transform(toCompareData).select(idKey, "scaledFeatures")
            val trasnformedOriginal: DataFrame = transformationPipeline.transform(originalData).select(idKey, "scaledFeatures")
            val similaratyResult: DataFrame =  (
                lshModel.approxSimilarityJoin(trasnformedOriginal, toCompareTransformed, maxDistance, "EuclideanDistance")
                .select(
                    col("datasetA." + idKey).alias(idKey + "A"), 
                    col("datasetB." + idKey).alias(idKey + "B"), 
                    col("EuclideanDistance")
                )
            )

        (
            similaratyResult
            .write
            .format("hive")
            .option("fileformat", "parquet")
            .mode("overwrite")
            .saveAsTable((executionConfig\"lsh_configuracion"\"automatic_result").as[String])

        )


        } else if (executionKind == "similarity") {

            val toCompareData = spark.sql((executionConfig\"lsh_configuracion"\"queries"\ "predict_table").as[String])
            val originalData: DataFrame = spark.sql((executionConfig\"lsh_configuracion"\"queries"\ "train_table").as[String])
            val idKey: String = (executionConfig\"lsh_configuracion"\"column_id").as[String]
            val maxDistance: Double = (executionConfig\"lsh_configuracion"\"maximun_distance").as[Double]
            val toCompareTransformed: DataFrame = transformationPipeline.transform(toCompareData).select(idKey, "scaledFeatures")
            val trasnformedOriginal: DataFrame = transformationPipeline.transform(originalData).select(idKey, "scaledFeatures")
            val similaratyResult: DataFrame =  (
                lshModel.approxSimilarityJoin(trasnformedOriginal, toCompareTransformed, maxDistance, "EuclideanDistance")
                .select(
                    col("datasetA." + idKey).alias(idKey + "A"), 
                    col("datasetB." + idKey).alias(idKey + "B"), 
                    col("EuclideanDistance")
                )
            )


            (

            similaratyResult
            .write
            .format("hive")
            .option("fileformat", "parquet")
            .mode("overwrite")
            .saveAsTable((executionConfig\"lsh_configuracion"\"approximity_result").as[String])

        )







        }
        

        spark.close()

    }
}
