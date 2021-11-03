package gettingVector

import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.types._
import play.api.libs.json._  

object neighborsTools {
    
    def getNeigbornsTable(spark: SparkSession, executionConfig: JsValue, lshModel: BucketedRandomProjectionLSHModel, transformationPipeline: PipelineModel): Unit = {
        val idKey: String = (executionConfig\"lsh_configuracion"\"column_id").as[String]
        val vectorThreshold = spark.sql("SELECT * FROM " + (executionConfig\ "lsh_configuracion" \"vector_table").as[String])
        val originalData = (
        spark.sql((executionConfig\"lsh_configuracion"\"queries"\ "train_table").as[String])
        )
        val convertedVector = transformationPipeline.transform(vectorThreshold).select("scaledFeatures")
        val convertedOriginal = transformationPipeline.transform(originalData).select(idKey, "scaledFeatures")
        val keyVector: Vector = (
            convertedVector.rdd.take(1)(0)(0).asInstanceOf[Vector]
        )


        val neigbortResult = (
            lshModel
            .approxNearestNeighbors(convertedOriginal, keyVector, (executionConfig\"lsh_configuracion"\"number_neighbors").as[Int])
            .select(col("key_mv"), col("distCol"))
        )


        (neigbortResult.write 
            .format("hive")
            .option("fileformat", "parquet")
            .mode("overwrite")
            .saveAsTable((executionConfig\"lsh_configuracion"\"neighnord_result").as[String]))

    }

}