
import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH

import monitoring.measureDistanceTools.getMapeTraining
import monitoring.monitoringTool.createMonitorTable
import train.finalPipeline.getFinalPipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import train.modelTtrain.getLSHStage
import org.apache.spark.ml.Pipeline
import play.api.libs.json._  
import java.nio.file.Paths
import scala.io.Source




object lshTrain extends App {

    val spark = (
        SparkSession.builder().appName("Fricciones App")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.sql.broadcastTimeout", "-1")
        .getOrCreate()
    )
    val configPath : String = Paths.get("./sparky_bc/config.json").toAbsolutePath().toString()
    val configFile: String = Source.fromFile(configPath).getLines.mkString
    val executionConfig: JsValue = Json.parse(configFile)
    val selectedData = spark.sql((executionConfig\"lsh_configuracion"\"queries"\ "train_table").as[String])
    val finalPipeline: Pipeline = getFinalPipeline(dataModel = selectedData, columnID = (executionConfig\"lsh_configuracion" \"column_id").as[String])
    val trainedPipeline: PipelineModel = finalPipeline.fit(selectedData)
    val transformedData = trainedPipeline.transform(selectedData).select("scaledFeatures", (executionConfig \"lsh_configuracion"\"column_id").as[String])
    transformedData.cache()
    val lshModel: BucketedRandomProjectionLSH = (
        getLSHStage(
            BucketLength = (executionConfig\"lsh_configuracion"\ "bucket_lenth").as[Double], 
            NumHashTables = (executionConfig\"lsh_configuracion"\"number_hash_tables").as[Int])
    )
    val lshTrained: BucketedRandomProjectionLSHModel = lshModel.fit(transformedData)

    (

        lshTrained
        .write
        .overwrite()
        .save((executionConfig\"lsh_configuracion"\"model_path").as[String])


    )
    (
        trainedPipeline
        .write
        .overwrite()
        .save((executionConfig\"lsh_configuracion"\"transformation_path").as[String])
    )


    val mapeDistance: Double =  getMapeTraining(transformedData, lshTrained, executionConfig)

    println(mapeDistance)
    transformedData.unpersist()
    val monitorTraining: DataFrame = (
        createMonitorTable(spark, "fricciones_app", "LSH", 10, "MAPE", Some(mapeDistance), 1)
    )



    (
        monitorTraining
        .write
        .format("hive")
        .option("fileformat", "parquet")
        .mode("append")
        .saveAsTable((executionConfig\"lsh_configuracion"\"monitor_table").as[String])
    )






}


