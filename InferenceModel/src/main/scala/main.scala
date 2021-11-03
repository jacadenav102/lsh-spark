import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel
import similarityCalculation.similarityTools.getSimilarityTable
import gettingVector.neighborsTools.getNeigbornsTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import play.api.libs.json._  
import java.nio.file.Paths
import scala.io.Source



object lshPredict extends App {

    val spark = (
        SparkSession.builder().appName("Fricciones App")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.dynamicAllocation.maxExecutors", 10)
        .getOrCreate()
    )
    val configPath : String = Paths.get("./sparky_bc/config.json").toAbsolutePath().toString()
    val configFile: String = Source.fromFile(configPath).getLines.mkString
    val executionConfig: JsValue = Json.parse(configFile)
    val lshModel = BucketedRandomProjectionLSHModel.load((executionConfig\"lsh_configuracion"\"model_path").as[String])
    val transformationPipeline = PipelineModel.load((executionConfig\"lsh_configuracion"\"transformation_path").as[String])
    
    if ((executionConfig\"lsh_configuracion"\"kind_inference").as[String] == "similarity") {

            getSimilarityTable(spark= spark, executionConfig=executionConfig,
            lshModel=lshModel, transformationPipeline=transformationPipeline, 
            executionKind="similarity")



    } else if ((executionConfig\"lsh_configuracion"\"kind_inference").as[String] == "automatic") { 
    
            getSimilarityTable(spark= spark, executionConfig=executionConfig,
            lshModel=lshModel, transformationPipeline=transformationPipeline, 
            executionKind="automatic")


    
    } else {


            getNeigbornsTable(spark= spark, executionConfig=executionConfig,
            lshModel=lshModel, transformationPipeline=transformationPipeline)

        
    }


}