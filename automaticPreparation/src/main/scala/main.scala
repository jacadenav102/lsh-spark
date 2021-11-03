
import automatic.clusterCalculation.getComparableClusterTable
import automatic.automaticScoredData.individualScoredData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import play.api.libs.json._  
import java.nio.file.Paths
import scala.io.Source




object automaticPreparation extends App {


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

    getComparableClusterTable(spark, executionConfig)
}