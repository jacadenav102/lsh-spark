import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import play.api.libs.json._  
import java.nio.file.Paths
import scala.io.Source


object dashBoardData extends App {


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
    val idKey: String = (executionConfig\"lsh_configuracion"\"column_id").as[String]
    val originalVariables: String = (executionConfig\"lsh_configuracion"\"variables_tocompare").as[String]
    val queryCompare: String = "SELECT " +  originalVariables + " FROM " + (executionConfig\"lsh_configuracion"\ "automatic_comprable_table").as[String]
    val queryResults: String = "SELECT " +  originalVariables + " FROM " + (executionConfig\"lsh_configuracion"\"automatic_result").as[String]
    val toCompareData: DataFrame = spark.sql(queryCompare)
    val resultData: DataFrame = spark.sql(queryResults)
    val originalData: DataFrame = spark.sql((executionConfig\"lsh_configuracion"\"automatic_result"\"train_table").as[String])

    var dashboardDataB: DataFrame = (
    resultData
    .join(toCompareData, toCompareData(idKey) === resultData(idKey + "b"), "inner")
    .drop(idKey)

    )
    
    val originalColumns: Array[String] = dashboardDataB.columns    
    val fixedColumnsArray: Array[String] = originalColumns.filterNot(_.startsWith(idKey))
    val columnsArray: Array[String]  = fixedColumnsArray.filter(_ != "EuclideanDistance")

    for(z<- columnsArray) {
        dashboardDataB = dashboardDataB.withColumnRenamed(z, s"${z}_b")
    }


    val dashboardData: DataFrame = (

        dashboardDataB
        .join(originalData, originalData(idKey)  === dashboardDataB(idKey + "a"), "inner")
        .drop(idKey)

        )

        (
            dashboardData
            .write
            .format("hive")
            .option("fileformat", "parquet")
            .mode("overwrite")
            .saveAsTable((executionConfig\"lsh_configuracion"\"dashboard_automatic_data").as[String])

        )

}
