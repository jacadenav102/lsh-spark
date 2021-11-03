package monitoring

import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel
import org.apache.spark.sql.expressions.Window
import scala.collection.JavaConversions._
import java.time.format.DateTimeFormatter
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
import java.time.LocalDateTime
import play.api.libs.json._ 


object monitoringTool {


    def createMonitorTable(spark: SparkSession, proyectName: String, modelName: String, metricId: Int,
        metricName: String, metricValue: Option[Double] = null, execution: Int = 0): DataFrame = {
        import spark.implicits._
        val monitorSchema: StructType = StructType( Array(
            StructField("id_momento", StringType, false),
            StructField("proyecto", StringType, false),
            StructField("modelo", StringType, false),
            StructField("id_indicador", IntegerType, false),
            StructField("nombre_indicador", StringType, false),
            StructField("ejecucion", IntegerType, false),
            StructField("valor_indicador", DoubleType, true))
        )
        val nowTime: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYY/MM/dd HH:mm:ss"))
        val trainInfo = Row(nowTime, proyectName, modelName, metricId, metricName, execution, metricValue)
        val monitorData: Seq[Row] = (
            Seq(trainInfo)
        ) 
        val monitorTable: DataFrame = spark.createDataFrame(monitorData, monitorSchema)
        
        return monitorTable

    }




}