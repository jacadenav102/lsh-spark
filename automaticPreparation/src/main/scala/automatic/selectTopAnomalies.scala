package automatic

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DoubleType
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.ml._
import play.api.libs.json._  


object automaticScoredData {

    def getCachedDataFrame(spark: SparkSession, executionConfig: JsValue): DataFrame = {

        val data: DataFrame = spark.sql((executionConfig\"lsh_configuracion"\"queries"\ "anomalies_table").as[String])
        data.cache()

        return data

    }


    def getWeigthsVector(spark:SparkSession,  data: DataFrame):  scala.collection.immutable.Vector[Double] = {
        

        import spark.implicits._    
        val groupedData: DataFrame = (
            data
            .groupBy("prediction")
            .count()
            .withColumn("weigths", pow(col("count"), -1))
            .orderBy(asc("prediction"))
            .drop("count")
        )
        val weightsVector =  groupedData.select("weigths").map(f => f.getDouble(0)).collect.toVector

        return weightsVector

    }


    def cleanDataFrame(data: DataFrame): DataFrame ={

        var cleanedData: DataFrame = (
            data
            .withColumn("probability" ,regexp_replace(col("probability"), "\\[", ""))
            .withColumn("probability" ,regexp_replace(col("probability"), "\\]", ""))
            .filter(col("anomalies") === 1)
        )

        return cleanedData

    }



    def weightedDataFrame(toProcessData: DataFrame,
        weightsVector: scala.collection.immutable.Vector[Double],
        numberCluster: Int,
        splitColumnNames: Column): DataFrame = {

            var cleanedData: DataFrame = toProcessData

            for (counter <- 0 to (numberCluster - 1) ) {


                cleanedData = (
                    cleanedData
                    .withColumn(s"f$counter", splitColumnNames.getItem(counter).cast(DoubleType) * weightsVector(counter)) 
                )


            }

            return cleanedData

    }


    def individualScoredData(spark: SparkSession, executionConfig: JsValue): DataFrame = {
                
        val data: DataFrame = getCachedDataFrame(spark=spark, executionConfig=executionConfig)
        val weightsVector: scala.collection.immutable.Vector[Double] = getWeigthsVector(spark=spark, data=data)
        var cleanedData: DataFrame = cleanDataFrame(data=data)
        val numberCluster: Int = (
            cleanedData
            .select("probability")
            .rdd
            .take(1)(0)(0)
            .asInstanceOf[String]
            .split(",").size
        )
        val splitColumnNames: Column = split(col("probability"), ",")
        val finalData: DataFrame = weightedDataFrame(cleanedData, weightsVector, numberCluster, splitColumnNames)
        data.unpersist()
        return finalData

    }

}

object topAnomaliesTools {



    def getToSumColumns(ScoredData: DataFrame): Array[Column] = {


        val columnsList: Array[String] = ScoredData.columns
        val columnsToSum: Array[Column] = (
            columnsList
            .filter(_.startsWith("f"))
            .map(s => col(s))

        )

        return columnsToSum
    }
 


    def getTotalScoredData(ScoredData: DataFrame, columnsToSum: Array[Column]): DataFrame = {


        var finalData = ScoredData.withColumn("score", columnsToSum.reduce(_+_))
        finalData = (
            finalData
            .select("key_mv", "score")
            .dropDuplicates("score")
            .orderBy(asc("score"))

        )


        return finalData

    }


    
    def saveScoredFeatures(spark: SparkSession, ScoredData: DataFrame, executionConfig: JsValue): Unit = {

        val keyId: String = (executionConfig\"lsh_configuracion"\"column_id").as[String]
        val columnsToSumm: Array[Column] = getToSumColumns(ScoredData)
        val dataMart: DataFrame = spark.sql((executionConfig\"lsh_configuracion"\"queries"\"train_table").as[String])
        val totalScoredData: DataFrame = getTotalScoredData(ScoredData=ScoredData, columnsToSum=columnsToSumm)



        val toExportData: DataFrame = (
            dataMart
            .alias("fst")
            .join(totalScoredData, dataMart(keyId) === totalScoredData(keyId), "inner")
            .limit((executionConfig\"lsh_configuracion"\"top_compare").as[Int]))
            .select("fst.*")


        (
        toExportData
        .write
        .format("hive")
        .mode("overwrite")
        .option("fileFormat", "parquet")
        .saveAsTable((executionConfig\"lsh_configuracion"\"automatic_comprable_table").as[String]))
    }

}



object clusterCalculation {


    def getStringArray(typesArray: Array[(String, String)], columnID: String = ""): scala.collection.mutable.ArrayBuffer[String] = {


        var stringArray = scala.collection.mutable.ArrayBuffer[String]()
        
        for ((label, dataType) <- typesArray) {


            if (dataType == "StringType") {


                if (label != columnID) {



                

                stringArray += label

                }

            }

        }


        return stringArray

        }


    def getNotStringArray(typesArray: Array[(String, String)], columnID: String = ""): scala.collection.mutable.ArrayBuffer[String] = {


        var stringArray = scala.collection.mutable.ArrayBuffer[String]()
        
        for ((label, dataType) <- typesArray) {


            if (label != columnID) {

                    
                if (dataType != "StringType") {
                stringArray += label

                }

            }

        }


    return stringArray

    }

    def getStrinColumns(dataFrame: DataFrame, columnID: String): scala.collection.mutable.ArrayBuffer[String] = {

        val typesArray: Array[(String, String)] = dataFrame.dtypes
        var stringArray: scala.collection.mutable.ArrayBuffer[String] = getStringArray(typesArray, columnID)
         
        return stringArray


    }

    def getNotStrinColumns(dataFrame: DataFrame, columnID: String): scala.collection.mutable.ArrayBuffer[String] = {

        val typesArray: Array[(String, String)] = dataFrame.dtypes
        var stringArray: scala.collection.mutable.ArrayBuffer[String] = getNotStringArray(typesArray, columnID)
            
        return stringArray


    }


    def getModeData(spark: SparkSession, executionConfig: JsValue, dataFrame: DataFrame): DataFrame = {

        var columnStrings: scala.collection.mutable.ArrayBuffer[String] = (
            getStrinColumns(dataFrame=dataFrame, columnID=(executionConfig\"lsh_configuracion"\"column_id").as[String])
        )
        columnStrings -= (executionConfig\"lsh_configuracion"\"column_id").as[String]
        columnStrings += "prediction"
        val countCategoricals: DataFrame = dataFrame.groupBy(columnStrings.map(c => col(c)): _*).count()
        val toJoin: DataFrame = (
            countCategoricals
            .groupBy(col("prediction").alias("prediction_"))
            .agg(max("count").as("max_"))

        )
        val modeData: DataFrame = ( 
            countCategoricals
            .join(toJoin, toJoin("prediction_") === countCategoricals("prediction") &&  toJoin("max_") === countCategoricals("count"), "inner")
        )

        val finalData: DataFrame = modeData.select(columnStrings.map(c => col(c)):_*)

        return finalData




    }


    def applyMedian(dataFrame: DataFrame, columnsArray: Array[String]): DataFrame = {

        val grpWindow = Window.partitionBy("prediction")
        var medianData: DataFrame = dataFrame
        
        for (col <- columnsArray) {

            val magicPercentile = expr(s"percentile_approx(${col}, 0.5)")
            medianData = medianData.withColumn(s"${col}", magicPercentile.over(grpWindow))


        }

        return medianData


    }

    def getMedianData(spark: SparkSession, executionConfig: JsValue, dataFrame: DataFrame): DataFrame = {
        import spark.implicits._
        

        var notStrinColumns: scala.collection.mutable.ArrayBuffer[String] = (
            getNotStrinColumns(dataFrame=dataFrame, columnID=(executionConfig\"lsh_configuracion"\"column_id").as[String])
        )
        
        notStrinColumns -= (executionConfig\"lsh_configuracion"\"column_id").as[String]
        val finalList: scala.collection.mutable.ArrayBuffer[String] = notStrinColumns
        val numericData: DataFrame = dataFrame.select(finalList.map(c => col(c)):_* )

        val medianData: DataFrame = applyMedian(numericData, finalList.toArray)

        val finalData: DataFrame = medianData.distinct()

        return finalData

    }


    def getComparableClusterTable(spark: SparkSession, executionConfig: JsValue): Unit = {

        val anomalyQuery: String = "SELECT " + (executionConfig\"lsh_configuracion"\"variables_tocompare").as[String] + " ,prediction" + " FROM " + (executionConfig\"lsh_configuracion"\"anomaly_table").as[String]

        val anomalyData: DataFrame = spark.sql(anomalyQuery)

        anomalyData.cache() 

        val medianData: DataFrame = getMedianData(spark, executionConfig, anomalyData)
        val modeData: DataFrame = getModeData(spark, executionConfig, anomalyData)

        anomalyData.unpersist()


        val TotalData: DataFrame = (
            modeData
            .join(medianData, Seq("prediction"), "inner")
            .distinct()
            .withColumnRenamed("prediction", "key_mv")
        )

        (


            TotalData
            .write
            .format("hive")
            .option("fileformat", "parquet")
            .mode("overwrite")
            .saveAsTable((executionConfig\"lsh_configuracion"\"automatic_comprable_table").as[String])



        )


    }


}








