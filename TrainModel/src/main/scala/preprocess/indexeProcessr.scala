package preprocess

import org.apache.spark.ml.feature.StringIndexer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.Map

class stringConverter(val dataFrame: DataFrame) {


    private def mapStringColumns(typesArray: Array[(String, String)], handleOption: String = "skip", columnID: String = ""): Map[String, StringIndexer] = {


        var indexersMap = Map[String, StringIndexer]()
        
        for ((label, dataType) <- typesArray) {


            if (dataType == "StringType") {


                if (label != columnID) {


                var labeIndexer: String = s"${label}Index"
                var indexerNew = ( new StringIndexer()
                .setInputCol(s"${label}")
                .setOutputCol(labeIndexer)
                .setHandleInvalid(handleOption)
                )

                indexersMap(labeIndexer) = indexerNew

                }

            }

        }


        return indexersMap

    }


    def getDataType(dataFrame:DataFrame) : Array[(String, String)] = {

        val typesArray: Array[(String, String)] = dataFrame.dtypes

        return typesArray


    }

    def getNumericFeatures(dataFrame:DataFrame): ArrayBuffer[String] = {


        val typesArray: Array[(String, String)] = getDataType(dataFrame)
        val numericColumns = ArrayBuffer[String]()
        
        
        for ((label, dataType) <- typesArray) {

            if (dataType != "StringType") {

                
                numericColumns += label

                

            }

        }


    return numericColumns
}

    def generateMapString(dataFrame:DataFrame, columnID: String = ""): Map[String, StringIndexer] = {


        val typesArray: Array[(String, String)] =  getDataType(dataFrame)
        val indexersMap: Map[String, StringIndexer]  =  mapStringColumns(typesArray = typesArray, columnID = columnID)

        return indexersMap

    }

    getNumericFeatures(dataFrame = dataFrame)
    generateMapString(dataFrame = dataFrame)

}