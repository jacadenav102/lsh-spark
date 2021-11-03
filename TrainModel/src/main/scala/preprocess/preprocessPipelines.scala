package preprocess

import org.apache.spark.ml.feature.VectorAssembler
import preprocess.vectorezTools.getStringIndexers
import org.apache.spark.ml.feature.StringIndexer
import preprocess.vectorezTools.getTotalColumns
import preprocess.vectorezTools.vectorizerStep
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.Map
import preprocess.stringConverter

object finalStages {

    def getVectorizeStages(dataFrame: DataFrame, columnID: String = ""): Array[PipelineStage] = {

        val indexer = new stringConverter(dataFrame = dataFrame)
        var indexersMap: Map[String, StringIndexer] = indexer.generateMapString(dataFrame = dataFrame, columnID = columnID)
        val numericColumns: ArrayBuffer[String] = indexer.getNumericFeatures(dataFrame = dataFrame)
        val totalColumns: Array[String] = getTotalColumns(indexersMap, numericColumns)
        val vectorizer: VectorAssembler =  vectorizerStep(columnsArray = totalColumns)
        var indexersArray: Array[StringIndexer] = getStringIndexers(indexersMap)
        val vectorizeStages: Array[PipelineStage] = indexersArray :+ vectorizer

        return vectorizeStages

    }

}
