package preprocess


import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map


object vectorezTools {

def vectorizerStep(columnsArray: Array[String], outputCol: String = "features", handleOption: String = "skip" ): VectorAssembler  = {


    val assembler = ( 
        new VectorAssembler()
        .setInputCols(columnsArray)
        .setOutputCol(outputCol)
        .setHandleInvalid(handleOption)
    )

    return assembler

}


def getTotalColumns(indexersMap: Map[String, StringIndexer],  numericColumns: ArrayBuffer[String]): Array[String] = { 
    
    var indexLabels = ArrayBuffer[String]()
    for ((labeIndexer, indexer) <- indexersMap) {

        indexLabels += labeIndexer



    }

    val totalColumns:  ArrayBuffer[String] = indexLabels ++ numericColumns

    return totalColumns.to[Array]

}

def getStringIndexers(indexersMap: Map[String, StringIndexer]): Array[StringIndexer] = { 
    
    var indexersArray = ArrayBuffer[StringIndexer]()
    for ((labeIndexer, indexer) <- indexersMap) {

        indexersArray += indexer



    }

    

    return indexersArray.to[Array]

}

}