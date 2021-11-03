package train

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.feature.StandardScaler
import preprocess.finalStages.getVectorizeStages
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.PipelineStage
import train.modelTtrain.getStandarStage
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.Pipeline

object finalPipeline {

    
    
    def getFinalPipeline(dataModel: DataFrame,  columnID: String = ""): Pipeline = {
        val vectorizeStages: Array[PipelineStage] = getVectorizeStages(dataFrame = dataModel, columnID = columnID)
        val standarStage: StandardScaler = getStandarStage()
        val modelStages: Array[PipelineStage] = Array(standarStage)
        val totalStages: Array[PipelineStage] = vectorizeStages ++ modelStages
        val finalPipeline =  ( 
            new Pipeline()
            .setStages(totalStages) 
            )

        return finalPipeline


    }
}

