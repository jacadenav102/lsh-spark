package train

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.feature.StandardScaler

object modelTtrain {
    
    
    def getLSHStage(BucketLength: Double = 2.0, NumHashTables: Integer = 3, InputCol: String= "scaledFeatures", OutputCol: String = "hashes"): BucketedRandomProjectionLSH = {
        val projectionModel = (
            new BucketedRandomProjectionLSH()
            .setBucketLength(BucketLength)
            .setNumHashTables(NumHashTables)
            .setInputCol(InputCol)
            .setOutputCol(OutputCol)
        )

        return projectionModel



    }


    def getStandarStage(InputCol: String = "features", OutputCol: String = "scaledFeatures", WithStd: Boolean = true, WithMean: Boolean = true): StandardScaler ={

        val scalerStage = (
            new StandardScaler()
            .setInputCol(InputCol)
            .setOutputCol(OutputCol)
            .setWithStd(WithStd)
            .setWithMean(WithMean)
        )


        return scalerStage


    }






}