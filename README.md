# LSH Implementation Spark

## Description
This Spark implementation was create to be use in a clouder enviroment. The idea es to create a LSH model who could be put in production in that enviroment. The project depends on
a configuration json file, which gives the control of the model and process to the user.

### Components

- trainModel: Methods to preprocess the data and train the LSH model. It also have methods to register the performance of the model in production. 
- inferenceModel: Methods to make inference with a trined model. It has two kinds of execution, automatic where the point of comparation of the model is calculated eith historical data, and vector, where the user gives the point of compraison. 
- dashBoardData: Spark script make to prepare model´s result fot Power Bi visualization.
- automaticPreparation: Methods to impelment the automatic execution of the model. 
