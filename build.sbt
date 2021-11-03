name := "lshProcess"
organization := "com.bancolombia"
version := "0.1-SNAPSHOT"
scalaVersion in ThisBuild := "2.11.12"


val sparkVersion = "2.4.0"

lazy val commonSettings = Seq(
libraryDependencies ++= commondepenencies,
resolvers ++=  commonResolvers
)

lazy val trainModel = (project in file("TrainModel"))
    .settings(commonSettings,
    assemblySettings)


lazy val automaticPreparation = (project in file("automaticPreparation"))
.settings(commonSettings, assemblySettings)


lazy val InferenceModel = (project in file("InferenceModel"))
    .settings(commonSettings,
    assemblySettings)

lazy val dashBoardData = (project in file("dashBoardData"))
    .settings(commonSettings,
    assemblySettings)

lazy val commondepenencies = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "com.typesafe.play" %% "play-json" % "2.7.2",
    "org.joda" % "joda-convert" % "1.8.1",
    "org.json4s" % "json4s-core_2.11" % "3.5.0",
    "org.apache.pdfbox" % "pdfbox" % "2.0.1"

  )




lazy val commonResolvers = Seq(
    "PDFBox Repository" at "https://mvnrepository.com/artifact/org.apache.pdfbox/pdfbox",
    "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases/",
    "MavenRepository" at "https://mvnrepository.com/",
    "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

)


lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "application.conf"            => MergeStrategy.concat
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)