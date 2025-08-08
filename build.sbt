// Root build definition
// Defines a multi‑project build with two sub‑projects:
//  - framework: reusable CDC pipeline framework
//  - memberPipeline: example domain pipeline depending on the framework

lazy val scalaV = "2.12.18"

lazy val framework = (project in file("framework"))
  .settings(
    name := "cdc-framework",
    scalaVersion := scalaV,
    libraryDependencies ++= Seq(
      "org.apache.beam" %% "beam-runners-google-cloud-dataflow-java" % "2.48.0" % "provided",
      "org.apache.beam" %% "beam-sdks-java-core" % "2.48.0" % "provided",
      "com.google.cloud" % "google-cloud-bigquery" % "2.41.2",
      "com.google.cloud" % "google-cloud-pubsub" % "1.125.6",
      "com.amazonaws" % "aws-java-sdk-s3" % "1.12.534",
      "org.yaml" % "snakeyaml" % "2.2"
    ),
    // packaging as an assembly is delegated to subprojects if needed
    Compile / unmanagedSourceDirectories ++= Seq((Compile / sourceDirectory).value / "scala")
  )

lazy val memberPipeline = (project in file("member-pipeline"))
  .dependsOn(framework)
  .settings(
    name := "member-pipeline",
    scalaVersion := scalaV,
    libraryDependencies ++= Seq(
      "org.apache.beam" %% "beam-runners-google-cloud-dataflow-java" % "2.48.0" % "provided"
    ),
    Compile / unmanagedSourceDirectories ++= Seq((Compile / sourceDirectory).value / "scala")
  )
