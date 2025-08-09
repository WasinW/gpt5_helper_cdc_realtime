ThisBuild / scalaVersion := "2.12.18"
lazy val versions = new { val beam="2.58.0"; val gcloudBq="2.43.2"; val slf4j="2.0.12"; val gson="2.10.1"; val snake="2.0"; val awsSdk="2.25.62"; val secret="2.30.0" }
lazy val root = (project in file(".")).aggregate(framework, memberPipeline).settings(name := "gpt5-helper-cdc-realtime", version := "0.4.0")
lazy val commonLibs = Seq(
  "org.apache.beam" % "beam-sdks-java-core" % versions.beam,
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % versions.beam,
  "com.google.cloud" % "google-cloud-bigquery" % versions.gcloudBq,
  "com.google.cloud" % "google-cloud-secretmanager" % versions.secret,
  "software.amazon.awssdk" % "s3" % versions.awsSdk,
  "com.google.code.gson" % "gson" % versions.gson,
  "org.yaml" % "snakeyaml" % versions.snake,
  "org.slf4j" % "slf4j-api" % versions.slf4j)
lazy val framework = (project in file("framework")).settings(name := "cdc-framework", libraryDependencies ++= commonLibs)
lazy val memberPipeline = (project in file("business-domains/member")).dependsOn(framework).settings(name := "member-pipeline")
