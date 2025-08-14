// Build settings specific to the member pipeline submodule.  This
// project depends on the framework defined in the root build.

// If you wish to create a fat JAR for deployment to Dataflow you can
// enable the sbt‑assembly plugin here.  Uncomment and adjust the
// following lines as needed:

// addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")

// assemblyMergeStrategy in assembly := {
//   case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//   case x => MergeStrategy.first
// }