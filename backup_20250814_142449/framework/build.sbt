// Build settings specific to the framework module
// This file can contain additional settings or overrides.  By default it
// inherits from the root project's settings defined in `build.sbt`.
// When packaging the framework on its own you can enable the sbt‑assembly plugin here.

// Example: enable sbt‑assembly for packaging
// addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")
// libraryDependencies ++= Seq(
//   "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Runtime
// )
libraryDependencies ++= Seq(
  "org.apache.beam" % "beam-runners-direct-java" % "2.58.0" % Runtime
)
