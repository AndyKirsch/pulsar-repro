scalaVersion := "2.13.12"

version := "1.0"

lazy val commonSettings = Seq(
  organization := "com.iterable",

  scalaVersion := "2.13.12",
  libraryDependencies ++= Seq(
    "com.clever-cloud.pulsar4s" %% s"pulsar4s-core" % "2.8.1" exclude("org.apache.pulsar", "pulsar-client"),
    "com.typesafe.play" %% "play-ws" % "2.9.0",
    "com.typesafe.play" %% "play-ahc-ws" % "2.9.0",
    "com.typesafe.play" %% "play-test" % "2.9.0",
    "org.scalatest" %% "scalatest" % "3.2.15" % IntegrationTest,
    "com.typesafe.play" %% "play-json" % "2.10.1",
  ),
  version := "1.0.0",
//  Compile / scalaSource := baseDirectory.value / ".." / ".." / "src" / "main" / "scala",
//  Test / scalaSource := baseDirectory.value / ".." / ".." / "src" / "test" / "scala",
//  IntegrationTest / scalaSource := baseDirectory.value / ".." / ".." / "src" / "it" / "scala"
)

def generateProject(subProjectName: String, pulsarClientVersion: String) = {
  configure(Project(subProjectName, file(s"target/${subProjectName}")), pulsarClientVersion)
    .settings(
      name := s"pulsar-repro-$subProjectName",
    )
}

def configure(project: Project, pulsarClientVersion: String) =
  project.configs(IntegrationTest)
    .settings(
      libraryDependencies += "io.streamnative" % "pulsar-client" % pulsarClientVersion,
      Defaults.itSettings,
    )
    .settings(commonSettings)


lazy val oldVersion = configure(project in file("oldVersion"), "2.10.4.4")
lazy val newVersion = configure(project in file("newVersion"), "2.11.2.13")

Global / onChangedBuildSource := ReloadOnSourceChanges