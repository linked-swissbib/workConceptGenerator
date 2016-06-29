lazy val commonSettings = Seq(
  organization := "ch.swissbib.linked",
  version := "0.1",
  //scalaVersion := "2.10.6"
  scalaVersion := "2.11.8"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

lazy val sparkVersion = "1.6.2"

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(

    name := "workConceptGenerator",

    resolvers += "clojars" at "https://clojars.org/repo",
    resolvers += "conjars" at "http://conjars.org/repo",
    resolvers += Resolver.sonatypeRepo("public"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.scala-lang", "*"),
      "org.elasticsearch" %% "elasticsearch-spark" % "2.3.2",
      "com.github.scopt" %% "scopt" % "3.5.0",
      "org.scala-lang" % "scala-xml" % "2.11.0-M4"
    )
  )
