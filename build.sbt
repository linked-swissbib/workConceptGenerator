lazy val commonSettings = Seq(
  organization := "org.swissbib.linked",
  version := "1.0",
  scalaVersion := "2.10.6"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("javax", "inject", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case PathList("org", "glassfish", xs@_*) => MergeStrategy.last
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

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(

    name := "workConceptGenerator",
    assemblyJarName := "workConceptGenerator.jar",
    mainClass in assembly := Some("org.swissbib.linked.Application"),

    resolvers += "clojars" at "https://clojars.org/repo",
    resolvers += "conjars" at "http://conjars.org/repo",
    resolvers += Resolver.sonatypeRepo("public"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.6.2",
      "org.elasticsearch" %% "elasticsearch-spark" % "2.4.0",
      "com.github.scopt" %% "scopt" % "3.5.0"
      // "org.scala-lang.modules" %% "scala-xml" % "1.0.6"
    )
  )
