lazy val commonSettings = Seq(
  organization := "br.bireme",
  version := "0.1.0",
  scalaVersion := "2.12.1"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "Iahx2Others"
  )

//resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"
resolvers += "lightshed-maven" at "http://dl.bintray.com/content/lightshed/maven"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
  "org.mongodb" %% "casbah" % "3.1.1",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "org.apache.commons" % "commons-csv" % "1.4",
  //"me.lessis" %% "courier" % "0.1.3"
  "ch.lightshed" %% "courier" % "0.1.4"
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
