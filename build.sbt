organization := "com.kifi"

name := "franz"

version := "0.3.16"

crossScalaVersions := Seq("2.10.4", "2.11.5", "2.12.2")

scalaVersion := "2.12.2"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

// libraryDependencies += "com.amazonaws" % "aws-java-sdk-sqs" % "1.10.16"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-sqs" % "1.11.167"

// libraryDependencies += "com.typesafe.play" %% "play-json" % "2.3.7"
libraryDependencies += "com.typesafe.play" %% "play-ws-standalone-json" % "1.0.0"

// libraryDependencies += "com.typesafe.play" %% "play-iteratees" % "2.3.7"
libraryDependencies += "com.typesafe.play" % "play-iteratees_2.10" % "2.4.11"

libraryDependencies += "com.amazonaws" % "amazon-sqs-java-extended-client-lib" % "1.0.1"

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/FortyTwoEng/franz</url>
  <licenses>
    <license>
      <name>MIT</name>
      <url>http://opensource.org/licenses/MIT</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:FortyTwoEng/franz.git</url>
    <connection>scm:git:git@github.com:FortyTwoEng/franz.git</connection>
  </scm>
  <developers>
    <developer>
      <id>stkem</id>
      <name>Stephen Kemmerling</name>
      <url>https://github.com/stkem</url>
    </developer>
  </developers>)
