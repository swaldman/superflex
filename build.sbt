ThisBuild / organization := "com.mchange"
ThisBuild / version      := "0.2.2-SNAPSHOT"

ThisBuild / resolvers += Resolver.sonatypeRepo("releases")
ThisBuild / resolvers += Resolver.sonatypeRepo("snapshots")

ThisBuild / publishTo := {
  if (isSnapshot.value) Some(Resolver.sonatypeRepo("snapshots")) else Some(Resolver.url("sonatype-staging", url("https://oss.sonatype.org/service/local/staging/deploy/maven2")))
}

lazy val root = project
  .in(file("."))
  .settings(
    name                := "superflex",
    scalaVersion        := "2.11.12",
    libraryDependencies += "com.mchange" %% "mchange-commons-scala" % "0.4.16",
    libraryDependencies += "com.mchange" %% "mlog-scala"            % "0.3.14",
    libraryDependencies += "org.scala-lang.modules" %% "scala-xml"  % "1.3.0",
    pomExtra := pomExtraForProjectName_LGPLv21( name.value )
  )


// publication, pom extra stuff, note this is single-licensed under LGPL v2.1

def pomExtraForProjectName_LGPLv21( projectName : String ) = {
    <url>https://github.com/swaldman/{projectName}</url>
    <licenses>
      <license>
        <name>GNU Lesser General Public License, Version 3</name>
        <url>https://www.gnu.org/licenses/lgpl-2.1.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:swaldman/{projectName}.git</url>
      <connection>scm:git:git@github.com:swaldman/{projectName}.git</connection>
    </scm>
    <developers>
      <developer>
        <id>swaldman</id>
        <name>Steve Waldman</name>
        <email>swaldman@mchange.com</email>
      </developer>
    </developers>
}



