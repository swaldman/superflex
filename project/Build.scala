import sbt._

object SuperFlexBuild extends Build {

  val nexus = "https://oss.sonatype.org/"
  val nexusSnapshots = nexus + "content/repositories/snapshots";
  val nexusReleases = nexus + "service/local/staging/deploy/maven2";

  val mySettings = Seq( 
    Keys.organization := "com.mchange",
    Keys.name := "superflex", 
    Keys.version := "0.2.1-SNAPSHOT", 
    Keys.scalaVersion := "2.11.7",
    Keys.publishTo <<= Keys.version { 
      (v: String) => {
	if (v.trim.endsWith("SNAPSHOT"))
	  Some("snapshots" at nexusSnapshots )
	else
	  Some("releases"  at nexusReleases )
      }
    },
    Keys.resolvers += ("snapshots" at nexusSnapshots ),
    Keys.pomExtra := pomExtraXml,
    Keys.scalacOptions ++= Seq("-deprecation","-feature")
  );

  val dependencies = Seq(
    "com.mchange" %% "mchange-commons-scala" % "0.4.1-SNAPSHOT" changing(),
    "org.scala-lang.modules" %% "scala-xml" % "1.0.5"
  );

  override lazy val settings = super.settings ++ mySettings;

  lazy val mainProject = Project(
    id = "superflex",
    base = file("."),
    settings = Project.defaultSettings ++ (Keys.libraryDependencies ++= dependencies)
  ); 

  val pomExtraXml = (
      <url>http://www.mchange.com/project/superflex</url>
      <licenses>
        <license>
          <name>GNU Lesser General Public License, Version 2.1</name>
          <url>http://www.gnu.org/licenses/lgpl-2.1.html</url> 
          <distribution>repo</distribution>                                                                                                                       
        </license>
     </licenses>
     <scm>
       <url>git@github.com:swaldman/superflex.git</url>
       <connection>scm:git:git@github.com:swaldman/superflex.git</connection>
     </scm>
     <developers>
       <developer>
         <id>swaldman</id>
         <name>Steve Waldmam</name>
         <email>swaldman@mchange.com</email>
       </developer>
     </developers>
  );
}

