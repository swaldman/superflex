package com.mchange.sc.v1.democognos.dbutil;

import java.io.{File, FileInputStream, InputStreamReader, BufferedReader, IOException};
import java.sql.{Connection,DriverManager,PreparedStatement,SQLException,Statement,Types};
import java.text.{ParseException,SimpleDateFormat};
import scala.actors.Actor;
import scala.collection.mutable.ArrayBuffer;

object TextFileDbArchiver
{
  var dateFormatPatterns = Array("yyyy-MM-dd", "yyyyMMdd", "dd-MMM-yyyy", "dd-MMM-yy", "MM/dd/yy", "MM/dd/yyyy");
  
  def createFormats() = for( p <- dateFormatPatterns ) yield new SimpleDateFormat(p);

  type Closable = { def close(): Unit };

  def attemptClose( closable : Closable ) = 
    {
      try { if (closable != null) closable.close(); }
      catch { case ex : Exception => ex.printStackTrace(); }
    }

  object Key
  {
    val COL_NAMES = "colNames";
    val LABELS    = "labels";
  }
}


// TODO: -- Replace colTypes array with a lazy getNumCols(), getColName(int num), getTypeDecl(int num)
//          style interface to permit arbitrary overrides
//       -- let values from readMetaInfo() serve as the defaults for unavailable info
abstract class TextFileDbArchiver
{
  import TextFileDbArchiver._; //bring in unqualified reference to companion objects members

  case class PresetColType( val header : String, val sqlTypeDecl : String, val setter : ( PreparedStatement, Int, String ) => Unit );

  val jdbcUrl             : String;
  val jdbcDriverClassName : String;
  val jdbcUserName        : String;
  val jdbcPassword        : String;

  val tableName           : String;

  val files      : Seq[File];
  val colTypes   : Array[Option[PresetColType]] ;

  val emptyStringIsNull : Boolean;

  // should trim if desired
  def split(row : String) : Array[String];

  def transformQuoteColName( colName : String ) : String;

  def padFieldLength( observedMaxLength : Int) : Int;

  def readMetaData( br: BufferedReader ) : MetaData;

  val bufferSize   = 1024 * 1024 * 256; //256M

  val fileEncoding = "ISO-8859-1";

  val mustInfer  : Boolean = { colTypes == null || colTypes.length == 0 || colTypes.exists( _ == None) };

  type MetaData = Map[String,List[String]];

  /** throw an Exception if not valid */
  def validateMetaData( mds : Seq[MetaData]) : Unit =
  {
    if ( mds.size == 0 )
      throw new DbArchiverException("Failed to read even one metadata instance: " + mds.mkString("{ ",", "," }"));
    val first = mds.head;
    if (! mds.forall( _ sameElements first) )
	throw new DbArchiverException("Metadata from all files must be identical! " + mds);
  }
  
  def archiveFiles() : Unit =
  {
    if (mustInfer)
      {
	val metaData = inferMetaData();

	val headers = metaData( Key.COL_NAMES );

	val inferredCols : Seq[ExaminedColumn] = inferCols(headers.length);


	// Note: Would it be worth setting up the insertions concurrently?
	//       Shouldn't be too hard to do...

	var con : Connection = null;
	var stmt : Statement = null;
	var ps : PreparedStatement = null;

	try
	{
	  Class.forName ( jdbcDriverClassName );
	  con = 
	    {
	      if ( jdbcUserName == null )
		DriverManager.getConnection( jdbcUrl );
	      else
		DriverManager.getConnection( jdbcUrl, jdbcUserName, jdbcPassword );
	    };

	  // XXX: 4096 is hardcoded here...
	  val sb : StringBuilder = new StringBuilder(4096);
	  sb.append("CREATE TABLE ");
	  sb.append( tableName );
	  val decls =
	    for (i <- 0 until headers.length) yield
	    {
	      val preset : Option[PresetColType] = getPresetCol(i);

	      if (preset != None)
		{
		  // type inference doesn't seem to be properly figuring out the type
		  // of present.get... I'll work around

		  println("Using preset!");
		  val pct : PresetColType = preset.getOrElse(null);
		  transformQuoteColName(pct.header) + " " + pct.sqlTypeDecl;
		}
	      else
		{
		  printf("headers(%d): %s %s\n", i.asInstanceOf[Object], headers(i), transformQuoteColName(headers(i)));
		  transformQuoteColName(headers(i)) + " " + inferredCols(i).bestTypeDeclaration._1;
		}
	    };
	  sb.append( decls.mkString("( ", ", ", " )") );
	  
	  val ddl = sb.toString();
	  println( ddl );
	  stmt = con.createStatement();
	  stmt.executeUpdate( ddl );

	  // generate appropriate data setting functions, mixing inferred with preset columns
	  val setters =
	    for (i <- 0 until headers.length) yield
	      {
		val preset : Option[PresetColType] = getPresetCol(i);
		
		if (preset != None)
		  {
		    // type inference doesn't seem to be properly figuring out the type
		    // of present.get... I'll work around
		    
		    val pct : PresetColType = preset.getOrElse(null);
		    pct.setter;
		  }
		else
		  inferredCols(i).setter;
	      };

	  val psInsertionPoints = ( (for (d <- decls) yield '?').mkString("( ", ", ", " )") );
	  ps = con.prepareStatement( String.format("INSERT INTO %s VALUES %s", tableName, psInsertionPoints ) );

	  for (f <- files)
	    {
	      var br : BufferedReader = null;
	      try
	      {
		br = f2br( f );
		readMetaData( br ); // skip preliminaries...
		var line = br.readLine();
		while ( goodLine( line ) )
		{
		  val data = split( line );
		  for (i <- 0 until data.length)
		    {
		      if (emptyStringIsNull && data(i).length == 0)
			ps.setNull(i+1, inferredCols(i).bestTypeDeclaration._2);
		      else
			setters(i)(ps, i + 1, data(i)); // ps slots are indexed by 1, arrays are indexed by zero
		    }
		  ps.executeUpdate();
		  line = br.readLine();
		}
	      }
	      finally
	      { attemptClose( br ); }
	    }
	}
	finally
	{ List[Closable](ps,stmt,con).foreach( attemptClose _ ); }
      }
    else
      {
	//TODO...
	throw new Exception("Not implemented without inference, but it should be EEEEZ.");
      }
  }

  private def getPresetCol( index : Int ) : Option[PresetColType] = if (colTypes == null || colTypes.length <= index) { None } else { colTypes(index) }



  private def f2br(f : File, bs : Int) : BufferedReader = { new BufferedReader( new InputStreamReader( new FileInputStream( f ), fileEncoding ), bs ) } 

  private def f2br(f : File) : BufferedReader = { f2br( f, bufferSize ); }

  // XXX: hardcoded 8K starting buffer for headers
  private def buildStreams() : Seq[BufferedReader] = 	
    {
      var buildStreams = new ArrayBuffer[BufferedReader];
      try
      {
	for (f <- files)
	  buildStreams += f2br(f, 8192);
	buildStreams.readOnly;
      }
      catch
      { case ex : Exception => buildStreams.foreach( attemptClose _ ); throw ex; }
    };

/*
  // note -- trying to retain the open streams and pass them to actors did not work well
  //      -- strange misbehavior. much simpler to close the streams and recreate in
  //      -- inspect cols
  def inferHeaders() : Array[String] =
    {
      val streams = buildStreams();

      try
      {
	val allHeaderRows = for( br <- streams ) yield split( br.readLine() );
	val firstHeaders = allHeaderRows(0);
	//printf("firstHeaders: %s\n", firstHeaders.mkString(", "));
	for (r <- allHeaderRows) println(r.mkString(", "));
	println( allHeaderRows(0) == allHeaderRows(1) )

	if (! allHeaderRows.forall(_ sameElements firstHeaders)) 
	  throw new DbArchiverException("Header rows of streams differ! " + allHeaderRows);
	else if (colTypes != null && colTypes.length > 0 && firstHeaders.length != colTypes.length) 
	  throw new DbArchiverException("Declared width and number of headers do not match! [" + colTypes.length + " vs " + firstHeaders + " ]");  
	else 
	  firstHeaders;
      }
      finally
      { streams.foreach( attemptClose _ ); }
    }
*/

  private def inferMetaData() : MetaData =
    {
      val streams = buildStreams();

      try
      {	
	val allMetaData = for( br <- streams ) yield readMetaData( br ); 
	validateMetaData( allMetaData );
	allMetaData.head
      }
      finally
      { streams.foreach( attemptClose _ ); }
    }


  private def inferCols( numCols : Int) : Seq[ExaminedColumn] =
  {
    // generate separate column inspections for each file
    val actors = for (f <- files) yield 
      {
	val a = new StreamColumnsExaminer(Actor.self);
	a.start();
	a ! ( f, numCols );
	a;
      }

    // use the && operator of examined columns to infer column definitions
    // "wide enough" to accommodate all data from all files
    //
    // note that we expact as many replies as there are actors, so we just iterate over the actors collection
    val fileResults = for (a <- actors) yield { Actor.self.receive {case ec : Vector[ExaminedColumn] => ec} };
    val inferredCols = 
      for (c <- 0 until numCols) yield
	( (for( i <- 0 until files.length) yield fileResults(i)(c)).reduceLeft( _ && _ ) );

    return inferredCols;
  }

  private case class ExaminedColumn(val numericOnly : Boolean, 
				    val integerOnly : Boolean, 
				    val fixedLength : Option[Int], 
				    val maxLength : Int, 
				    val dateFormatStr : Option[String])
  {
      def &&( other : ExaminedColumn ) = ExaminedColumn(this.numericOnly && other.numericOnly,
							this.integerOnly && other.integerOnly,
							if (this.fixedLength == other.fixedLength) { fixedLength } else { None },
							this.maxLength max other.maxLength,
							if (this.dateFormatStr == other.dateFormatStr) { dateFormatStr } else { None });

    // TODO: Make this customizable to different databases
    val bestTypeDeclaration = 
      {
	if (dateFormatStr != None)
	  ("DATE", Types.DATE);
	else if (integerOnly)
	  {
	    if (maxLength < 10)
	      ("INTEGER", Types.INTEGER);
	    else
	      ("BIGINT", Types.BIGINT);
	  }
	else if (numericOnly)
	  ("DOUBLE PRECISION", Types.DOUBLE);
	else if (fixedLength != None)
	  (String.format("CHAR(%d)", int2Integer(fixedLength.get.asInstanceOf[Int]) ), Types.CHAR); //workaround of weird type, ambiguous conversion problems
	else
	  (String.format("VARCHAR(%d)", int2Integer(padFieldLength( maxLength ))), Types.VARCHAR);
      }

    val setter : (PreparedStatement, Int, String) => Unit =
      {


	bestTypeDeclaration._1 match
	{
	  case "DATE" => 
	    new Function3[PreparedStatement,Int,String,Unit]
	    {
	      val df = new SimpleDateFormat( dateFormatStr.getOrElse(null) ); // work around weird problems with Option.get()

	      override def apply( ps : PreparedStatement, i : Int, s : String) : Unit = { ps.setDate(i, new java.sql.Date(df.parse( s ).getTime() ) ) }
	    }
	  case "INTEGER"          => ( (ps : PreparedStatement, i : Int, s: String) => ps.setInt(i, Integer.parseInt(s)) );
	  case "BIGINT"           => ( (ps : PreparedStatement, i : Int, s: String) => ps.setLong(i, java.lang.Long.parseLong(s)) );
	  case "DOUBLE PRECISION" => ( (ps : PreparedStatement, i : Int, s: String) => ps.setDouble(i, java.lang.Double.parseDouble(s)) );
	  case _                  => ( (ps : PreparedStatement, i : Int, s: String) => ps.setString(i, s ) );
	}
      }
  }

  private def goodLine( line : String ) : Boolean = { line != null && line.trim().length > 0 } 

  private class StreamColumnsExaminer(val mgr : Actor) extends Actor
  {
    override def act : Unit =
      {
	val recs : Array[Rec] = receive
	{
	  case ( f : File, numCols : Int ) => 
	    {
	      val br = f2br(f);
	      readMetaData( br ); //skip preliminaries
	      try { examineColumns( br, numCols ); }
	      finally { br.close(); }
	    }
	  //case ( a : Any) => printf("%s: Unexpected message -- %s", this, a);
	}
	mgr ! ( for (rec <- recs) yield rec.toExaminedColumn );
      }

    def examineColumns( br : BufferedReader, numCols : Int) : Array[Rec] =
      {
	val out = new Array[Rec](numCols);
	for (i <- 0 until numCols)
	  out(i) = new Rec;

	var line = br.readLine();

	while (goodLine(line))
	{
	  println(line);
	  val data = split(line);
	  assert( data.length == numCols, String.format("data.length (%s) and numCols (%s) should be equal", data.length.asInstanceOf[Object], numCols.asInstanceOf[Object] ) );

	  for (i <- 0 until numCols)
	    out(i).update(data(i));

	  line = br.readLine();
	}

	out;
      }

    class DateFormatGuesser
    {
      import scala.collection.mutable.Queue;

      var inPlay = new Queue[SimpleDateFormat];
      inPlay ++= createFormats();

      var anyHope = !inPlay.isEmpty;

      def check( maybeDate : String) : Unit = 
	{
	  def check(maybeDate : String, df : SimpleDateFormat) : Boolean =
	    {
	      try { df.parse( maybeDate ); true; }
	      catch { case ex : ParseException => false; }
	    }

	  inPlay.dequeueAll( ! check( maybeDate, _ ) );
	  anyHope = !inPlay.isEmpty;
	}

      def guess = { inPlay.front; }
    }	



    class Rec
    {
      var numericOnly : Boolean = true;
      var integerOnly : Boolean = true;
      var fixedLength : Int = -1; // -2 means not fixed, -1 means unknown, nonneg means putatively fixed
      var maxLength : Int = 0;

      val dfg : DateFormatGuesser = new DateFormatGuesser;

      def toExaminedColumn = ExaminedColumn(numericOnly, 
					    integerOnly,
					    if (fixedLength > 0) { Some(fixedLength) } else { None },
					    maxLength,
					    if ( dfg.anyHope ) { Some( dfg.guess.toPattern ) } else { None } );

					     

      def update( datum : String ) =
	{
	  if (numericOnly || integerOnly)
	    {
	      if (datum.length == 0 && ! emptyStringIsNull) //we interpret this case as empty string
		{
		  numericOnly = false;
		  integerOnly = false;
		}
	      else if (! datum.forall( "0123456789.".contains( _ ) ) )
		{
		  numericOnly = false;
		  integerOnly = false;
		}
	      else if ( datum.contains('.') )
		{
		  integerOnly = false;
		}
	    }

	  if (fixedLength > -2)
	    {
	      if (fixedLength >= 0)
		{
		  fixedLength = if (datum.length == fixedLength) { datum.length } else { -2 }; //a variation
		}
	      else
		fixedLength = datum.length;
	    }

	  maxLength = maxLength max datum.length;

	  if ( dfg.anyHope )
	    dfg.check ( datum );
	}
    }


  }
}



