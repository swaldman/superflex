package com.mchange.sc.v1.democognos.dbutil;

import java.io.{File, FileInputStream, InputStreamReader, BufferedReader, IOException};
import java.sql.{Connection,DriverManager,PreparedStatement,SQLException,Statement,Types};
import java.text.{ParseException,SimpleDateFormat};
import scala.actors.Actor;
import scala.actors.Actor._;
import scala.collection._;
import scala.collection.mutable.ArrayBuffer;
import com.mchange.sc.v1.actors.CollectionProcessor;
import com.mchange.sc.v1.util.ClosableUtils._;
import com.mchange.sc.v1.sql.ResourceUtils._;

object SuperFlexDbArchiver
{
  val dfltDateFormatPatterns = Array("yyyy-MM-dd", "yyyyMMdd", "MMddyyyy", "dd-MMM-yyyy", "dd-MMM-yy", "MM/dd/yy", "MM/dd/yyyy", "ddMMMyyyy");
  val dfltDateTimeFormatPatternAddenda = Array("'T'HH:mm:ss");
  val dfltDateTimeFormatPatterns = for (p <- dfltDateFormatPatterns; sfx <- dfltDateTimeFormatPatternAddenda) yield (p + sfx);

  // to distinguish dates from datetimes, since datetime patterns are
  // succeeding when only the prefix date matches, leading to timestamp
  // cols where we want dates
  val extraDateValidator = new Function2[String,String,Boolean]
  {
    def apply(maybeDate : String, patternStr : String) : Boolean =
    {
      def countColons(s : String) : Int =
      {	s.filter( _ == ':').length }

      return ( countColons( maybeDate ) == countColons( patternStr ) );
    }
  }

  // lowercase only! -- we lowercase values before trying to match with these strings.
  val trueStrings  = "true"::"t"::"yes"::"y"::"1"::Nil;
  val falseStrings = "false"::"f"::"no"::"n"::"0"::Nil;

  val leadingZerosRegex = "0\\d".r;

  type MetaData = Map[String,List[String]];

  trait NamedDataFileSource
  {
    def createBufferedReader( bufferSize : Int, fileEncoding : String ) : BufferedReader;
    def sourceName : String;
  
    override def equals( other : Any ) : Boolean; 
    override def hashCode() : Int; 
  }

  object FileDataFileSource
  {
    def apply( fileName : String ) : FileDataFileSource = new FileDataFileSource( new File( fileName ) ); 
    def apply( file : File ) : FileDataFileSource = new FileDataFileSource( file ); 

  }

  final class FileDataFileSource( val f : File, fullpath : Boolean ) extends NamedDataFileSource
  {
    def createBufferedReader( bufferSize : Int, fileEncoding : String ) : BufferedReader = { new BufferedReader( new InputStreamReader( new FileInputStream( f ), fileEncoding ), bufferSize ) };
    def sourceName : String = { if (fullpath) f.getCanonicalPath(); else f.getName(); }

    def this( f : File ) = this( f, true ); 

    override def equals( other : Any ) : Boolean = 
      {
	other match 
	{
	  case (o : FileDataFileSource) => this.f == o.f;
	  case _ => false;
	}
      } 

    override def hashCode() : Int = f.hashCode() + 3;
  }

  def dfltColSort( pkNames : List[String] ) : Ordering[String] = new Ordering[String] {
    def compare( t : String, o : String ) = oldDfltColSort( pkNames )( t ).compare( o )
  }

  def oldDfltColSort( pkNames : List[String] )( cn : String) : Ordered[String] =
    {
      new Ordered[String]
      {
	def compare( other : String ) : Int =
	  {
	    val tpki = pkNames.indexOf( cn );
	    val opki = pkNames.indexOf( other );
	    if ( tpki >= 0 && opki >= 0)
	      { 
		if (tpki > opki)       1; 
		else if (tpki < opki) -1;
		else 0;
	      }
	    else if (tpki >= 0)
	      -1;
	    else if (opki >=0)
	      1;
	    else
	      cn.compareTo( other );
	  }
      }
    }

    def asBoolean( maybeBoolean : String) : Option[Boolean] =
      {
	val chk = maybeBoolean.toLowerCase;
	if ( trueStrings.contains( chk ) )
	  Some(true);
	else if ( falseStrings.contains( chk ) )
	  Some(false);
	else
	  None;
      }

  //should only be called on valid, non-null booleans, so use of Option.get should effectively be guarded
  val booleanSetter         = ( (ps : PreparedStatement, i : Int, s: String) => ps.setBoolean(i, asBoolean(s).get ) ); 

  val charTypeSetter        = ( (ps : PreparedStatement, i : Int, s: String) => ps.setString(i, s ) );
  val integerSetter         = ( (ps : PreparedStatement, i : Int, s: String) => ps.setInt(i, Integer.parseInt(s)) );
  val bigintSetter          = ( (ps : PreparedStatement, i : Int, s: String) => ps.setLong(i, java.lang.Long.parseLong(s)) );
  val doublePrecisionSetter = ( (ps : PreparedStatement, i : Int, s: String) => ps.setDouble(i, java.lang.Double.parseDouble( s ) ) );

  def dateSetter( pattern : String )     = new DateTimeSetter( pattern )
  def timestampSetter( pattern : String ) = new DateTimeSetter( pattern )

  // each thread should make a copy, as not all setters can be called concurrently
  // setters that can be called concurrently will be passed back as "copies".
  def copySetter( setter : Function3[PreparedStatement, Int, String, Unit] ) : Function3[PreparedStatement, Int, String, Unit] =
  {
    if ( setter.isInstanceOf[DateTimeSetter] )
      setter.asInstanceOf[DateTimeSetter].copy;
    else
      setter;
  }

  def copyMaybeSetter( maybeSetter : Option[Function3[PreparedStatement, Int, String, Unit]] ) : Option[Function3[PreparedStatement, Int, String, Unit]] =
  {
    if ( maybeSetter != None )
      Some( copySetter( maybeSetter.get ) );
    else
      None;
  }


  // ensures that multiple instances are equal while allowing for distinct instances (required
  // to avoid synchronization during multithreaded use, as SimpleDateFormat is not thread
  // safe). (I'm afraid to use a case class here, 'cuz what if they're intern()ed somehow?
  final class DateTimeSetter( val pattern : String ) extends Function3[PreparedStatement,Int,String,Unit]
  {
    val df = new SimpleDateFormat( pattern );

    override def apply( ps : PreparedStatement, i : Int, s : String) : Unit = { 
      try {  ps.setDate(i, new java.sql.Date(df.parse( s ).getTime() ) ) }
      catch{ case t : Throwable => printf("Pattern %s, Bad datum %s\n", pattern, s); throw t; }
    }

    override def equals( other : Any ): Boolean =
      {
	other match
	{
	  case o  : DateTimeSetter => ((this eq o) || (this.pattern == o.pattern));
	  case _  : Any => false;
	}
      }

    override def hashCode : Int = pattern.hashCode;

    def copy = new DateTimeSetter( pattern );
  }

  case class FkdLine( rawLine : String, transformedLine : String, splitLine : Seq[String], probDesc : String )
  {
    def xmlSnippet =
      <line>
	<raw>{ this.rawLine }</raw>
	<transformed>{ this.transformedLine }</transformed>
	<split>{ this.splitLine.map('"' + _ + '"').mkString(",") }</split>
      </line>
  };

  case class FkdLineKeeper( source : NamedDataFileSource, colNames : List[String], fkdLines : List[FkdLine] )
  {
    def xmlSnippet =
      <datasource>
	<sourcename>{ this.source.sourceName }</sourcename>
	<colnames>{ this.colNames.map('"' + _ + '"').mkString(",") }</colnames>
	<lines>
	  { fkdLines.map( _.xmlSnippet ) }
	</lines>
      </datasource>
  }

  def unreadableLinesXml( keepers : Iterable[FkdLineKeeper] ) =
    {
      <unreadable-lines>
	{ keepers.filter( _.fkdLines.length > 0).map( _.xmlSnippet ) }
      </unreadable-lines>
    }

  object Key
  {
    val COL_NAMES = "colNames";
    val LABELS    = "labels";
    val PROLOGUE  = "prologue";
  }

  val emptyColumnInfoArray = new Array[ColumnInfo](0);
  val emptyStringArray     = new Array[String](0);
}

// TODO: Add a parameter to limit the number of rows per batch in inserts.
//       Currently we treat full files as one batch, which is fast but 
//       overindulgent of memory.
abstract class SuperFlexDbArchiver extends Splitter {
  import SuperFlexDbArchiver._; //bring in unqualified reference to companion objects members

  protected val priorTableInfo : TableInfo;

  protected val files          : Seq[NamedDataFileSource];

  protected val debugColumnInspection = false;

  protected val concurrentInserts : Boolean = true;

  /**
   * -1 means unlimited. Only matters of concurrentInserts is true
   */ 
  protected val maxConcurrency : Int = -1;

  protected val quoteColNames : Boolean = false;

  protected val shouldDrop = false;

  //println ("priorTableInfo " + priorTableInfo);

  protected val colSort = dfltColSort( if (priorTableInfo.pkNames == None) Nil; else priorTableInfo.pkNames.get );

  protected val dateFormatPatterns : Iterable[String] = dfltDateFormatPatterns;

  protected val dateTimeFormatPatterns : Iterable[String] = dfltDateTimeFormatPatterns;

  protected val defaultTypeDeclaration = ( "INTEGER", Types.INTEGER );

  protected val bufferSize   = 512 * 1024 * 1024; //500M

  protected val fileEncoding = "ISO-8859-1";

  protected val tmpTablePfx = "tmp_";

  protected val leadingZerosNonNumeric = true;

  // use -1 to treat full files as batches, irrespective of size
  protected val maxBatchSize = 50000;

  // if true, read will die with an exception if there are unreadable lines
  // if false, bad lines will be retained for logging / correcting, and then skipped
  protected val strict = true; 

  protected val mustInfer  : Boolean = true;




  protected def transformColName( colName : String ) : String = colName;

  protected def readMetaData( br: BufferedReader ) : MetaData;

  protected def isNull( datum : String, colName : String ) : Boolean = (datum == null || datum.length == 0);

  protected def readDataLine( br : BufferedReader ) : String = br.readLine();

  /**
   * Note -- this method will only be called oif quoteColNames is
   *         overridden to true.
   */
  protected def quoteColName( colName : String ) = '"' + colName + '"';

  // we need users to set transformation and quoting conventions distinctly,
  // because we do not want to quote when we index columns. So the
  // combined method is final.
  private final def transformQuoteColName( colName : String ) : String =
    {
      var out = transformColName( colName );
      if ( quoteColNames )
	out = quoteColName( out );
      out;
    }

  /**
   *  if table structure inference is disabled, then the synthetic columns must be fully specified. Otherwise, you can use nameOnlyColumnInfos(...) below.
   */ 
  protected def syntheticColumns( f : NamedDataFileSource, colNames : Seq[String] ) : Array[ColumnInfo] = emptyColumnInfoArray;
  protected def syntheticColumnValues( data : Array[String], f : NamedDataFileSource, colNames : Seq[String] ) : Array[String] = emptyStringArray;

  /**
   * Just a utility for specifying synthetic columns whose type should be inferred like "natural" columns 
   */ 
  protected final def nameOnlyColumnInfos( names : String* ) : Array[ColumnInfo] = names.map( ColumnInfo( _, None, None, None, None ) ).toArray;

  /**
   *  when overriding, place the super.prepareTransformDataLines() in out._1, and your own info in out._2.
   *
   *  super.prepareTransformDataLines() must always be called, as SuperFlexDbArchiver uses prepare / transform methods. They are not just hooks.
   *
   *  Note -- any class that overrides prepare/prepareTransform method must override ALL THREE to pass
   *          the left side of the prepObj pair to and or call the superclass method.
   */ 
  protected def prepareTransformFileData( f : NamedDataFileSource, colNames : Array[String] ) : Pair[Any,Any] = Pair( f, colNames );

  /**
   *  when overriding, get the parent class' line using super.prepareTransformUnsplitDataLine( prepObj._1 ), and retrieve
   *  your own info from prepObj._2
   *
   *  super.transformUnsplitDataLines() must always be called, as SuperFlexDbArchiver uses prepare / transform methods. They are not just hooks.
   *
   *  Note -- any class that overrides prepare/prepareTransform method must override ALL THREE to pass
   *          the left side of the prepObj pair to and or call the superclass method.
   */ 
  protected def transformUnsplitDataLine( line : String, prepInfo : Pair[Any,Any] ) : String = line;

  /**
   *  when overriding, get the parent class' line using super.transformUnsplitDataLine( prepObj._1 ), and retrieve
   *  your own info from prepObj._2
   *
   *  super.transformSplitData() must always be called, as SuperFlexDbArchiver uses prepare / transform methods. They are not just hooks.
   *
   *  Note -- any class that overrides prepare/prepareTransform method must override ALL THREE to pass
   *          the left side of the prepObj pair to and or call the superclass method.
   */ 
  protected def transformSplitData( data : Array[String], prepInfo : Pair[Any,Any] ) : Array[String] = 
    {
      val ( f, colNames ) = prepInfo.asInstanceOf[Pair[NamedDataFileSource,Array[String]]];
      val synthCols = syntheticColumns( f, colNames );
      val sz = synthCols.size;
      if (sz == 0)
	data;
      else
	{
	  val synthData = syntheticColumnValues( data, f, colNames );
	  val out = new Array[String]( data.length + sz );
	  // printf("colNames: %s\n", colNames.mkString("[ ", ", "," ]"));
	  // printf("synthCols.size: %s, synthData.size %s\n", synthCols.size.toString, synthData.size.toString);
	  System.arraycopy(data, 0, out, 0, data.length );
	  System.arraycopy(synthData, 0, out, data.length, sz);
	  out;
	}
    }

  protected def afterTableCreate( con : Connection ) : Unit = {};
  protected def afterRowInsert( con : Connection, data : Array[String], f : NamedDataFileSource, colNames : Seq[String] ) : Unit = {};
  protected def afterAllInserts( csrc : ConnectionSource ) : Unit = {};

  /**
   *  Can be set to override the usual inference process for a
   *  given column. The return value is the SQL type that should be
   *  declared and the java.sql.Types code it should be associated
   *  with. Returning None means to use the usual inference
   *  process.
   */ 
  protected def forceInferredType( colName : String, maxLength : Int ) : Option[Pair[String,Int]] = None;


  // we let this be a def, because we'll find it convenient to let this depend
  // on the column count sometimes in order to work around a postgres bug.
  //
  // search the pg-sql mailing list for threads
  //   "Deadlock detection"
  //   "Connection hanging on INSERT apparently due to large batch size and 4 CPU cores"
  //   "deadlocks on JDBC batch inserts into tables with many columns"
  //
  // note that table insepction has completed by the time this "parameter" is accessed,
  // so we can check, e.g. _unifiedNamesToColInfos
  //
  protected def batchFileInserts : Boolean = true;


  /** throw an Exception if not valid */
  protected def validateMetaData( mds : MetaData) : Unit = {}

  protected def padFieldLength( observedMaxLength : Int) : Int = observedMaxLength;

  // Note: this object imposes the sort that will determine the ordering of columns

  protected object FilesInfo
  {
    // note that synth columns are always appended to "natural" file columns in the
    // order they are presented by syntheticColNames. parse and insepction should
    // supply synthetic values appropriately
    def apply( metaDatas : Array[MetaData] ) : FilesInfo =
    {
	val allColNames = mutable.Set.empty[String];
	val colNamesByFile = mutable.HashMap.empty[NamedDataFileSource, Array[String]];
	for (i <- 0 until files.length)
	{
	  val fileColNames = metaDatas(i)( Key.COL_NAMES ).toArray;
	  val synthCols = syntheticColumns( files(i), fileColNames ); 
	  val synthColNames = synthCols.map( _.name );
	  val fileSynthColNames = fileColNames ++ synthColNames;
	  //printf( "fileSynthColNames %s", fileSynthColNames.mkString("[ ", ", ", " ]") );
	  allColNames ++= fileSynthColNames;
	  colNamesByFile += ( files(i) -> fileSynthColNames.toArray );
	}
	
	val fcbcn = mutable.Map.empty[String, Iterable[(NamedDataFileSource, Int)]] ++
	( for ( cn <- allColNames )
	  yield ( Pair(cn, for (f <- files; fcn = colNamesByFile(f).indexOf( cn ); if fcn >= 0) yield ( f, fcn ) ) ) );
	
	new FilesInfo( allColNames, colNamesByFile, fcbcn, colSort );
    }
  }

  protected class FilesInfo( cnames : Iterable[String], 
			     cnbf   : Map[NamedDataFileSource,Array[String]], 
			     fcbcn  : Map[String, Iterable[(NamedDataFileSource, Int)]], 
			     sort   : Ordering[String] )
  {
    val allColumnNames       : SortedSet[String]                                          = immutable.TreeSet.empty(sort) ++ cnames;
    val colNamesByFile       : Map[NamedDataFileSource, Array[String]]                    = immutable.HashMap.empty ++ cnbf;
    val fileColsByColumnName : SortedMap[String, Iterable[ (NamedDataFileSource, Int) ] ] = immutable.TreeMap.empty(sort) ++ fcbcn;
  }

  /**
   * An empty String, rather than a None value, signifies no prologues
   */ 
  private def mergePrologues(metaDatas : Array[MetaData]) : String =
  {
    // XXX hardcoded 8k buffer
    val sb : StringBuilder = new StringBuilder(8192);
    for (i <- 0 until metaDatas.length; prologue = metaDatas(i).getOrElse(Key.PROLOGUE, null); if (prologue != null))
      {
	assert( prologue.length == 1, "Prologues should be parsed to a single string (the first value of the metadata List." );

	sb.append( "Source file: ");
	sb.append( files(i).sourceName );
	sb.append(':');
	sb.append("\n");
	sb.append( prologue(0).trim() );
	sb.append( "\n" );
      }
    sb.toString();
  }

  // if label vals would be interpreted as nulls in data, they
  // are interpreted as nulls here
  protected def createLabelMap(md : MetaData, file : NamedDataFileSource) : Option[Map[String,String]] =
    {
      val colNames : Option[List[String]] = md.get( Key.COL_NAMES );
      val labels   : Option[List[String]] = md.get( Key.LABELS );
      if (colNames == None || labels == None)
	None;
      else if (colNames.get.length == labels.get.length)
	{
	  val cng = colNames.get;
	  val lg = labels.get;
	  val pairs : Seq[ (String, String) ] = 
	    {
	      for (i <- 0 until cng.length; if (! isNull(lg(i), cng(i))))
		yield Pair( cng(i), lg(i) );
	    };
	  val synthPairs =
	    {
	      val synthCols = syntheticColumns( file, cng );
	      for ( ci <- synthCols; label = ci.label.getOrElse( null ); if (! isNull( label, ci.name ) ) )
		yield ( ci.name, label );
	    };
	  Some( immutable.Map( (pairs ++ synthPairs) : _* ) );
	}
      else
	{
	  printf("Uh oh... length of colNames and labels lists don't match. Reporting no labels.\n");
	  None;
	}
    }

  /**
   *  An empty map, rather than a None value, signifies no labels
   */
  protected def findLabels(metaDatas : Array[MetaData], files : Seq[NamedDataFileSource]) : Map[String,String] = 
    {
      val labelMaps = {
	for (i <- 0 until metaDatas.length)
	  yield ( createLabelMap( metaDatas(i), files(i) ) );
      }
      val goodLabelMaps = labelMaps.filter( _ != None ).map( _.get );
      val bindingsUnion : Set[Pair[String,String]] = goodLabelMaps.foldLeft( immutable.Set.empty[Pair[String,String]] )( _ ++ _ );

      // merge duplicate values
      val keySet : Set[String] = immutable.Set( bindingsUnion.map( _._1 ).toSeq : _* );
      val outBuilder = mutable.Map.empty[String,String];
      for ( k <- keySet )
	outBuilder += Pair( k , bindingsUnion.filter( _._1 == k ).map( _._2 ). mkString(" || ") );
      return immutable.Map.empty ++ outBuilder;
    }

  // these options all get filled in sequence
  // it is safe to assume they have values if the function
  // that fills in the data has been called
  //
  // however some vals (empty strings, empty maps) may signify
  // unknown or null
  protected var _metaDatas               : Option[Array[MetaData]]                        = None;
  protected var _labels                  : Option[Map[String,String]]                     = None; //empty if unknown
  protected var _mergedPrologues         : Option[String]                                 = None; //"" if no prologues provided
  protected var _filesInfo               : Option[FilesInfo]                              = None;
  protected var _unifiedTableInfo        : Option[TableInfo]                              = None;
  protected var _unifiedNamesToColInfos  : Option[Map[String,ColumnInfo]]                 = None;
  protected var _maybeQualifiedTableName : Option[String]                                 = None;
  protected var _createDdl               : Option[String]                                 = None;
  protected var _fkdLineKeepers          : Option[Map[NamedDataFileSource,FkdLineKeeper]] = None;

  // pkConstraint need NOT be filled in, if the table being
  // generated has specified no primary key fields. calls
  // to get should always be guarded.
  protected var _pkConstraint           : Option[String]                 = None;

  def archiveFiles( csrc : ConnectionSource )
  {
    inferTableInfo();
    generateCreateDdl();

    if (shouldDrop)
      dropDeindexTable( csrc );

    executeCreateDdl( csrc );
    insertFiles( csrc );
  }

  def archiveFilesNoDups( csrc : ConnectionSource ) : Unit = archiveFilesNoDups( csrc, true );

  def archiveFilesNoDups( csrc : ConnectionSource, imposePkConstraint : Boolean )
  {
    inferTableInfo();
    
    // asserts that _values are set during inference
    val maybeQualifiedTableName = _maybeQualifiedTableName.get;
    val unifiedTableInfo = _unifiedTableInfo.get;
    val tmpTableName = 
      if (unifiedTableInfo.tschema == None ) 
	tmpTablePfx + maybeQualifiedTableName; //schema-less case
      else
	unifiedTableInfo.tschema.get + '.' + tmpTablePfx + unifiedTableInfo.tname.get;

    printf("tmpTableName: %s\n", tmpTableName);

    // if for some reason there is an old shadow of the tempory table,
    // drop it
    dropTable( csrc, tmpTableName ); 

    generateCreateDdl( false, tmpTableName );
    executeCreateDdl( csrc, tmpTableName );
    insertFiles( csrc, this.files, _filesInfo.get, concurrentInserts, tmpTableName ); 

    if ( shouldDrop ) 
      dropDeindexTable(csrc);

    var con : Connection = null;
    var stmt : Statement = null;

    try
    {
      con = csrc.getConnection();
      stmt = con.createStatement();


      stmt.executeUpdate("CREATE TABLE %s AS ( SELECT DISTINCT * FROM %s )".format(maybeQualifiedTableName, tmpTableName));
      stmt.executeUpdate("DROP TABLE %s".format(tmpTableName));
      if ( imposePkConstraint )
	{
	  if ( _pkConstraint != None)
       	    stmt.executeUpdate("ALTER TABLE %s ADD %s".format( maybeQualifiedTableName, _pkConstraint.get ));
	  else
	    println("Could not impose PRIMARY KEY constraint on generated table. No PRIMARY KEY columns specified.");
	}
    }
    finally
    { attemptClose( stmt, con ); }
  }

  def dropDeindexTable( csrc : ConnectionSource ) : Unit =
  {
    dropTable( csrc );
  }

  protected def prologuesDefined() : Boolean =
  {
    // should only be called after inference is complete. effectively asserts...
    _mergedPrologues.get.length > 0;
  }

  final def dropTable( csrc : ConnectionSource ) : Unit =
  {
    withConnection( csrc )
    {
      con =>
	{
	  withStatement( con )
	  { stmt => dropTable( stmt ); }
	}
    }
  }

  final def dropTable( csrc : ConnectionSource, mqtn : String ) : Unit =
  {
    withConnection( csrc )
    {
      con =>
	{
	  withStatement( con )
	  { stmt => dropTable( stmt, mqtn ); }
	}
    }
  }

  final def dropTable( stmt : Statement ) : Unit =
  {
    // effectively asserts that _maybeQualifiedTableName has been set
    val maybeQualifiedTableName = _maybeQualifiedTableName.get;
    dropTable( stmt, maybeQualifiedTableName );
  }

  final def dropTable(stmt : Statement, mqtn : String) : Unit =
  {
    try { stmt.executeUpdate("DROP TABLE %s".format(mqtn)); }
    catch
    { case (exc : SQLException) => ; } //ignore, assume table wasn't there
  }

  private def learnFromMetaDatas() : Unit =
  {
    if ( _metaDatas == None )
      readAllMetaDatas();

    val metaDatas = _metaDatas.get;

    if ( _filesInfo == None )
      _filesInfo = Some( FilesInfo( metaDatas ) );

    val filesInfo = _filesInfo.get;

    val labels : Map[String,String] = findLabels( metaDatas, files );
    this._labels = Some( labels );

    this._mergedPrologues = Some( mergePrologues( metaDatas ) );
  }

  private def learnFromUnifiedTableInfo() : Unit =
  {
    //println( "getting unified table info");
    // effectively asserts that _unifiedTableInfo has been set
    val unifiedTableInfo = _unifiedTableInfo.get;

    val unifiedNamesToColInfos = immutable.TreeMap[String,ColumnInfo]()(colSort) ++ unifiedTableInfo.cols.get.map( ci => (ci.name -> ci) ); //sorted
    this._unifiedNamesToColInfos = Some( unifiedNamesToColInfos );

    if (unifiedTableInfo.pkNames != None)
      { 
	val pkeyExtras = unifiedTableInfo.pkNames.get -- unifiedNamesToColInfos.keySet.toList;
	if (! pkeyExtras.isEmpty )
	  throw new DbArchiverException("Column names specified as primary keys don't exist in the inferred or specified table. Unknown primary keys: " + 
					pkeyExtras.mkString(", ") );
      }

    this._maybeQualifiedTableName = 
      if (unifiedTableInfo.tschema == None)
	unifiedTableInfo.tname;
      else
	Some( unifiedTableInfo.tschema.get + '.' + unifiedTableInfo.tname.get );
  }

  private def inferTableInfo(): Unit =
  {
    if (priorTableInfo.tname == None)
      throw new DbArchiverException("A table name must be specified in priorTableInfo. Currently: " + priorTableInfo);

    if (mustInfer)
      {
	learnFromMetaDatas();
	val metaDatas = _metaDatas.get;
	val filesInfo = _filesInfo.get;
	val labels    = _labels.get;

	// this has the side effect of setting up _fkdLineKeepers
	val inferredColMap : Map[ NamedDataFileSource, Seq[ExaminedColumn] ] = inferFileCols( filesInfo );

	val inferredCols : SortedMap[String, ExaminedColumn] = 
	  {
	    immutable.TreeMap.empty[String, ExaminedColumn]( colSort ) ++
	      ( for ( cn <- filesInfo.allColumnNames )
		  yield cn -> (filesInfo.fileColsByColumnName( cn ).map( fc => inferredColMap( fc._1 ).apply(fc._2) ) ).reduceLeft( _ && _ ) );
	  }

	val inferredColInfos = inferredCols.map( tup => new ColumnInfo(tup._1, 
								       if (labels == None || ! labels.contains(tup._1)) None; else Some( labels(tup._1) ),
								       Some(tup._2.bestTypeDeclaration._1), 
								       Some(tup._2.bestTypeDeclaration._2), 
								       Some(tup._2.setter) ) ).toSeq;
	/*
	val synthCols = syntheticColumns;

	val allColMap = mutable.Map.empty[String,ColumnInfo];
	allColMap ++= inferredColInfos.map( ci => (ci.name -> ci) );
	allColMap ++= synthCols.map( ci => ( ci.name -> ci ) );
	if (priorTableInfo.cols != None)
	  allColMap ++= priorTableInfo.cols.get.map( ci => ( ci.name -> ci ) );
	val allColSort = immutable.TreeMap.empty[String, ColumnInfo](colSort) ++  allColMap;
	val allColSeq = allColSort.map( _._2 ).toSeq;
	*/ 

	val inferredTableInfo = new TableInfo( None, None, Some(inferredColInfos), None ); // name and any schema or pkeys should be set on priorTableInfo

	// unified table info will contain information about synthetic colums by inference (they were included in filesInfo)
	val unifiedTableInfo = priorTableInfo.reconcileOver( inferredTableInfo );

	//val unifiedTableInfo = TableInfo( priorTableInfo.tschema, priorTableInfo.tname, Some( allColSeq ), priorTableInfo.pkNames );
	this._unifiedTableInfo = Some( unifiedTableInfo );

	//printf("unifiedTableInfo: %s\n", unifiedTableInfo);

	learnFromUnifiedTableInfo();

	val unifiedNamesToColInfos = _unifiedNamesToColInfos.get;
	val extraCols = immutable.Set(unifiedNamesToColInfos.keySet.toSeq : _*) -- filesInfo.allColumnNames;
	if (! extraCols.isEmpty )
	  throw new DbArchiverException("Information is specified (via priorTableInfo) about columns not in the data. Extras: " + extraCols.mkString(", "));
      }
    else
      {
	// we still have to make sure all the member variables that
	// would normally have been filled by inspection get filled.

	learnFromMetaDatas();

	// we still need to infer what synth columns will be required
	// metaDatas was set above
	val metaDatas = _metaDatas.get;

	val synthCols = 
	  {
	    val fileSynthCols = for (i <- 0 until metaDatas.length) yield syntheticColumns( files(i), metaDatas(i).apply( Key.COL_NAMES ) );

	    val tmp = mutable.Map.empty[String, ColumnInfo];
	    for ( fileseq <- fileSynthCols; ci <- fileseq )
	    {
	      val check = tmp.get( ci.name );
	      if (check != None)
		tmp += ( ci.name -> check.get.reconcile( ci ));
	      else
		tmp += ( ci.name -> ci );
	    }
	    tmp.map( _._2 );
	  };

	if (synthCols.isEmpty)
	  this._unifiedTableInfo = Some( priorTableInfo );
	else
	  this._unifiedTableInfo = Some( priorTableInfo.reconcileOver( TableInfo( None, None, Some(synthCols), None ) ) );
	
	printf("priorTableInfo: %s\n", priorTableInfo);

	printf("set table info: %s\n", this._unifiedTableInfo);

	learnFromUnifiedTableInfo();
	this._fkdLineKeepers = Some( createEmptyFkdLineKeepers() );
      }
  }

  // for when we are skipping inference and fully specifying the expected table structure
  // in this case, we don't have the opportunity to pre-inspect files for bad records
  private def createEmptyFkdLineKeepers() : Map[NamedDataFileSource,FkdLineKeeper] = 
  { immutable.Map( files.map( f => ( f, FkdLineKeeper(f, _filesInfo.get.colNamesByFile(f).toList, Nil) ) ) : _* );  } 


  private def generateCreateDdl() : Unit = generateCreateDdl( true, _maybeQualifiedTableName.get /* asserts _maybeQualifiedTableName set */ );

  private def generateCreateDdl( includePkConstraint : Boolean, maybeQualifiedTableName : String ) : Unit =
  {
    val unifiedTableInfo = _unifiedTableInfo.get;
    val unifiedNamesToColInfos =  _unifiedNamesToColInfos.get;

    //printf("In generateCreateDdl( ... ) -- unifiedTableInfo %s\n", unifiedTableInfo);

    // XXX: 4096 is hardcoded here...
    val sb : StringBuilder = new StringBuilder(4096);
    sb.append("CREATE TABLE ");
    sb.append( maybeQualifiedTableName );
    val decls : List[String] = 
      {
	val rawPkColNames = unifiedTableInfo.pkNames;
	val noPkDecls = unifiedNamesToColInfos.elements.toList.map( tup => ( transformQuoteColName( tup._2.name ) + " " + tup._2.sqlTypeDecl.get ) );
	if (rawPkColNames != None && rawPkColNames.get != Nil)
	  {
	    val pkConstraint = "PRIMARY KEY( " + rawPkColNames.get.map( transformQuoteColName _ ).mkString(", ") + " )";
	    this._pkConstraint = Some( pkConstraint ); 

	    if (includePkConstraint)
	      noPkDecls:::pkConstraint::Nil;
	    else
	      noPkDecls;
	  }
	else
	  noPkDecls;
      };
    sb.append( decls.mkString(" ( ", ", ", " )") );
    
    _createDdl = Some( sb.toString() );
  }

  private def executeCreateDdl( csrc : ConnectionSource )
  {
    val maybeQualifiedTableName = _maybeQualifiedTableName.get; // effectively asserts that _maybeQualifiedTableName has been set
    executeCreateDdl( csrc, maybeQualifiedTableName );
  }

  private def executeCreateDdl( csrc : ConnectionSource, maybeQualifiedTableName : String )
  {
    val createDdl = _createDdl.get; // effectively asserts that _createDdl has been set

    var con : Connection = null;
    var stmt : Statement = null;
    
    try
    {
      con = csrc.getConnection();
      stmt = con.createStatement();
      
      // if ( drop )
      // 	{
      // 	  printf("Attempting to drop table '%s', if present.\n", maybeQualifiedTableName);
      // 	  drop( stmt );
      // 	}

      printf("Creating table '%s'.\n", maybeQualifiedTableName);
      println( createDdl );

      stmt.executeUpdate( createDdl );

      afterTableCreate( con );

      println("CREATE was executed without exceptions.");
    }
    finally
    { attemptClose( stmt, con ); }
  }

  private def insertFile( csrc : ConnectionSource, f : NamedDataFileSource, filesInfo : FilesInfo ) : Unit =  withConnection( csrc ) { insertFile( _, f, filesInfo) }

  private def insertFile( csrc : ConnectionSource, f : NamedDataFileSource, filesInfo : FilesInfo, maybeQualifiedTableName : String ) : Unit =
    withConnection( csrc ) { insertFile( _, f, filesInfo, maybeQualifiedTableName) }

  private def insertFile( con : Connection, f : NamedDataFileSource, filesInfo : FilesInfo ) : Unit =
    {
      // effectively assets that member variables have been set...
      var maybeQualifiedTableName = _maybeQualifiedTableName.get;
      insertFile( con, f, filesInfo, maybeQualifiedTableName );
    }

  private def insertFile( con : Connection, f : NamedDataFileSource, filesInfo : FilesInfo, maybeQualifiedTableName : String ) : Unit =
    {
      //printf( "insertFile -- %s\n", f.sourceName );

      // effectively assets that member variables have been set...
      val unifiedTableInfo = _unifiedTableInfo.get;
      val unifiedNamesToColInfos = _unifiedNamesToColInfos.get;
      val fkdLineKeepers = _fkdLineKeepers.get;
      val myFkdLineKeeper = fkdLineKeepers(f); // there should be one for every file
      val myFkdRawLines = immutable.HashSet( myFkdLineKeeper.fkdLines.map( _.rawLine ) : _* );
      //printf("%s -- myFkdRawLines\n%s\n", f.sourceName, myFkdRawLines.mkString("\n"));

      var skipped : Int = 0;

      var br : BufferedReader = null;
      var ps : PreparedStatement = null;
      try
      {
	val fileColNames = Array[String]( filesInfo.colNamesByFile( f ) : _* );
	val psColList = fileColNames.map( transformQuoteColName( _ ) ).mkString("( ", ", ", " )"); // in File order, not table order
	val psInsertionPoints = ( (for (fcn <- fileColNames) yield '?').mkString("( ", ", ", " )") );
	
	
	// generate appropriate data setting functions, mixing inferred with preset columns
	val setters  = for ( cn <- fileColNames) yield copyMaybeSetter( unifiedNamesToColInfos( cn ).setter ); //not all setters are thread safe... we must copy 
	val typeCode = for ( cn <- fileColNames) yield unifiedNamesToColInfos( cn ).typeCode; 
	
	val insertStmt = String.format("INSERT INTO %s %s VALUES %s", maybeQualifiedTableName, psColList, psInsertionPoints );

	//printf("insertStmt: %s\n", insertStmt);

	ps = con.prepareStatement( insertStmt );
	br = f.createBufferedReader( bufferSize, fileEncoding );
	readMetaData( br ); // skip preliminaries...

	val prepObj = prepareTransformFileData( f, fileColNames ); 
	var batchCount = 0;
	var line = readDataLine( br );

	//println("line: " + line);

	while ( goodLine( line ) )
	{
	  if (! myFkdRawLines( line ) ) // if the line is known to be fkd, we don't try to parse, read, and insert it
	    {
	      line = transformUnsplitDataLine( line, prepObj );
	      val data = transformSplitData( split( line ), prepObj );

	      /*
	      printf( "data %s\n", data.mkString("[ ", ", " , " ]") );
	      printf( "psColList %s\n", psColList );
	      printf( "psInsertionPoints %s\n", psInsertionPoints );
	      */ 

	      for (i <- 0 until data.length)
		{
		  if ( isNull( data(i), fileColNames(i) ) )
		    ps.setNull( i + 1, typeCode(i).get); // XXX: should guard this get
		  else
		    {
		      try {
			setters(i).get.apply(ps, i + 1, data(i)); // XXX: should guard this get, ps slots are indexed by 1, arrays are indexed by zero
		      } catch {
			case e : Exception => {
			  printf("BAD VALUE: COLUMN %d of\n%s\n",i+1,line);
			  throw e;
			}
		      }
		    }
		}
	      // println(ps);
	  
	      if ( batchFileInserts )
		{
		  ps.addBatch();
		  batchCount += 1;
		  if (batchCount == maxBatchSize)
		    {
		      printf("%s: batch size limit %d reached. executing, then resetting.\n", f.sourceName, batchCount);
		      ps.executeBatch();
		      batchCount = 0;
		      printf("%s: executed batch and reset batch size count.\n", f.sourceName);
		    }
		}
	      else
		ps.executeUpdate();
	      afterRowInsert( con, data, f, fileColNames );
	    }
	  else
	    {
	      skipped += 1;
	      printf("%s: Skipped bad line: %s\n", f.sourceName, line );
	    }
	  
	  line = readDataLine( br );
	}
	if (batchFileInserts)
	  ps.executeBatch();

	printf("%s: Skipped %d lines. Expected to skip %d lines.\n", f.sourceName, skipped, myFkdLineKeeper.fkdLines.length);
      }
      catch
      { case e : SQLException => { printThrowable(e); throw e; } }
      finally
      { attemptClose( ps, br ); }
    }

  def insertFiles(csrc : ConnectionSource)
  { 
    // effectively asserts that _filesInfo and _maybeQualifiedTableName have been set
    insertFiles(csrc, this.files, _filesInfo.get, concurrentInserts, _maybeQualifiedTableName.get ); 
  }
  
  def insertFiles(csrc : ConnectionSource, files : Collection[NamedDataFileSource], filesInfo : FilesInfo, concurrent : Boolean, maybeQualifiedTableName : String) : Unit =
    {
      if (concurrent)
	{
	  val fcn : (NamedDataFileSource) => Option[Throwable] = 
	    (f : NamedDataFileSource) =>
	      { try { insertFile( csrc, f, filesInfo, maybeQualifiedTableName ); None; }
	       catch { case (t : Throwable) => { printThrowable(t); Some(t); } }; }

	  var processor = CollectionProcessor( fcn, maxConcurrency );
	  processor.process( files )
	}
      else
	{
	  withConnection( csrc ) 
	  { con => files.foreach( insertFile( con, _, filesInfo, maybeQualifiedTableName ) ); }
	}

      afterAllInserts( csrc );
    }

  private def printThrowable( t : Throwable ) : Unit =
    {
      if ( t != null )
	{
	  t.printStackTrace();
	  if ( t.isInstanceOf[SQLException] )
	    printThrowable( t.asInstanceOf[SQLException].getNextException )
	}
    }
  

  //private def f2br(f : File, bs : Int) : BufferedReader = { new BufferedReader( new InputStreamReader( new FileInputStream( f ), fileEncoding ), bs ) } 
  
  //private def f2br(f : File) : BufferedReader = { f2br( f, bufferSize ); }
  
  // XXX: hardcoded 8K starting buffer for headers
  private def buildStreams() : Seq[BufferedReader] = 	
    {
      var buildStreams = new ArrayBuffer[BufferedReader];
      try
      {
	for (f <- files)
	  buildStreams += f.createBufferedReader(8192, fileEncoding);
	buildStreams.readOnly;
      }
      catch
      { case ex : Exception => buildStreams.foreach( attemptClose _ ); throw ex; }
    };
  
  private def readAllMetaDatas() : Unit =
    {
      val streams = buildStreams();

      try
      {	_metaDatas = Some( for( br <- streams.toArray[BufferedReader] ) yield ( readValidateMetaData( br ) ) ); }
      finally
      { streams.foreach( attemptClose _ ); }
    }

  private def readValidateMetaData( br : BufferedReader ) : MetaData =
    {
      val out = readMetaData( br );
      validateMetaData( out );
      out;
    }

  // as a side effect, sets up _fkdLineKeepers
  private def inferFileCols( fi : FilesInfo ) : Map[ NamedDataFileSource, Seq[ExaminedColumn] ] =
  {
    // since synthetic file cols are set up in filesInfo, they will be passed to doInferFile
    def doInferFile( f : NamedDataFileSource, colNames : Array[String] )  : (NamedDataFileSource, Seq[ExaminedColumn], FkdLineKeeper) =
      {
	var numCols : Int = colNames.length;
	printf("Examining %s, which has %d columns.\n", f.sourceName, numCols);
	val br = f.createBufferedReader( bufferSize, fileEncoding );
	readMetaData( br ); //skip preliminaries
	try 
	{
	  val (recSeq, flk) = examineColumns( br, colNames, f );
	  ( f, recSeq.map( _.toExaminedColumn), flk )
	}
	finally { br.close(); }
      }

    val xformFcn = (pair : Pair[NamedDataFileSource,Array[String]]) => { doInferFile( pair._1, pair._2 ) };
    val processor = CollectionProcessor( xformFcn, maxConcurrency );
    val triples = processor.process( files.map( f => (f, fi.colNamesByFile(f)) ) );

    /*
    var cnt = 0;
    val actors = files.map
    {
      f =>
	{
	  val a = new StreamColumnsExaminer(Actor.self);
	  a.start(); cnt +=1;
	  printf("%s started. [actors running %d]\n", a, cnt);
	  a ! ( f, fi.colNamesByFile(f)); 
	  a
	}
    };

/*
    val actors = new Array[Actor]( files.length );
    for ( i <- 0 until files.length )
      {
	val f = files(i);
	val a = new StreamColumnsExaminer(Actor.self);
	actors(i) = a;
	a.start();
	println(a + " started");
	a ! ( f, fi.colNamesByFile(f)); 
      }
 */ 

    println( "All examination actors started." );

    // note that we expact as many replies as there are actors, so we just iterate over the actors collection

    val triples : Iterable[ Tuple3[NamedDataFileSource,Seq[ExaminedColumn],FkdLineKeeper] ] = 
    {
      for (a <- actors) yield 
	{ 
	  Actor.self.receive 
	  {
	    case msg : (NamedDataFileSource, Seq[ExaminedColumn], FkdLineKeeper) =>  
	      {
		cnt -= 1;
		printf("examination actor completed with message. [%d still running]\n", cnt);
		msg; 
	      }
	    case t : Throwable => 
	      {
		// if an Actor breaks kill everything...
		try { actors.foreach( _ ! 'exit ); } catch { case e : Throwable => printThrowable( e ); }
		throw t; 
	      }
	    case x @ _ => { val s : String = "UNEXPECTED MESSAGE from StreamColumnsExaminer: " + x; println(s); throw new Exception(s); }
	  } 
	} 
    };


/*
    val triples : Iterable[ Tuple3[NamedDataFileSource,Seq[ExaminedColumn],FkdLineKeeper] ] = 
    {
      var out : List[ Tuple3[NamedDataFileSource,Seq[ExaminedColumn],FkdLineKeeper] ] = Nil;
      while (cnt > 0) 
	{ 
	  Actor.self.receive 
	  {
	    case msg : (NamedDataFileSource, Seq[ExaminedColumn], FkdLineKeeper) =>  
	      {
		cnt -= 1;
		printf("examination actor completed with message. [%d still running]\n", cnt);
		out = msg::out;
	      }
	    case t : Throwable => 
	      {
		// if an Actor breaks kill everything...
		try { actors.foreach( _.exit ); } catch { case e : Throwable => printThrowable( e ); }
		throw t; 
	      }
	    case x @ _ => { val s : String = "UNEXPECTED MESSAGE from StreamColumnsExaminer: " + x; println(s); throw new Exception(s); }
	  } 
	} 
      out
    };

 */ 

*/

    val keepers : Map[NamedDataFileSource,FkdLineKeeper] = immutable.Map.empty ++ triples.map( trip => (trip._3.source -> trip._3 ) );
    this._fkdLineKeepers = Some( keepers );

    val pairs = triples.map( tup => Pair(tup._1, tup._2) )
    immutable.HashMap( pairs.toSeq  : _*);
  }

  private[SuperFlexDbArchiver] case class ExaminedColumn(val colName : String,
				    val booleanOnly : Boolean,
				    val numericOnly : Boolean, 
				    val integerOnly : Boolean, 
				    val fixedLength : Option[Int], 
				    val maxLength : Int, // if maxLength < 0, all known entries of this colum are null
				    val dateFormatStr : Option[String],
				    val dateTimeFormatStr : Option[String])
  {
      def &&( other : ExaminedColumn ) = 
	{
	  require( this.colName == other.colName );

	  ExaminedColumn(colName,
			 this.booleanOnly && other.booleanOnly,
			 this.numericOnly && other.numericOnly,
			 this.integerOnly && other.integerOnly,
			 if (this.fixedLength == other.fixedLength) { fixedLength } else { None },
			 this.maxLength max other.maxLength,
			 if (this.dateFormatStr == other.dateFormatStr) { dateFormatStr } else { None },
			 if (this.dateTimeFormatStr == other.dateTimeFormatStr) { dateTimeFormatStr } else { None } );
	}

    // XXX: Put all this type crap into some formal enumeration
    // TODO: Make this customizable to different databases
    val bestTypeDeclaration : Pair[String, Int] = 
      {
	val forced = forceInferredType( colName, maxLength );
	if (forced != None )
	  {
	    val out = ( forced.get._1.toUpperCase, forced.get._2 );
	    
	    assert( ("TIMESTAMP"::"DATE"::"INTEGER"::"BIGINT"::"DOUBLE PRECISION"::"BOOLEAN"::Nil).contains( out._1 ) || out._1.indexOf("CHAR") >= 0,
		    "Forced column types must be one of 'TIMESTAMP', 'DATE', 'INTEGER', 'BIGINT', 'DOUBLE PRECISION', 'BOOLEAN', or something containg 'CHAR'" );

	    if (debugColumnInspection)
	      printf("[%s] Column type '%s' was forced by a subclass override.", colName, out._1 );

	    out
	  }
	else if (maxLength < 1) // all null
	  defaultTypeDeclaration;
	else if (booleanOnly)
	  ("BOOLEAN", Types.BOOLEAN)
	else if (dateTimeFormatStr != None)
	  ("TIMESTAMP", Types.TIMESTAMP);
	else if (dateFormatStr != None)
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
	  case "BOOLEAN"          => booleanSetter;
	  case "TIMESTAMP"        => timestampSetter( dateTimeFormatStr.get );
	  case "DATE"             => dateSetter( dateFormatStr.get );
	  case "INTEGER"          => integerSetter;
	  case "BIGINT"           => bigintSetter;
	  case "DOUBLE PRECISION" => doublePrecisionSetter;
	  case _                  => charTypeSetter;
	}
      }
  }

  private def goodLine( line : String ) : Boolean = { line != null && line.trim().length > 0 } 

/*
  private class StreamColumnsExaminer(val mgr : Actor) extends Actor
  {
    override def act : Unit =
      {
	try
	{
	  val ( f, recs, blk ) : (NamedDataFileSource, Seq[Rec], FkdLineKeeper) = 
	    receive
	    {
	      case ( f : NamedDataFileSource, colNames : Seq[String] ) => 
	      {
		var numCols : Int = colNames.length;
		printf("Examining %s, which has %d columns.\n", f.sourceName, numCols);
		val br = f.createBufferedReader( bufferSize, fileEncoding );
		readMetaData( br ); //skip preliminaries
		try 
		{
		  val examination = examineColumns( br, colNames, f );
		  ( f, examination._1, examination._2 ); 
		}
		finally { br.close(); }
	      }
	      case 'exit => exit();
	      case msg => { val s = "unexpected message " + msg; println(s); throw new Exception(s); }
	      //case (a : Any) => printf("%s: Unexpected message -- %s", this, a);
	    }
	  mgr ! ( f, (for (rec <- recs) yield rec.toExaminedColumn).toSeq, blk );
	}
	catch
	{ case t : Throwable => { printThrowable( t ); mgr ! t; throw t; } }
      }

  }
*/
    // colNames includes synthetic colNames (appended at end), as they are set up in filesInfo
    private def examineColumns(br : BufferedReader, colNames : Array[String], f : NamedDataFileSource) : Pair[Seq[Rec], FkdLineKeeper] =
      {
	var fkdLines : List[FkdLine] = Nil;

	var numCols = colNames.length;
	val out = new Array[Rec](numCols);
	for (i <- 0 until numCols)
	  out(i) = new Rec(colNames(i));

	// the prepare / transform API takes care of appending any synthetic values
	// to the parsed data
	val prepObj = prepareTransformFileData( f, colNames ); 

	var rawLine = readDataLine( br );

	while (goodLine(rawLine))
	{
	  val line = transformUnsplitDataLine( rawLine, prepObj );
	  //println(line);
	  //print('.');
	  var data = transformSplitData( split(line), prepObj );

	  if ( data.length == numCols )
	    {
	      for (i <- 0 until numCols)
		out(i).update(data(i));
	    }
	  else 
	    {
	      var fkd = FkdLine( rawLine, line, data, String.format("data.length (%s) and numCols (%s) should be equal.", data.length.asInstanceOf[Object], numCols.asInstanceOf[Object]) );
	      //printf("BAD LINE: %S\n", fkd);
	      if (strict)
		throw new DbArchiverException("UNREADABLE LINE in %s! ABORTING (since we are in strict mode). INFO: %s".format(f.sourceName, fkd.toString));
	      else
		fkdLines = fkd::fkdLines;
	    }

	  rawLine = readDataLine( br );
	}

	Pair(out, FkdLineKeeper( f, colNames.toList, fkdLines.reverse ) );
      }

    /**
     * extraValidators is necessary because sometimes SimpleDateFormat fails to
     * reject non-exact matches, filling in missing info, even with lenient set to
     * false. The params of extraValidator should be maybeDate and patternStr, in
     * that order
     */
    private[SuperFlexDbArchiver] class DateFormatGuesser( colName : String, patternStrs : Iterable[String], extraValidator : (String,String)=>Boolean )
    {

      def this( colName : String, patternStrs: Iterable[String] )
      { this( colName, patternStrs, null) }

      import scala.collection.mutable.Queue;

      var inPlay = new Queue[SimpleDateFormat];
      inPlay ++= patternStrs.map( new SimpleDateFormat( _ ) );
      inPlay.foreach( _.setLenient( false ) );

      var anyHope = !inPlay.isEmpty;

      def check( maybeDate : String) : Unit = 
	{
	  def check(maybeDate : String, df : SimpleDateFormat) : Boolean =
	    {
	      var out =
		{
		  try { df.parse( maybeDate ); true; }
		  catch { case ex : ParseException => false; }
		}
	      
	      if ( debugColumnInspection )
		if (! out)
		  printf("[%s] Pattern %s ruled out by datum '%s'\n", colName, df.toPattern, maybeDate);

	      if ( out && extraValidator != null )
		{
		  out = extraValidator( maybeDate, df.toPattern );

		  if ( debugColumnInspection && !out)
		    printf("[%s] Datum %s conforms to pattern %s, but is ruled out extra validator.\n", colName, maybeDate, df.toPattern);
		}
	      out;
	    }

	  inPlay.dequeueAll( ! check( maybeDate, _ ) );
	  anyHope = !inPlay.isEmpty;
	  
	  if (!anyHope && debugColumnInspection)
	    printf("[%s] now hopeless in %s.\n", colName, this);
	}

      def guess = { inPlay.front; }
    }	

    class Rec( colName : String )
    {
      var booleanOnly : Boolean = true;
      var numericOnly : Boolean = true;
      var integerOnly : Boolean = true;
      var fixedLength : Int = -1; // -2 means not fixed, -1 means unknown, nonneg means putatively fixed
      var maxLength : Int = -1;

      val dfg  : DateFormatGuesser = new DateFormatGuesser( colName, dateFormatPatterns, extraDateValidator);
      val dtfg : DateFormatGuesser = new DateFormatGuesser( colName, dateTimeFormatPatterns, extraDateValidator);

      //val dfg  : DateFormatGuesser = new DateFormatGuesser( colName, dateFormatPatterns);
      //val dtfg : DateFormatGuesser = new DateFormatGuesser( colName, dateTimeFormatPatterns);

      def toExaminedColumn = ExaminedColumn(colName,
					    booleanOnly,
					    numericOnly, 
					    integerOnly,
					    if (fixedLength > 0) { Some(fixedLength) } else { None },
					    maxLength,
					    if ( dfg.anyHope ) { Some( dfg.guess.toPattern ) } else { None },
					    if ( dtfg.anyHope ) { Some( dtfg.guess.toPattern ) } else { None }
					  );

      //note that we don't treat all zero strings of any length as containing "leading zeros"
      def hasLeadingZeros( datum : String ) : Boolean =
      {
	val m = leadingZerosRegex.findFirstMatchIn(datum);
	(m != None && m.get.start == 0 && !datum.forall( _ == '0')) 
      }

      def update( datum : String ) : Unit =
	{
	  if (datum == null || isNull(datum, colName)) //we can draw no inferences from nulls
	    return; 

	  booleanOnly = ( booleanOnly && (asBoolean(datum) != None) ); // does &&= work?
	  
	  if (numericOnly) // integerOnly implies numericOnly
	    {
	      if (datum.length == 0) //we interpret this case as empty string, since we know it isn't interpreted as null
		{
		  numericOnly = false;
		  integerOnly = false;
		  if ( debugColumnInspection )
		    printf("[%s] Numeric types ruled out by empty string not interpreted as NULL\n", colName);
		}
	      else if ( (! datum.forall( "-0123456789.eE".contains( _ ) )) || 
                        (! "0123456789.".contains(datum.last)) ) //we accept the letter E for representations in scientific notation
		{
		  numericOnly = false;
		  integerOnly = false;
		  if ( debugColumnInspection )
		    printf("[%s] Numeric types ruled out by datum '%s', which cannot be interpreted as a number.\n", colName, datum);
		}
	      else if ( numericOnly && datum.indexOf('-') > 0 && datum.toLowerCase().indexOf("e-") != 0) //we have to deal with negatives from scientific notation... yes i should use parseDouble or NumberFormat...
		{
		  numericOnly = false;
		  integerOnly = false;
		  if ( debugColumnInspection )
		    printf("[%s] Numeric types ruled out by datum '%s', which cannot be interpreted as a number because of an internal dash.\n", colName, datum);
		}
	      else if( leadingZerosNonNumeric && numericOnly && hasLeadingZeros( datum ) )
		{
		  numericOnly = false;
		  integerOnly = false;
		  if ( debugColumnInspection )
		    printf("[%s] Numeric types ruled out by datum '%s', since config param 'leadingZerosNonNumeric' is true, and the datum contains leading zeros.\n", colName, datum);
		}
	      else if ( integerOnly && (datum.contains('.') || datum.contains('e') || datum.contains('E') ) )
		{
		  integerOnly = false;
		  if ( debugColumnInspection )
		    printf("[%s] Integral types ruled out by datum '%s', which contains a '.'\n", colName, datum);
		}
	    }

	  if (fixedLength > -2)
	    {
	      if (fixedLength >= 0)
		{
		  val newFixedLength = if (datum.length == fixedLength) { datum.length } else { -2 }; //a variation, -2 means not fixed

		  if ( debugColumnInspection && newFixedLength == -2)
		    printf("[%s] Puative fixed length of %d invalidated by '%s' of length %d.\n", colName, fixedLength, datum, datum.length);
		  
		  fixedLength = newFixedLength;
		}
	      else
		{
		  fixedLength = datum.length;

		  if ( debugColumnInspection )
		    printf("[%s] Puative fixed length of %d set by first non-null value, '%s'.\n", colName, fixedLength, datum);
		}
	    }

	  maxLength = maxLength max datum.length;

	  if ( dfg.anyHope )
	    dfg.check ( datum );

	  if (dtfg.anyHope)
	    dtfg.check ( datum );
	}
    }
}



