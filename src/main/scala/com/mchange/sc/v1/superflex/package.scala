package com.mchange.sc.v1.democognos.dbutil;

import java.io.{BufferedReader,BufferedWriter,File,FileReader,FileWriter,PrintWriter};
import java.sql.{Connection,DriverManager,Statement,SQLException};
import com.mchange.v2.csv.FastCsvUtils;
import com.mchange.sc.v1.sql.ResourceUtils._;
import com.mchange.sc.v1.util.ClosableUtils;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.immutable.Set;
import scala.collection.immutable.SortedSet;
import scala.collection.immutable.TreeSet;
import scala.collection.immutable.HashSet;

object DbUtils
{
  def attemptCreate (stmt : Statement, objName : String, createDdl : String ) : Unit = 
  {
    try
    { stmt.executeUpdate(createDdl); }
    catch
    {
      // assume an SQLException means that the object is already there.
      case (t : SQLException) =>
	{ 
	  printf("Failed to create %s. Verify that it is already present.\n", objName);
	  t.printStackTrace();
	}
    }
  }

  def attemptCreateSchema( csrc : ConnectionSource, sname : String ) = 
    withConnection( csrc ) { 
      con => withStatement( con ) { 
	stmt => DbUtils.attemptCreate( stmt, sname, "CREATE SCHEMA " + sname );
      }
    }
  private def headers( f : File ) : Array[String] = {
    headers( f, None );
  }

  private def headers( f : File, xform : Option[Function1[Array[String],Array[String]]] ) : Array[String] = {
    //println(f);

    // XXX: hardcoded buffer-size of 8K
    ClosableUtils.withClosable( () => new BufferedReader( new FileReader( f ), 8192 ) ) {
      br => headers( br, xform );
    }
  }

/*
  def getOrElsePrint( thang : Map[String,String], key : String, dflt : String ) : String = {
    val out = thang.getOrElse( key, dflt );
    if (out != dflt)
      printf("found key: %s\n", key);
    out;
  }
*/

  // BufferedReader should be prior to the first line
  private def headers( br : BufferedReader, xform : Option[Function1[Array[String],Array[String]]] ) : Array[String] = {
    val raw =  FastCsvUtils.splitRecord( br.readLine() );
    if ( xform != None ) {
      xform.get.apply( raw );
    } else {
      raw;
    }
  }


  def allColNamesOneRowHeaderCsv( files : Iterable[File] ) : SortedSet[String] = {
    allColNamesOneRowHeaderCsv( files, None );
  }
  def allColNamesOneRowHeaderCsv( files : Iterable[File], xform : Option[Function1[Array[String],Array[String]]] ) : SortedSet[String] = {
    files.foldLeft( (new TreeSet[String]).asInstanceOf[SortedSet[String]] )( (set : SortedSet[String], f : File) => set ++ headers( f, xform ) );
  }

  def findBounds( files : Iterable[File], excludeKeyCols : Iterable[String], numBounds : Int ) : List[String] = {
    findBounds( files, excludeKeyCols, numBounds, None )
  }

  def findBounds( files : Iterable[File], excludeKeyCols : Iterable[String], numBounds : Int, xform : Option[Function1[Array[String],Array[String]]] ) : List[String] = {
    var colList = (allColNamesOneRowHeaderCsv( files, xform ) -- excludeKeyCols).toList;
    val spaceBetween = Math.round( Math.ceil( colList.length.asInstanceOf[Float] / (numBounds + 1) ).asInstanceOf[Float] ); 

    println( "colList.length: " + colList.length );
    println( "spaceBetween: " + spaceBetween );
    println( (spaceBetween until colList.length by spaceBetween).mkString(", ") );
    (spaceBetween until colList.length by spaceBetween).map( colList(_) ).toList;
  }

  def csvColumnCount( f : File ) : Int = headers(f).size; //no need to transform, number of columns must be invariant to transformations

  def splitOneRowHeaderCsvFile( splitMe : File, primaryKeyColNames : Set[String], maxCols: Int, splitBufferSize : Int, splitFileDir : File ) : Collection[File] = {
    splitOneRowHeaderCsvFile( splitMe, primaryKeyColNames, maxCols, splitBufferSize, splitFileDir, None );
  }

  def splitOneRowHeaderCsvFile(splitMe : File, 
			       primaryKeyColNames : Set[String], 
			       maxCols: Int, 
			       splitBufferSize : Int, 
			       splitFileDir : File, 
			       xform : Option[Function1[Array[String],Array[String]]] ) : Collection[File] = {
    assert( splitMe.getName().endsWith(".csv") );

    var totalCols = csvColumnCount( splitMe );
    //printf("%s: totalCols: %d, maxCols: %d\n", splitMe, totalCols, maxCols);
    if (totalCols > maxCols) {
      val baseTableName = splitMe.getName().substring(0, splitMe.getName().length() - 4);
      var numFiles = totalCols / maxCols + 1;
      var outFiles = (1 to numFiles).map( n => new File( splitFileDir, baseTableName + "_"+ n + ".csv" ) ).toList;
      printf("Splitting %s into %s\n", splitMe, outFiles.mkString(", "));
      divideOneRowHeaderCsvFile( primaryKeyColNames, splitMe, outFiles, splitBufferSize, xform );
      outFiles;
    } else {
      splitMe::Nil;
    }
  }

  def splitOneRowHeaderCsvFileByBounds(splitMe : File, 
				       allColNames : List[String],
				       primaryKeyColNames : Set[String], 
				       boundaryCols : List[String], 
				       splitBufferSize : Int, 
				       splitFileDir : File ) : Collection[File] = {
    splitOneRowHeaderCsvFileByBounds(splitMe, allColNames, primaryKeyColNames, boundaryCols, splitBufferSize, splitFileDir, None);
  }

  def splitOneRowHeaderCsvFileByBounds(splitMe : File, 
				       allColNames : List[String],
				       primaryKeyColNames : Set[String], 
				       boundaryCols : List[String], 
				       splitBufferSize : Int, 
				       splitFileDir : File,
				       xform : Option[Function1[Array[String],Array[String]]] ) : Collection[File] = {
    assert( splitMe.getName().endsWith(".csv") );

    val baseTableName = splitMe.getName().substring(0, splitMe.getName().length() - 4);
    var numFiles = boundaryCols.length + 1;
    var outFiles = (1 to numFiles).map( n => new File( splitFileDir, baseTableName + "_"+ n + ".csv" ) ).toList;
    //printf("Splitting %s into %s on bounds %s.\n", splitMe, outFiles.mkString(", "), boundaryCols.mkString(", "));
    divideOneRowHeaderCsvFileByBounds( primaryKeyColNames, allColNames, splitMe, boundaryCols, outFiles, splitBufferSize, xform );
    outFiles;
  }

  def divideOneRowHeaderCsvFile( keyColNames : Set[String], inFile: File, outFiles : List[File], bufferSize : Int) : Unit = {
    divideOneRowHeaderCsvFile( keyColNames, inFile, outFiles, bufferSize, None );
  }

  def divideOneRowHeaderCsvFile( keyColNames : Set[String], inFile: File, outFiles : List[File], bufferSize : Int, xform : Option[Function1[Array[String],Array[String]]]) : Unit = {
    val br = new BufferedReader( new FileReader( inFile ), bufferSize );
    try {
      val headerList = headers( br, xform ).toList;

      var line = br.readLine();
      var rows = new Iterator[List[String]] {
	def hasNext : Boolean = (line != null);
	def next : List[String] = {
	  var out = FastCsvUtils.splitRecord( line ).toList;
	  line = br.readLine();
	  out;
	}
      }
    
      val sinks = outFiles.map( new CsvFileWritableTable( _ , bufferSize ) );
      try {
	divideIntoJoinableByNumSinks(headerList, keyColNames, rows, sinks );
      }
      finally {
	ClosableUtils.attemptCloseAll( sinks : _* );
      }
    }
    finally {
      ClosableUtils.attemptClose( br );
    }
  }

  def divideOneRowHeaderCsvFileByBounds(keyColNames : Set[String],
 					allColNames : List[String],
					inFile: File, 
					boundaryColsExclusive : List[String],
					outFiles : List[File], 
					bufferSize : Int) : Unit = {
    divideOneRowHeaderCsvFileByBounds(keyColNames,
 				      allColNames,
				      inFile,
				      boundaryColsExclusive,
				      outFiles,
				      bufferSize,
				      None);
  }

  def divideOneRowHeaderCsvFileByBounds(keyColNames : Set[String],
 					allColNames : List[String],
					inFile: File, 
					boundaryColsExclusive : List[String],
					outFiles : List[File], 
					bufferSize : Int,
					xform : Option[Function1[Array[String],Array[String]]]) : Unit = {
    assert( boundaryColsExclusive.length == outFiles.length - 1 );

    val br = new BufferedReader( new FileReader( inFile ), bufferSize );
    try {
      val headerList = headers( br, xform ).toList;

      //printf("file header list: %s\n", headerList.mkString(", "));

      var line = br.readLine();
      var rows = new Iterator[List[String]] {
	def hasNext : Boolean = (line != null);
	def next : List[String] = {
	  var out = FastCsvUtils.splitRecord( line ).toList;
	  line = br.readLine();
	  out;
	}
      }
    
      val sinks = outFiles.map( new CsvFileWritableTable( _ , bufferSize ) );
      try {
	divideIntoJoinableByBounds(headerList, allColNames, keyColNames, boundaryColsExclusive, rows, sinks );
      }
      finally {
	ClosableUtils.attemptCloseAll( sinks : _* );
      }
    }
    finally {
      ClosableUtils.attemptClose( br );
    }
  }

  trait WritableTable {
    def setColNames( colNames : List[String] ) : Unit;
    def addDataRow( row : List[String] ) : Unit; 
    def close() : Unit;
  }

  class CsvFileWritableTable( f : File, bufferSize : Int ) extends WritableTable {
    private var pw = new PrintWriter( new BufferedWriter( new FileWriter(f), bufferSize ) );
    private def mkStringCsv( data : List[String] ) : String = data.mkString("\"","\",\"","\"");
    def setColNames( colNames : List[String] ) : Unit = pw.println( mkStringCsv(colNames) );
    def addDataRow( row : List[String] ) : Unit = pw.println( mkStringCsv(row) );
    def close() : Unit = pw.close();
  }

  private def divideIntoJoinableByNumSinks(colNames    : List[String],
					   keyColNames : Set[String], 
					   inputRows   : Iterator[List[String]], 
					   sinks       : List[WritableTable]) : Unit =  
					     divideIntoJoinableByNumSinks(colNames, keyColNames, inputRows, sinks, None);

  private def divideIntoJoinableByBounds(colNames              : List[String], 
					 allColNames           : List[String],
					 keyColNames           : Set[String], 
					 boundaryColsExclusive : List[String],
					 inputRows             : Iterator[List[String]], 
					 sinks                 : List[WritableTable]) : Unit = 
					   divideIntoJoinableByBounds(colNames, allColNames, keyColNames, boundaryColsExclusive, inputRows, sinks, None);

  private def splitColSort( colNames : List[String] ) : (String) => Ordered[String] = {
    me => {
      new Ordered[String] {
	def compare( other : String ) : Int = {
	  val meIdx = colNames.indexOf( me );
	  val othIdx = colNames.indexOf( other );
	  assert( meIdx >= 0 && othIdx >= 0, "colNames: %s | me: %s | meIdx: %s | other: %s | othIdx: %s\n".format( colNames.mkString(", "), me, meIdx, other, othIdx ) );
	  if ( meIdx > othIdx ) 1;
	  else if ( meIdx < othIdx ) -1;
	  else 0;
	}
      }
    }
  }

  private def splitByNumSinks(colNames : List[String], keyColNames  : Set[String], numSinks : Int) : List[List[String]] = {

    val numKeys = keyColNames.size;

    implicit val sort = splitColSort( colNames );

    val keyColList = (TreeSet.empty[String] ++ keyColNames).toList;
    val othColList = ( (TreeSet.empty[String] ++ colNames) -- keyColNames ).toList;

    //println( keyColList.mkString(", ") );
    //println( othColList.mkString(", ") );

    val tailTableNonKeyLen  = (othColList.length / numSinks);
    val firstTableNonKeyLen = (tailTableNonKeyLen + (othColList.length % numSinks));

    val firstTableCols = keyColList ++ othColList.take( firstTableNonKeyLen );
    val otherTablesColsList = (for ( start <- firstTableNonKeyLen until othColList.length by tailTableNonKeyLen )
			      yield ( keyColList ++ othColList.slice(start, start + tailTableNonKeyLen) )).toList;

    firstTableCols::otherTablesColsList;
  }

  private def splitByBounds( colNames : List[String], allColNames : List[String], keyColNames : Set[String], boundaryColsExclusive : List[String] ) : List[List[String]] = {
    assert ( ! boundaryColsExclusive.isEmpty );

    val numKeys = keyColNames.size;

    implicit val sort = splitColSort( allColNames );

    val keyColList : List[String]      = (TreeSet.empty[String] ++ keyColNames).toList;
    val othColSet  : SortedSet[String] = TreeSet[String]( (colNames -- keyColNames.toList) : _*  );
    val othColList : List[String]      = othColSet.toList;

    printf( "keyColList: %s\n\n", keyColList );

    val boundaryColsIter = (TreeSet.empty[String] ++ boundaryColsExclusive).elements;

    val outBuf = new ArrayBuffer[List[String]];

    var from : Option[String] = None;
    var to   : Option[String] = None;

    do {
      to = {
	if (boundaryColsIter.hasNext) Some(boundaryColsIter.next);
	else None;
      }

      outBuf += ( 
	( from, to ) match {
	  case ( None, Some(_) )    => keyColList:::((othColSet.to( to.get )).toList);
	  case ( Some(_), Some(_) ) => keyColList:::((othColSet.range( from.get, to.get )).toList);
	  case ( Some(_), None )    => keyColList:::((othColSet.from( from.get )).toList);
	  case _ @ pair             => throw new RuntimeException("Huh? Unexpected pair: " + pair);
	} 
      );

      println( Pair( from, to ) );

      from = to
    }
    while ( to != None );
    
    printf("outBuf.size: %d\n", outBuf.size);

    val out = outBuf.toList;
    println( out.mkString("\n~~~\n") );
    out;
  }

  def divideIntoJoinableByNumSinks(colNames           : List[String], 
				   keyColNames        : Set[String], 
				   inputRows          : Iterator[List[String]], 
				   sinks              : List[WritableTable],
				   badRowWriter       : Option[PrintWriter]) : Unit = {
    def createTableColList() : List[List[String]] = {
      splitByNumSinks(colNames, keyColNames, sinks.size);
    }

    divideIntoJoinable(colNames, keyColNames, inputRows, sinks, badRowWriter, createTableColList);
  }

  def divideIntoJoinableByBounds(colNames              : List[String], 
				 allColNames           : List[String], 
				 keyColNames           : Set[String], 
				 boundaryColsExclusive : List[String],
				 inputRows             : Iterator[List[String]], 
				 sinks                 : List[WritableTable],
				 badRowWriter          : Option[PrintWriter]) : Unit = {

    printf("Unexpected? %s\n", (Set( colNames : _* ) -- allColNames).mkString(", "));

    def createTableColList() : List[List[String]] = {
      printf("Calling split by bounds.\n");
      splitByBounds(colNames, allColNames, keyColNames, boundaryColsExclusive);
    }

    divideIntoJoinable(colNames, keyColNames, inputRows, sinks, badRowWriter, createTableColList);
  }
			 

  private def divideIntoJoinable(colNames           : List[String], 
				 keyColNames        : Set[String], 
				 inputRows          : Iterator[List[String]], 
				 sinks              : List[WritableTable],
				 badRowWriter       : Option[PrintWriter],
			         createTableColList : () => List[List[String]]) : Unit = { 

    val numSinks = sinks.length;
    val allTablesCols = createTableColList();

    val headerSinkTuples = allTablesCols.zip( sinks );
    headerSinkTuples.foreach( tup => tup._2.setColNames( tup._1 ) );

    inputRows.foreach {
      row => {

	if ( colNames.size != row.size ) {

	  if ( row.size >= 3 )
	    printf("badRow: \"%s\",\"%s\",\"%s\"...\n", row(0), row(1), row(2));
	  else
	    println("short bad row... "+ row.mkString("\"","\",\"","\""));

	  if ( badRowWriter != None ) {
	    var badRow = row.mkString("\"","\",\"","\"");
	    badRowWriter.get.println( badRow );
	  }
	} else {
	  val rowMap = Map.empty ++ colNames.zip( row );
	  headerSinkTuples.foreach {
	    tup => {
	      val headers = tup._1;
	      val sink = tup._2;
	      val dataRow = headers.map( rowMap( _ ) );
	      sink.addDataRow( dataRow );
	    }
	  }
	}

      }

    }
  }


/*
  // boundsList should contain sinks-1 (inclusive boundary col names)
  def divideIntoJoinable(colNames    : List[String], 
			 keyColNames : Set[String], 
			 inputRows   : Iterator[List[String]], 
			 sinks       : List[WritableTable],
                         boundsList  : List[String]) : Unit = { 

    val numSinks = sinks.length;
    val numKeys = keyColNames.size;

    implicit val sort : (String) => Ordered[String] = {
      me => {
	new Ordered[String] {
	  def compare( other : String ) : Int = {
	    val meIdx = colNames.indexOf( me );
	    val othIdx = colNames.indexOf( other );
	    assert( meIdx >= 0 && othIdx >= 0 );
	    if ( meIdx > othIdx ) 1;
	    else if ( meIdx < othIdx ) -1;
	    else 0;
	  }
	}
      }
    }

    val keyColList = (TreeSet.empty[String] ++ keyColNames).toList;
    val othColList = ( (TreeSet.empty[String] ++ colNames) -- keyColNames ).toList;

    //println( keyColList.mkString(", ") );
    //println( othColList.mkString(", ") );

    val tailTableNonKeyLen  = (othColList.length / numSinks);
    val firstTableNonKeyLen = (tailTableNonKeyLen + (othColList.length % numSinks));

    val firstTableCols = keyColList ++ othColList.take( firstTableNonKeyLen );
    val otherTablesColsList = (for ( start <- firstTableNonKeyLen until othColList.length by tailTableNonKeyLen )
			      yield ( keyColList ++ othColList.slice(start, start + tailTableNonKeyLen) )).toList;

    val allTablesCols = firstTableCols::otherTablesColsList;
    val headerSinkTuples = allTablesCols.zip( sinks );
    headerSinkTuples.foreach( tup => tup._2.setColNames( tup._1 ) );
    inputRows.foreach {
      row => {
	val rowMap = Map.empty ++ colNames.zip( row );
	headerSinkTuples.foreach {
	  tup => {
	    val headers = tup._1;
	    val sink = tup._2;
	    sink.addDataRow( headers.map( rowMap( _ ) ) );
	  }
	}
      }
    }
  }
*/

}
