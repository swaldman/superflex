package com.mchange.sc.v1.superflex;

import scala.collection._;
import com.mchange.sc.v1.reconcile._;
import com.mchange.sc.v1.reconcile.Reconcilable._;

final case class TableInfo(val tschema    : Option[String], 
			   val tname      : Option[String],
			   val cols       : Option[Iterable[ColumnInfo]],
			   val pkNames    : Option[List[String]]) extends Reconcilable[TableInfo]
{
  val colsByName : Option[Map[String, ColumnInfo]] = 
    {
      if ( cols == None )
	None;
      else
	{
	  val tmp = new mutable.HashMap[String, ColumnInfo];
	  cols.get.foreach( ci => { tmp += (ci.name -> ci) } );
	  Some( immutable.Map.empty ++ tmp );
	}
    }

  //require( pkNames == None || pkNames.get.forall( isNamedCol( _ ) ) );

  private def isNamedCol( cn : String) : Boolean =
    {
      if (colsByName == None)
	false;
      else
	colsByName.get.contains( cn );
    }


  def reconcile(other : TableInfo) : TableInfo = 
  {
    if (this != other)
    {
      val tschema = reconcileLeaf(this.tschema, other.tschema);
      val tname = reconcileLeaf(this.tname, other.tname);
      val cols = combineCols(other, false);
      val pkNames = reconcileLeaf(this.pkNames, other.pkNames);
      
      new TableInfo( tschema, tname, cols, pkNames );
    }
    else
      this;
  }

  def reconcileOver(other : TableInfo) : TableInfo = 
  {
    if (this != other)
    {
      val tschema = reconcileOverLeaf(this.tschema, other.tschema);
      val tname = reconcileOverLeaf(this.tname, other.tname);
      val cols = combineCols( other, true );
      val pkNames = reconcileOverLeaf(this.pkNames, other.pkNames);
      
      new TableInfo( tschema, tname, cols, pkNames );
    }
    else
      this;
  }

  @throws(classOf[CantReconcileException])
  private def combineCols(other : TableInfo, over : Boolean ) : Option[Iterable[ColumnInfo]] =
  {
    ( this.cols, other.cols ) match
    {
      case (None, None)               => None;
      case (None, Some(cols))         => Some(cols);
      case (Some(cols), None)         => Some(cols);
      case (Some(tcols), Some(ocols)) =>
      {
	//println("SUBSTANTIVE COMBINE");

	val tmpOut = mutable.Set.empty[ColumnInfo];
	val unhandledOtherCols = mutable.Set.empty[ColumnInfo] ++ ocols;
	
	assert( other.colsByName != null );
	
	val oColsByName : Map[String, ColumnInfo] = other.colsByName.get;
	
	for ( myCol <- tcols )
	  {
	    val maybeOtherCol = oColsByName.get( myCol.name );
	    if ( maybeOtherCol != None )
	      {
		val otherCol = maybeOtherCol.get;
		tmpOut += (if (over) myCol.reconcileOver( otherCol ); else myCol.reconcile( otherCol ) );
		unhandledOtherCols -= otherCol;
	      }
	    else
	      tmpOut += myCol;
	  }
	tmpOut ++= unhandledOtherCols;

	Some( tmpOut );
      }
    }
  }
}

    /*
    val numCols = 
      {
	( this.numCols, other.numCols ) match
	  {
	    case ( None, None ) => None;
	    case ( Some( num ), None ) => Some( num );
	    case ( None, Some( num ) ) => Some( num );
	    case ( Some( tnum ), Some( onum ) ) => Some( tnum.max(onum) );
	  }
    */

