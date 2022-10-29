/*
 * Distributed as part of superflex v0.2.0
 *
 * Copyright (C) 2013 Machinery For Change, Inc.
 *
 * Author: Steve Waldman <swaldman@mchange.com>
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of EITHER:
 *
 *     1) The GNU Lesser General Public License (LGPL), version 2.1, as 
 *        published by the Free Software Foundation
 *
 * OR
 *
 *     2) The Eclipse Public License (EPL), version 1.0
 *
 * You may choose which license to accept if you wish to redistribute
 * or modify this work. You may offer derivatives of this work
 * under the license you have chosen, or you may provide the same
 * choice of license which you have been offered here.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received copies of both LGPL v2.1 and EPL v1.0
 * along with this software; see the files LICENSE-EPL and LICENSE-LGPL.
 * If not, the text of these licenses are currently available at
 *
 * LGPL v2.1: http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 *  EPL v1.0: http://www.eclipse.org/org/documents/epl-v10.php 
 * 
 */

package com.mchange.sc.v1.superflex;

import scala.collection._;
import com.mchange.sc.v1.reconcile._;
import com.mchange.sc.v1.reconcile.Reconcilable._;

final case class TableInfo(val tschema    : Option[String], 
			   val tname      : Option[String],
			   val cols       : Option[Iterable[ColumnInfo]],
			   val pkNames    : Option[List[String]]) extends Reconcilable[TableInfo]
{
  val tableFullName : Option[String] = {
    val schemaPfx = tschema.map( _ + "." ).getOrElse("")
    tname.map( schemaPfx + _ )
  }

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

