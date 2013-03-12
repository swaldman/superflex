package com.mchange.sc.v1.democognos.dbutil;

import java.sql.{PreparedStatement, Types};
import com.mchange.sc.v1.reconcile._;
import com.mchange.sc.v1.reconcile.Reconcilable._;

final case class ColumnInfo(val name        : String, 
			    val label       : Option[String],
			    val sqlTypeDecl : Option[String], 
			    val typeCode    : Option[Int], 
			    val setter      : Option[( PreparedStatement, Int, String ) => Unit ]) extends Reconcilable[ColumnInfo]
{
  def reconcile( other : ColumnInfo ) : ColumnInfo =
  {
    if (this != other)
      {
	val name = reconcileLeaf(this.name, other.name);
	val label = reconcileLeaf(this.label, other.label);
	val sqlTypeDecl = reconcileLeaf(this.sqlTypeDecl, other.sqlTypeDecl);
	val typeCode = reconcileLeaf(this.typeCode, other.typeCode);
	
	//XXX: we'd better be careful to ensure that same logical functions are equal
	val setter = reconcileLeaf(this.setter, other.setter);
	
	ColumnInfo( name, label, sqlTypeDecl, typeCode, setter);
      }
    else
      this;
  }

  def reconcileOver( other : ColumnInfo ) : ColumnInfo =
  {
    if (this != other)
      {
	val name = reconcileOverLeaf(this.name, other.name);
	val label = reconcileOverLeaf(this.label, other.label);
	val sqlTypeDecl = reconcileOverLeaf(this.sqlTypeDecl, other.sqlTypeDecl);
	val typeCode = reconcileOverLeaf(this.typeCode, other.typeCode);
	
	//XXX: we'd better be careful to ensure that same logical functions are equal
	val setter = reconcileOverLeaf(this.setter, other.setter);
	
	ColumnInfo( name, label, sqlTypeDecl, typeCode, setter);
      }
    else
      this;
  }
}

