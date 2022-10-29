package com.mchange.sc.v1.superflex;

class UndefinedTableException(message : String, cause : Throwable) extends DbArchiverException( message, cause )
{
  def this( message : String ) = this( message, null );
  def this( cause : Throwable ) = this( null, cause );
  def this() = this( null, null );
}
