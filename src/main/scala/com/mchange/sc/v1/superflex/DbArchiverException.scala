package com.mchange.sc.v1.democognos.dbutil;

class DbArchiverException(message : String, cause : Throwable) extends Exception( message, cause )
{
  def this( message : String ) = this( message, null );
  def this( cause : Throwable ) = this( null, cause );
  def this() = this( null, null );
}

