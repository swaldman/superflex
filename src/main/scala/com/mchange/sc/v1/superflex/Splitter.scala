package com.mchange.sc.v1.democognos.dbutil;

trait Splitter
{
  // should trim if desired
  def split(row : String) : Array[String];
}
