package com.mchange.sc.v1.superflex;

trait Splitter
{
  // should trim if desired
  def split(row : String) : Array[String];
}
