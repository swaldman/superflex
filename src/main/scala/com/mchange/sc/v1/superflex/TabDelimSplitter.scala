package com.mchange.sc.v1.superflex;

trait TabDelimSplitter extends Splitter
{
  // should trim if desired
  override def split(row : String) : Array[String] = { row.split("\t",-1).map( _.trim ) };
}
