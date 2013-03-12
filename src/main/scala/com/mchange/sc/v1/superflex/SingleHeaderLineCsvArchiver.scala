package com.mchange.sc.v1.democognos.dbutil;

import java.io.BufferedReader;
import scala.collection._;
import com.mchange.sc.v1.democognos.dbutil.SuperFlexDbArchiver.{Key,MetaData};

abstract class SingleHeaderLineCsvArchiver extends SuperFlexDbArchiver with CsvSplitter 
{
  override def readMetaData( br : BufferedReader ) : MetaData = 
    { 
      val colNameLine = br.readLine();
      val prologue = colNameLine;
      
      immutable.Map( Key.COL_NAMES -> List.fromArray(split( colNameLine ) ), Key.PROLOGUE -> (prologue::Nil) ); 
    };
}
