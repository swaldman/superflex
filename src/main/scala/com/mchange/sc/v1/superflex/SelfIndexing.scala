package com.mchange.sc.v1.superflex;

import com.mchange.sc.v1.sql.ResourceUtils._;

trait SelfIndexing extends SuperFlexDbArchiver
{
  def indexTable( csrc : ConnectionSource ) : Unit;

  def deindexTable( csrc : ConnectionSource ) : Unit;

  override def archiveFiles( csrc : ConnectionSource ) =
  {
    super.archiveFiles( csrc );
    indexTable( csrc );
  }

  override def archiveFilesNoDups( csrc : ConnectionSource, imposePkConstraint : Boolean ) =
  {
    super.archiveFilesNoDups( csrc, imposePkConstraint );
    indexTable( csrc );
  }

  override def dropDeindexTable( csrc : ConnectionSource) = 
  {
    deindexTable( csrc );
    super.dropDeindexTable( csrc );
  }
}
