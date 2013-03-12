package com.mchange.sc.v1.superflex;

import com.mchange.v2.csv.FastCsvUtils;

trait CsvSplitter extends Splitter
{
  // should trim if desired
  override def split(row : String) : Array[String] = { 
    return FastCsvUtils.splitRecord( row );
  };
}

/*

    import com.mchange.sc.v1.parsing.CsvParser;

    val p = new CsvParser();
    val result = p.parseAll( p.line, row );
    if (result.successful)
      {
	var out = result.get.map( _.trim ).toArray;
	//printf("Parse: %s\n", out.mkString("[", "][", "]"));
	out
      }
    else
      throw new DbArchiverException("Could not split as CSV row: '%s' [Parse error: %s]".format(row, result));

*/ 
