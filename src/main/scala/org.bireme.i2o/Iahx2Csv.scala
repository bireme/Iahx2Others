package org.bireme.i2o

class Iahx2Csv {
  def export(query: String,
             csvFile: String,
             parameters: Map[String,String]) : Unit = {
    def getIterator(): Iterator[Option[Map[String,Any]]] =
                                new Iahx2Mongo2Json().toJson(query, parameters)
    new Json2Csv().toCsv(getIterator, csvFile, parameters)
  }

  def exportFromJava(query: String,
                     csvFile: String,
                     parameters: scala.collection.mutable.Map[String,String]):
                                                                        Unit = {
    export(query, csvFile, parameters.toMap)
  }
}

object Iahx2Csv extends App {
  private def usage(): Unit = {
    Console.err.println("usage: Iahx2Csv (<query>|@<queryFile>) <csvFile>" +
                  "\n\t\t  [-exportFields=<fld1>,<fld2>,...,<fldn>]" +
                  "\n\t\t  [-from=<int>] [-quantity=<int>]" +
                  "\n\t\t  [-convFile=<path>] [-convEncoding=<str>]" +
                  "\n\t\t  [-outEncoding=<encod>] [-fieldDelim=<delimiter>]" +
                  "\n\t\t  [-explodeFields=<fldName>,<fldName>,..,<fldName>]" +
                  "\n\t\t  [-iahxServiceUrl=<url>]" +
                  "\n\t\t  [-mongoHost=<host>] [-mongoPort=<port>]")
    System.exit(1)
  }

  if (args.length < 2) usage()

  val parameters = args.drop(2).foldLeft[Map[String,String]](Map()) {
    case (map,par) => {
      val split = par.split(" *= *", 2)
      map + ((split(0).substring(1), split(1)))
    }
  }

  new Iahx2Csv().export(args(0), args(1), parameters)
}
