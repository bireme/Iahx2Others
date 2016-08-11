package org.bireme.i2o

import scala.collection.JavaConversions._

class Iahx2Csv {
  def exportToFile(query: String,
                   csvFile: String,
                   parameters: Map[String,String]) : Unit = {
    def getIterator(): Iterator[Option[Map[String,Any]]] =
                                new Iahx2Mongo2Json().toJson(query, parameters)
    new Json2Csv().toFileCsv(getIterator, csvFile, parameters)
  }

  def export(query: String,
             writer: java.io.Writer,
             parameters: Map[String,String]) : Unit = {
    def getIterator(): Iterator[Option[Map[String,Any]]] =
                                new Iahx2Mongo2Json().toJson(query, parameters)
    new Json2Csv().toCsv(getIterator, writer, parameters)
  }

  def exportFromJava(query: String,
                     writer: java.io.Writer,
                     parameters: java.util.Map[String,String]): Unit = {
    val params = parameters.toMap
println(params)
    def getIterator(): Iterator[Option[Map[String,Any]]] =
                                     new Iahx2Mongo2Json().toJson(query, params)
    new Json2Csv().toCsv(getIterator, writer, params)
  }
}

object Iahx2Csv extends App {
  private def usage(): Unit = {
    Console.err.println("usage: Iahx2Csv (<query>|@<queryFile>)" +
                  "\n\t\t  [-csvFile=<fileName>]" +
                  "\n\t\t  [-exportFields=<fld1>,<fld2>,...,<fldn>]" +
                  "\n\t\t  [-from=<int>] [-quantity=<int>]" +
                  "\n\t\t  [-convFile=<path>] [-convEncoding=<str>]" +
                  "\n\t\t  [-outEncoding=<encod>] [-fieldDelim=<delimiter>]" +
                  "\n\t\t  [-explodeFields=<fldName>,<fldName>,..,<fldName>]" +
                  "\n\t\t  [-iahxServiceUrl=<url>]" +
                  "\n\t\t  [-mongoHost=<host>] [-mongoPort=<port>]")
    System.exit(1)
  }

  if (args.length < 1) usage()

  val parameters = args.drop(1).foldLeft[Map[String,String]](Map()) {
    case (map,par) => {
      val split = par.split(" *= *", 2)
      map + ((split(0).substring(1), split(1)))
    }
  }

  if (parameters.contains("csvFile")) {
    new Iahx2Csv().exportToFile(args(0), parameters("csvFile"), parameters)
  } else {
    val writer = new java.io.StringWriter()
    new Iahx2Csv().export(args(0), writer, parameters)
    writer.close()
    println(writer.toString())
  }
}
