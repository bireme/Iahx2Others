package org.bireme.i2o

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons._

import org.slf4j.impl.SimpleLogger._
import org.slf4j.LoggerFactory

object Iahx2Mongo extends App {


  private def usage(): Unit = {
    Console.err.println("usage: Iahx2Mongo (<query>|@<queryFile>) <database>"  +
                  "<collection>\n\t\t  [-iahxUrl=<url>] [-mongoHost=<host>]" +
                  "\n\t\t  [-mongoPort=<port>] [-from=<int>] [-quantity=<int>]")
    System.exit(1)
  }

  if (args.length < 3) usage()

  System.setProperty(DEFAULT_LOG_LEVEL_KEY, "ERROR")

  val parameters = args.drop(3).foldLeft[Map[String,String]](Map()) {
    case (map,par) => {
      val split = par.split("=", 2)
      map + ((split(0), split(1)))
    }
  }
  val url = parameters.getOrElse("-iahxUrl", "http://db02dx.bireme.br:8983/portal-org/select")
  val query = args(0)
  val from = parameters.getOrElse("-from", "0").toInt
  val quantity = parameters.getOrElse("-quantity", "-1").toInt
  val host = parameters.getOrElse("-mongoHost", "ts01vm.bireme.br")
  val port = parameters.getOrElse("-mongoPort", "27017")
  val dbName = args(1)
  val collection = args(2)
  val mongoClient = MongoClient(host)
  val dbase = mongoClient(dbName)
  val coll = dbase(collection)
  val iterator = new DocumentIterator(query, url, from, quantity)

  toMongo(iterator, coll)

  private def toMongo(iterator: DocumentIterator,
                      coll: MongoCollection): Unit = {
    coll.drop()

    iterator.foreach {
      case doc => {        
        //println("\n" + doc + "\n")
        val builder = new MongoDBObjectBuilder() ++= doc
        coll += new MongoDBObject(builder.result())
      }
    }
  }
}
