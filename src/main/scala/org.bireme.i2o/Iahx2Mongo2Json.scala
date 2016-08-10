package org.bireme.i2o

import com.mongodb.DBCollection
import com.mongodb.casbah.Imports._

import org.slf4j.impl.SimpleLogger._
import org.slf4j.LoggerFactory

import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils

import scala.collection.immutable.TreeMap
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.parsing.json.JSON

class Iahx2Mongo2Json {
  def toJson(query: String,
             parameters: Map[String,String]):
                                           Iterator[Option[Map[String,Any]]] = {
    System.setProperty(DEFAULT_LOG_LEVEL_KEY, "ERROR")

    val fields = parameters.getOrElse("exportFields", "").split(" *, *").
                                            map(_.trim).filter(!_.isEmpty).toSet
    val iahxServUrl = parameters.getOrElse("iahxServiceUrl",
                              "http://db02dx.bireme.br:8983/portal-org/select/")
    val host = parameters.getOrElse("mongoHost", "mongodb.bireme.br")
    val port = parameters.getOrElse("mongoPort", "27017").toInt
    val from = parameters.getOrElse("from", "0").toInt
    val quantity = parameters.getOrElse("quantity", "1000").toInt
    val convFile = parameters.getOrElse("convFile", "convertion.txt")
    val convEncoding = parameters.getOrElse("convEncoding", "utf-8")

    val colls = getCollections(host, port, convFile, convEncoding)
    val ids = getIds(iahxServUrl, query, from, quantity)
//println(ids)
    val ids2 = ids.filter(id => id.startsWith("lil-"))

    export(ids2, colls, fields)
  }

  private def export(ids: List[String],
                     colls: Map[String, MongoCollection],
                     exportFields: Set[String]):
                                           Iterator[Option[Map[String,Any]]] = {
    new Iterator[Option[Map[String,Any]]] {
      var ids2 = ids

      def hasNext = !ids2.isEmpty

      def next(): Option[Map[String, Any]] = {
        if (!hasNext) None

        val split = ids2.head.split("-", 2)
        ids2 = ids2.tail

        colls.get(split(0)) match {
          case Some(col) => {
            val res = if (exportFields.isEmpty) {
              col.findOneByID(split(1))
            } else {
              val flds = exportFields.map(f => (f,1)).toList
              col.findOneByID(split(1), MongoDBObject(flds))
            }
            res match {
              case Some(obj: BasicDBObject) => {
                Some(JSON.parseFull(obj.toString()).get.
                                                  asInstanceOf[Map[String,Any]])
              }
              case _ => None
            }
          }
          case None => {
            Console.err.println("prefix [" + split(0) +
                                                "] not found. Database skiped.")
            next()
          }
        }
      }
    }
  }

  /*
    <prefix>,<dbname>,<collection>
    mdl, isis, medline
    lil, isis, lilacs
  */
  private def getCollections(host: String,
                             port: Int,
                             convFile: String,
                             convEncoding: String):
                                                Map[String, MongoCollection] = {
    val mongoClient = MongoClient(host, port)
    val reader = Source.fromFile(convFile, convEncoding)
    val lines = reader.getLines()
    val map = lines.foldLeft[Map[String,(String,String)]](Map()) {
      (map,line) => {
        line.trim match {
          case "" => map
          case l => {
            val elems = l.split(" +", 3)
            map + ((elems(0), (elems(1), elems(2))))
          }
        }
      }
    }
    reader.close()

    val nameDb = map.foldLeft[Map[String,MongoDB]](Map()) {
      case (mp, (k,v)) => if (mp.contains(v._1)) mp
                          else mp + ((v._1, mongoClient(v._1)))
    }

    map.foldLeft[Map[String,MongoCollection]](Map()) {
      case (mp, (k,v)) => {
        val col = nameDb(v._1)(v._2)
        mp + ((k, col))
      }
    }
  }

  private def getIds(url: String,
                     query: String,
                     from: Int,
                     quantity: Int): List[String] = {
    val qry = if (query.charAt(0) == '@') {
      val source = Source.fromFile(query.substring(1))
      try source.mkString finally source.close()
    } else query
    val nvps = List[NameValuePair](new BasicNameValuePair("wt", "json"),
                             new BasicNameValuePair("fl", "id"),
                             new BasicNameValuePair("facet", "false"),
                             new BasicNameValuePair("q", qry),
                             new BasicNameValuePair("start", from.toString),
                      new BasicNameValuePair("rows", quantity.toString)).asJava
    val httpClient = HttpClientBuilder.create().build()
    val post = new HttpPost(url)
    post.addHeader("Accept", "application/json")
    post.setEntity(new UrlEncodedFormEntity(nvps))
    val response = httpClient.execute(post);
    val entity = response.getEntity();
    val content = if (entity == null) "" else EntityUtils.toString(entity)
    httpClient.close()
    if (content.isEmpty) List()
    else {
      val pat = """"id":"([^"]+)"""".r
      val lst = pat.findAllMatchIn(content).map(_.group(1)).toList
      //set.foreach(id => println("id=" + id))
      lst
    }
  }
}

object Iahx2Mongo2Json extends App {

  private def usage(): Unit = {
    Console.err.println("usage: Iahx2Mongo2Json (<query>|@<queryFile>)" +
                  "\n\t\t  [-exportFields=<fld1>,<fld2>,...,<fldn>]" +
                  "\n\t\t  [-iahxServiceUrl=<url>]" +
                  "\n\t\t  [-mongoHost=<host>] [-mongoPort=<port>]" +
                  "\n\t\t  [-from=<int>] [-quantity=<int>]" +
                  "\n\t\t  [-convFile=<path>] [-convEncoding=<str>]")
    System.exit(1)
  }

  if (args.length < 1) usage()

  val parameters = args.drop(1).foldLeft[Map[String,String]](Map()) {
    case (map,par) => {
      val split = par.split(" *= *", 2)
      map + ((split(0).substring(1), split(1)))
    }
  }
  val imj = new Iahx2Mongo2Json()
  val expo = imj.toJson(args(0), parameters)

  expo.foreach(println)
}
