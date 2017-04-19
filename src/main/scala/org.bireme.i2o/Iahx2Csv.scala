package org.bireme.i2o

import bruma.master.Master
import bruma.master.MasterFactory
import bruma.master.Record

import java.io.{File, StringWriter, Writer}
import java.nio.charset.Charset
import java.nio.file.Files
import java.util.regex.{Pattern,Matcher}

import org.apache.commons.csv.{CSVFormat,CSVPrinter}

import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils

import scala.collection.immutable.TreeSet
import scala.collection.JavaConverters._

import scala.io.Source
import scala.util.{Try, Success, Failure}
import scala.util.parsing.json.JSON


class Iahx2Csv(query: String,
               queryFileEncoding: String,
               csvFile: String,
               exportFields: Set[String],
               from: Int,
               quantity: Int,
               outEncoding: String,
               fieldDelim: Char,
               explodeFields: Set[String],
               iahxServiceUrl: String,
               decsPath: String) {

  val defNullCell = ""   // Empty cell string

  val defFields = TreeSet("af", "ab", "ab_en", "ab_es", "ab_fr", "ab_pt", "af",
    "afiliacao_autor", "au", "bvs", "carpha_languange", "cc", "cp", "ct", "da",
    "db", "entry_date", "fa", "fo", "id", "instance",
    "instituicao_pais_afiliacao", "ip", "is", "la", "mh", "mj_styh",
    "nivel_tratamento", "pais_afiliação", "prder_sjr", "pg", "pt", "services",
    "sh", "st_cluster1", "st_cluster2", "st_cluster3", "st_cluster4", "ta",
    "ti", "ti_en", "ti_es", "ti_fr", "ti_pt", "type", "ur", "vi", "weight",
    "_version")

  val header: List[String] = (if (exportFields.isEmpty) defFields
                else defFields.intersect(exportFields)).toList
  val iahxServUrl = if (iahxServiceUrl.endsWith("/"))
                           iahxServiceUrl.substring(0,iahxServiceUrl.length - 1)
                    else iahxServiceUrl
  val decs =  if ((decsPath == null) || (decsPath.isEmpty)) Map.empty[Int,String]
              else loadDecs(decsPath)

  def export() : Unit = {
    val (isString, writer) = if (csvFile.trim.isEmpty) (true, new StringWriter())
      else (false, Files.newBufferedWriter(new File(csvFile).toPath(),
                                                  Charset.forName(outEncoding)))
    toCsv(writer)
    writer.close()

    if (isString) println(writer.toString)
  }

  def exportFromJava(writer: Writer): Unit = {
    toCsv(writer)
    writer.close()
  }

  private def loadDecs(path: String): Map[Int,String] = {
    val idField = 999
    val descField = 3
    val map = scala.collection.mutable.Map.empty[Int,String]
    val mst = MasterFactory.getInstance(path).open()
    val iterator = mst.iterator()

    while (iterator.hasNext()) {
      val rec = iterator.next()
      if (rec.isActive()) map += ((rec.getField(idField,1).getContent().toInt,
                                  rec.getField(descField,1).getContent()))
    }
    mst.close()
    map.toMap
  }

  private def toCsv(writer: Writer): Unit = {
    val csv = new CSVPrinter(writer, CSVFormat.DEFAULT.withDelimiter(fieldDelim))
    val ids = getIds(query, queryFileEncoding, from, quantity)
    println(s"ids found = ${ids.size}")

    header.foreach(csv.print(_))
    csv.println()

    ids.zipWithIndex.foreach {
      case (docId,idx) =>
        Try(getDocument(s"$iahxServUrl/?wt=json&q=id:$docId")) match {
          case Success(flds) => writeFields(flds, header, explodeFields, List(), csv)
          case Failure(e) => println(s"skipping document id:'$docId' - server error: $e")
        }
        if (idx % 100 == 0) println(s"+++ $idx")
    }
  }

  private def getIds(query: String,
                     queryFileEncoding: String,
                     from: Int,
                     quantity: Int): List[String] = {
    val qry = if (query.charAt(0) == '@') {
      val source = Source.fromFile(query.substring(1), queryFileEncoding)
      try source.mkString finally source.close()
    } else query
    val nvps = List[NameValuePair](new BasicNameValuePair("wt", "json"),
                             new BasicNameValuePair("fl", "id"),
                             new BasicNameValuePair("facet", "false"),
                             new BasicNameValuePair("q", qry),
                             new BasicNameValuePair("start", from.toString),
                      new BasicNameValuePair("rows", quantity.toString)).asJava
    val httpClient = HttpClientBuilder.create().build()
    val post = new HttpPost(iahxServiceUrl)
    post.addHeader("Accept", "application/json")
    //post.addHeader("Accept-Charset", "utf-8")
    post.setEntity(new UrlEncodedFormEntity(nvps, "utf-8"))
    val response = httpClient.execute(post);
    val entity = response.getEntity();
    val content = if (entity == null) "" else EntityUtils.toString(entity)

    httpClient.close()
    if (content.isEmpty) List()
    else {
      val pat = """"id":"([^"]+)"""".r
      pat.findAllMatchIn(content).map(_.group(1)).toList
    }
  }

  private def getDocument(url: String): Map[String, Any] = {
    val map = JSON.parseFull(loadPage(url)).get.asInstanceOf[Map[String,Any]]

    map.get("response") match {
      case Some(res: Map[String,Any]) => res.get("docs") match {
        case Some(lst: List[Map[String,Any]]) => if (lst.isEmpty) Map() else lst(0)
        case _ => Map()
      }
      case _ => Map()
    }
  }

  private def loadPage(url: String): String = {
    val html = Source.fromURL(url)
    val str = html.mkString
    html.close()
    str
  }

  private def writeFields(flds: Map[String,Any],
                          header: List[String],
                          expl: Set[String],
                          prefix: List[String],
                          csv: CSVPrinter): Unit = {
    header match {
      case Nil =>
        prefix.foreach(csv.print(_))
        csv.println()
        csv.flush()
      case h::t =>
        flds.get(h) match {
          case Some(fld) => fld match {
            case lst:List[_] =>
              if (expl.contains(h))
                lst.foreach(elem =>
                    writeFields(flds, t, expl, prefix :+ getString(elem), csv))
              else writeFields(flds, t, expl, prefix :+ getString(lst), csv)
            case elem => writeFields(flds, t, expl, prefix :+ getString(elem), csv)
          }
          case _ => writeFields(flds, t, expl, prefix :+ defNullCell, csv)
      }
    }
  }

  private def getString(elem: Any,
                        unique: Boolean = true): String = {
    elem match {
      case map:Map[String,Any] =>
        map.foldLeft[String]("") {
          (s,e) => s + (if (s.isEmpty) if (unique) "" else "{" else "|") + e._1 +
                                                    ":" + getString(e._2, false)
        } + (if (unique) "" else "}")
      case lst:List[Any] =>
        lst.foldLeft[String]("") {
          (s,e) => s + (if (s.isEmpty) if (unique) "" else "[" else "|") +
                                                             getString(e, false)
        } + (if (unique) "" else "]")
      case x:Any => convertDecsCode(x.toString())
    }
  }

  private def convertDecsCode(in: String): String = {
    if (decs.isEmpty) in else {
      val sb = new StringBuffer()

      // Replace strings of type "^d999^s9999" by <descriptor>/<qualifier>
      val pat = Pattern.compile("\\^d(\\d+)(\\^s(\\d+))?")
      val mat = pat.matcher(in)

      while (mat.find()) {
        val desc = mat.group(1).toInt
        val descStr = decs.getOrElse(desc, s"^d$desc")
        val qualStr = if (mat.group(2) == null) "" else {
          val qual = mat.group(3).toInt
          "/" + decs.getOrElse(qual, s"^s$qual")
        }
        mat.appendReplacement(sb, s"descStr$qualStr");
      }
      mat.appendTail(sb)
      sb.toString()
    }
  }
}

object Iahx2Csv extends App {
  private def usage(): Unit = {
    Console.err.println("usage: Iahx2Csv -query=(<expr>|@<queryFile>)" +
      "\n\t\t  [-queryFileEncoding=<encod>]" +
      "\n\t\t  [-csvFile=<fileName>]" +
      "\n\t\t  [-exportFields=<fld1>,<fld2>,...,<fldn>]" +
      "\n\t\t  [-from=<int>]" +
      "\n\t\t  [-quantity=(<int>|ALL)]" +
      "\n\t\t  [-outEncoding=<encod>] [-fieldDelim=<delimiter>]" +
      "\n\t\t  [-explodeFields=<fldName>,<fldName>,..,<fldName>]" +
      "\n\t\t  [-iahxServiceUrl=<url>" +
      "\n\t\t  [-decsPath=<path>] - Path to Decs isis db if replacement" +
      " of decs code to its content is desired")
    System.exit(1)
  }
  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) => {
      val split = par.split(" *= *", 2)
      map + ((split(0).substring(1), split(1)))
    }
  }
  val query = parameters.get("query") match {
    case Some(x) => x
    case None => usage();""
  }
  val queryFileEncoding = parameters.getOrElse("queryFileEncoding", "utf-8")
  val csvFile = parameters.getOrElse("csvFile", "")
  val exportFields = parameters.getOrElse("exportFields", "").trim.split(" *, *")
                                                .filter(!_.isEmpty).toSet
  val from = parameters.getOrElse("from", "1").toInt
  val quantity0 = parameters.getOrElse("quantity", "20")
  val quantity = if (quantity0.equals("ALL")) Integer.MAX_VALUE - 1
                 else quantity0.toInt
  val outEncoding = parameters.getOrElse("outEncoding", "utf-8")
  val fieldDelim = parameters.getOrElse("fieldDelim", ",").charAt(0)
  val explodeFields = parameters.getOrElse("explodeFields", "").trim.split(" *, *")
                                                       .filter(!_.isEmpty).toSet
  val iahxServiceUrl = parameters.getOrElse("iahxServiceUrl",
                         "http://bases.bireme.br:8986/solr5/portal/select").trim
  val decsPath =  parameters.getOrElse("decsPath", "")

  new Iahx2Csv(query, queryFileEncoding, csvFile, exportFields, from, quantity,
               outEncoding, fieldDelim, explodeFields, iahxServiceUrl, decsPath).
               export()
}
