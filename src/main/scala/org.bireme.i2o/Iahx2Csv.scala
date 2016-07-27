package org.bireme.i2o

import java.io.File
import java.nio.file.Files
import java.nio.charset.Charset

import org.apache.commons.csv._

import scala.collection.immutable.{TreeMap,TreeSet}

object Iahx2Csv extends App {
  private def usage(): Unit = {
    Console.err.println("usage: Iahx2Csv (<query>|@<queryFile>) <outFile>"  +
          "\n\t\t[-outEncoding=<encod>] [-fieldDelim=<delimiter>]" +
          "\n\t\t[-explodeFields=<fldName>,<fldName>,..,<fldName>] " +
          "\n\t\t[-exportFields=<fldName>,<fldName>,..,<fldName>] " +
          "\n\t\t[-iahxUrl=<url>] [-from=<int>] [-quantity=<int>]")
    System.exit(1)
  }

  if (args.length < 2) usage()

  val parameters = args.drop(2).foldLeft[Map[String,String]](Map()) {
    case (map,par) => {
      val split = par.split("=", 2)
      map + ((split(0), split(1)))
    }
  }

  val defNullCell = "null"
  val query = args(0)
  val outFile = args(1)
  val outEncoding = parameters.getOrElse("-outEncoding", "utf-8")
  val fieldDelim = parameters.getOrElse("-fieldDelim", ",").charAt(0)
  val explode = parameters.getOrElse("-explodeFields", "").trim.split(" *, *")
                                                             .filter(!_.isEmpty)
  val export = parameters.getOrElse("-exportFields", "").trim.split(" *, *")
                                                             .filter(!_.isEmpty)
  val url = parameters.getOrElse(
                   "-iahxUrl", "http://db02dx.bireme.br:8983/portal-org/select")
  val from = parameters.getOrElse("-from", "0").toInt
  val quantity = parameters.getOrElse("-quantity", "-1").toInt

  toCsv(query, url, from, quantity, outFile, outEncoding, fieldDelim,
                                                    explode.toSet, export.toSet)

  private def toCsv(query: String,
                    url: String,
                    from: Int,
                    quantity: Int,
                    outFile: String,
                    outEncoding: String,
                    fieldDelim: Char,
                    explode: Set[String],
                    export: Set[String]): Unit = {
    val output = Files.newBufferedWriter(new File(outFile).toPath(),
                                                  Charset.forName(outEncoding))
    val tmpFile = File.createTempFile("temp-file-name", ".tmp")
    val iter = new DocumentIterator(query, url, from, quantity, Some(tmpFile))
    val max = getMaxFldOcc(iter, explode, export)
    val checkAll = export.isEmpty || export.exists(x => explode.contains(x))
    if (!checkAll && !export.isEmpty) iter.foreach(x => ()) // to save temp file
    iter.close()
    val fields = new TreeSet[String]() ++
                                (if (export.isEmpty) max.keys.toSet else export)
    val header = getHeader(max, fields)
    val csv = new CSVPrinter(output, CSVFormat.DEFAULT.withDelimiter(fieldDelim))
    //val docs = new DocumentIterator(query, url, from, quantity)
    val docs = new FileDocIterator(tmpFile)
    println("Total docs: " + iter.numDocs)

    header.foreach(csv.print)
    csv.println()
    //max.foreach(println)

    docs.foreach {
      case doc => {
        fields.foreach {
          fld => if (export.isEmpty || export.contains(fld)) {
            if (doc.contains(fld)) {
              doc(fld) match {
                case lst:List[Any] => {
                  if (explode(fld)) {
                    val diff = max(fld) - lst.size
                    lst.foreach(value => csv.print(value))
                    for (i <- 0 until diff) csv.print(defNullCell)
                  } else {
                    val str = lst.foldLeft[String]("") {
                      case(s,elem) => s + (if (s.isEmpty) "" else "|") + elem
                    }
                    csv.print(str)
                  }
                }
                case x => csv.print(x)
              }
            } else {
              if (explode(fld)) for (i <- 0 until max(fld)) csv.print(defNullCell)
              else csv.print(defNullCell)
            }
          }
        }
        csv.println()
      }
    }
    tmpFile.delete()
    docs.close()
    csv.close()
    output.close()
  }

  private def getMaxFldOcc(docs: Iterator[Map[String,Any]],
                           explode: Set[String],
                           export: Set[String]): Map[String,Int] = {
    val checkAll = export.isEmpty || export.exists(x => explode.contains(x))

    if (checkAll) {
      docs.foldLeft[Map[String,Int]](TreeMap()) {
        case(map,doc) => doc.foldLeft[Map[String,Int]](map) {
          case(map2,(k,v)) => {
            val qtt1 = map2.getOrElse(k,0)
            val qtt2 = v match {
              case lst:List[Any] => if (explode(k)) lst.size else 1
              case _ => 1
            }
            map2 + ((k, Integer.max(qtt1,qtt2)))
          }
        }
      }
    } else export.foldLeft[Map[String,Int]](TreeMap()) {
      case (map,str) => map + ((str,1))
    }
  }

  private def getHeader(max: Map[String,Int],
                        export: Set[String]): List[String] = {
    max.foldLeft[List[String]](List()) {
      case (lst, (k,v)) => if (export.contains(k)) {
        if (v < 2) lst :+ k
        else (1 to v).foldLeft[List[String]](lst) {
          case (lst2,i) => lst2 :+ (k + "[" + i + "]")
        }
      } else lst
    }
  }
}
