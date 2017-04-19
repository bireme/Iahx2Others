package org.bireme.i2o

import scala.concurrent.duration._
import scala.concurrent.{Await,Future}
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global

object Futuro extends App {

  def espera(secs: Int): Future[Int] = Future {
    println("de dentro do future: " + Thread.currentThread().getName())
    Thread.sleep(secs * 1000)
    println("de dentro do future. Terminei : " + Thread.currentThread().getName())
    secs
  }
  val future = {
    println(Thread.currentThread().getName())
    espera(15)
  }
  println("passo1 - " + Thread.currentThread().getName())

  future.onComplete {
    case Success(x) => println("de dentro do onComplete: " +
                                Thread.currentThread().getName() +
                                ". Tempo esperado: " + x)
    case Failure(f) => println(f.getMessage)
  }
  println("passo2 - " + Thread.currentThread().getName())

  val futMap = (1 to 20).map(espera(_))

  Await.ready(future, 60.seconds)
  println("fim")
}
