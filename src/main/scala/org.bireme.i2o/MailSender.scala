package org.bireme.i2o

import scala.concurrent.duration._
import scala.concurrent.{Await,Future}
import scala.util.{Success, Failure}
import courier._
import Defaults._

// Need NoTimeConversions to prevent conflict with scala.concurrent.duration._
object MailSender extends App {

  val envelope:Envelope = Envelope.from("iahx2others" `@` "paho.org")
        .to("superheitor" `@` "gmail.com")
        .cc("barbieri" `@` "paho.org")
        .subject("miss you")
        .content(Text("hi big Heroe!"))

  val mailer = Mailer("esmeralda.bireme.br", 25)
               .auth(true)
               .as("serverofi", "bir@2012#")
               .startTtls(true)()
  val future:Future[Unit] = mailer(envelope)

  future.onComplete {
    case Success(x) => println("message delivered")
    case Failure(f) => println(f.getMessage)
  }

  Await.ready(future, 60.seconds)
}
