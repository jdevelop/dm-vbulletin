package dm.forum.vbulletin

import java.io.File

import akka.NotUsed
import akka.actor.ActorSystem
import akka.util.ByteString
import dm.forum.vbulletin.Profile.{FullProfile, Profile}
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.ScallopException

import scala.concurrent.{Future, Promise}

/**
  * User: Eugene Dzhurinsky
  * Date: 10/30/16
  */
object Aggregate {

  def main(args: Array[String]): Unit = {

    object Conf extends ScallopConf(args) {

      val basePath = opt[String]("base", required = true)

      val startId = opt[Int]("userid-start", short = 's', required = true)

      val endId = opt[Int]("userid-end", short = 'e', required = true)

      val userid = opt[String]("userid", short = 'u', required = true)

      val password = opt[String]("password", short = 'p', required = true)

      val forumUrl = opt[String]("url-prefix", noshort = true, required = true)

      override protected def onError(e: Throwable): Unit = e match {
        case exc: ScallopException ⇒
          println(exc.getMessage)
          printHelp()
          sys.exit(1)
        case exc ⇒ super.onError(exc)
      }
    }

    Conf.verify()

    implicit val context = HTTP.HttpContext(Conf.userid(), Conf.password(), Conf.forumUrl())

    import akka.stream._
    import akka.stream.scaladsl._

    implicit val system = ActorSystem("Local")
    implicit val materializer = ActorMaterializer()

    val base = new File(Conf.basePath())
    base.mkdirs()

    type OptProfile = Option[FullProfile]

    type LikesAndCount = (Int, Stream[Profile])

    val src: Source[Int, NotUsed] = Source[Int](Conf.startId() to Conf.endId()).async

    val fetchProfileFlow: Flow[Int, OptProfile, NotUsed] = Flow.fromFunction(Profile.extractFullProfile)

    val fetchLikesFlow: Flow[Int, LikesAndCount, NotUsed] = Flow.fromFunction(Likes.extractUserList)

    import argonaut._, Argonaut._

    implicit def fullProfile2Json = casecodec5(FullProfile.apply, FullProfile.unapply)("id", "name", "expected_likes", "actual_likes", "metadata")

    val profileDataSink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(new File(base, "profiles").toPath)

    import system.dispatcher

    val fetchLikesAndUpdateProfileG = Flow[(OptProfile, LikesAndCount)].flatMapConcat {
      case (None, _) ⇒ Source.empty
      case (Some(profile), (likesExpected, stream)) ⇒ GraphDSL.create() {
        implicit builder ⇒
          import GraphDSL.Implicits._

          val in = builder.add(Source(stream))

          val fan = builder.add(Broadcast[Profile.Profile](2))

          val profileLikesSink = builder.add(FileIO.toPath(new File(base, s"likes_${profile.id}").toPath))

          val profileComplete = Promise[Int]

          in ~> fan.in

          fan.out(0) ~> Flow.fromFunction[Profile.Profile, ByteString](
            p ⇒ ByteString(s"${p.id}\t${p.username}\t${p.liked.map(Likes.DateTimeParser.print).getOrElse("")}\n")
          ) ~> profileLikesSink
          fan.out(1) ~> Flow.apply[Profile.Profile].fold(0) { case (x, _) ⇒ x + 1 }
            .map(finalV ⇒ profileComplete.success(finalV)) ~> Sink.ignore

          // update profile here with the numbers of records and emit i
          builder.add(
            Source
              .fromFuture(
                profileComplete
                  .future
                  .map {
                    count ⇒ profile.copy(expectedLikes = likesExpected, actualLikes = count)
                  }
              )
          )

      }
    }

    RunnableGraph.fromGraph(
      GraphDSL.create() {
        implicit builder ⇒

          import GraphDSL.Implicits._

          val inlet = builder.add(Broadcast[Int](2))
          val merge = builder.add(Zip[OptProfile, LikesAndCount])

          src.async ~> inlet.in
          inlet.out(0) ~> fetchProfileFlow.async ~> merge.in0
          inlet.out(1) ~> fetchLikesFlow.async ~> merge.in1

          merge.out ~> fetchLikesAndUpdateProfileG.async ~>
            Flow.fromFunction[Profile.FullProfile, ByteString](p ⇒ ByteString(s"${p.asJson.nospaces}\n")).async ~>
            profileDataSink

          ClosedShape
      }
    ).run()

  }

}