package dm.forum.vbulletin

import java.io.File

import akka.NotUsed
import akka.actor.ActorSystem
import akka.util.ByteString
import dm.forum.vbulletin.Profile.{FullProfile, Profile}
import org.apache.commons.io.FileUtils
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.ScallopException
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}

/**
  * User: Eugene Dzhurinsky
  * Date: 10/30/16
  */
object Aggregate {

  private val LOG = LoggerFactory.getLogger(getClass)

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


    import argonaut._, Argonaut._

    implicit def fullProfile2Json = casecodec5(FullProfile.apply, FullProfile.unapply)("id", "name", "expected_likes", "actual_likes", "metadata")

    val profileDataSink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(new File(base, "profiles").toPath)

    import system.dispatcher

    val fetchLikesAndUpdateProfileG = Flow[OptProfile].flatMapConcat {
      case (None) ⇒ Source.empty
      case Some(profile) ⇒ GraphDSL.create() {
        implicit builder ⇒
          import GraphDSL.Implicits._

          val (pages, likesExpected) = Likes.estimateUserLikesPages(profile.id)

          LOG.info(s"Start processing profile ${profile.username} with expected likes/pages: ${likesExpected} / ${pages}")

          val in = builder.add(Source(1 to pages).mapAsyncUnordered(10) {
            page ⇒ Future(Likes.getLikesAtPage(profile.id)(page))
          }.mapConcat(identity))

          val fan = builder.add(Broadcast[Profile.Profile](2))

          val likesFile: File = new File(base, s"likes_${profile.id}")

          val profileLikesSink = builder.add(FileIO.toPath(likesFile.toPath))

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
                    count ⇒
                      LOG.info("Finished profile {} with likes {}", profile.username, count)
                      if (likesFile.length() == 0L) {
                        LOG.warn("No links found, cleaning file {}", likesFile)
                        FileUtils.deleteQuietly(likesFile)
                      }
                      profile.copy(expectedLikes = likesExpected, actualLikes = count)
                  }
              )
          )

      }
    }

    val flowFuture = src.mapAsync(10)(pid ⇒ Future(Profile.extractFullProfile(pid)))
      .via(fetchLikesAndUpdateProfileG)
      .via(Flow.fromFunction[Profile.FullProfile, ByteString](p ⇒ ByteString(s"${p.asJson.nospaces}\n")))
      .to(profileDataSink)

    flowFuture.run()

  }

}