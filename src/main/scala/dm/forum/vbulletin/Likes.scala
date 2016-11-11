package dm.forum.vbulletin

import dm.forum.vbulletin.HTTP.HttpContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Retrieves the list of users from the front-page.
  */
object Likes {

  private val LOG = LoggerFactory.getLogger(getClass)

  import Profile._

  case class Likes(profile: Profile, likes: Iterator[Profile])

  // Private API

  private def extractTotalPages(doc: Document): Int = {
    (for (
      els <- Option(doc.select("span.first_last > a[rel=nofollow]"));
      fst <- Option(els.first());
      href <- Option(fst.attr("href"));
      page <- extractPageNumberFromLink(href)
    ) yield {
      page
    }).getOrElse(1)
  }

  private val LikesRegex = "(\\d+) Likes".r

  private def extractTotalLikes(doc: Document): Int = {
    val text: String = doc.select("h3.subsectionhead.userprof_title:matchesOwn(Likes)").text()
    text match {
      case LikesRegex(likes) ⇒ likes.toInt
      case _ ⇒ 0
    }
  }

  val LikesRegexBackDate = """(?i)Liked on: (\d)+ (день|дн.|недель\(и\)|неделю) назад, (\d+:\d+)""".r


  val LikesRegexDatetime = """(?i)Liked on: (\d+\.\d+.\d+), (\d+:\d+)""".r

  val DateTimeParser = DateTimeFormat.forPattern("dd.MM.yyyy HH:mm")

  val TimeParser = DateTimeFormat.forPattern("HH:mm")

  def setTime(date: DateTime, timeStr: String): DateTime = {
    val dt = TimeParser.parseDateTime(timeStr)
    date
      .withHourOfDay(dt.hourOfDay().get())
      .withMinuteOfHour(dt.minuteOfHour().get())
      .minuteOfDay().roundFloorCopy()
  }

  def resolveRelativeDate(value: Int, relTime: String): Option[DateTime] = {
    relTime match {
      case "минут(ы)" ⇒ Some(DateTime.now().minusMinutes(value))
      case "день" | "дн." ⇒ Some(DateTime.now().minusDays(value))
      case "неделю" | "недель(и)" ⇒ Some(DateTime.now().minusWeeks(value))
      case _ ⇒ None
    }
  }

  def parseLikesDate(src: String): Option[DateTime] = {
    src match {
      case LikesRegexBackDate(day, backType, time) ⇒
        resolveRelativeDate(day.toInt, backType).map(x ⇒ setTime(x, time))
      case LikesRegexDatetime(date, time) ⇒
        util.Try(DateTimeParser.parseDateTime(s"$date $time")).toOption
      case _ ⇒ None
    }
  }

  private def extractUserLikes(doc: Document): mutable.Buffer[(String, Option[DateTime])] = {
    doc.select("li[id^=like]").map {
      likes ⇒
        val profile = likes.select("a.avatarlink")
          .map(href ⇒ extractNameFromUrl(href.attr("href")))
          .headOption
          .getOrElse {
            likes.select("span.avatarlink > img").map(el ⇒ el.attr("alt")).head
          }
        val likedOn = likes.select("blockquote.posttext.likedate").text()
        profile -> parseLikesDate(likedOn)
    }
  }

  // Public API

  def extractUserList(userId: Int)(implicit ctx: HttpContext): (Int, Stream[Profile]) = {
    (for (
      content <- HTTP.receiveString(profileUrl(userId, 1, "likes_received")).right
    ) yield {
      val doc = Jsoup.parse(content)
      val pages = extractTotalPages(doc)
      val likes = extractTotalLikes(doc)
      if (LOG.isDebugEnabled()) {
        LOG.debug(s"Found ${likes} likes on ${pages} pages for user id ${userId}")
      }
      likes -> (extractUserLikes(doc) #:: Stream.from(2).takeWhile(_ <= pages).map {
        page ⇒
          val url: String = profileUrl(userId, page, "likes_received")
          val resp = HTTP.receiveString(url)
          LOG.debug(s"Retrieved page response for {} of {} for ${userId}", page, pages)
          page -> resp
      }.collect {
        case (page, Right(contentNext)) ⇒
          val links = extractUserLikes(Jsoup.parse(contentNext))
          if (links.size != 15) {
            println(links)
          }
          LOG.debug(s"Extracted {} links for {} at ${page}", links.length, userId)
          links
      }).flatMap(x ⇒ x.map(y ⇒ Profile(y._1, y._2)))
    }) match {
      case Left(exc) ⇒
        LOG.error("Can't process the page", exc)
        0 -> Stream.empty[Profile]
      case Right(x) ⇒
        x
    }
  }

}