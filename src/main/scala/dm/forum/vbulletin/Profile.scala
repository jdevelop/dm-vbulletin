package dm.forum.vbulletin

import java.net.URI

import dm.forum.vbulletin.HTTP.HttpContext
import org.apache.http.client.utils.URLEncodedUtils
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._

/**
  * User: Eugene Dzhurinsky
  * Date: 11/5/16
  */
object Profile {

  case class FullProfile(id: Int,
                         username: String,
                         expectedLikes: Int,
                         actualLikes: Int,
                         metadata: Map[String, String]
                        )

  case class Profile(id: Int, username: String, liked: Option[DateTime])

  object Profile {
    def apply(name: String, date: Option[DateTime]) = {
      val (id, username) = {
        name.lastIndexOf('-') match {
          case idx if idx > -1 ⇒
            name.substring(idx + 1).toInt -> name.substring(0, idx)
          case _ ⇒
            (-1, name)
        }
      }
      new Profile(id, username, date)
    }

  }


  def extractPageNumberFromLink(url: String) = {
    URLEncodedUtils.parse(new URI(url), "UTF8").find(_.getName == "page").map(_.getValue.toInt)
  }

  def profileUrl(userId: Int, page: Int = 1, tab: String)(implicit ctx: HttpContext) =
    s"${ctx.urlPrefix}${userId}/?tab=${tab}&page=${page}"

  def extractNameFromUrl(url: String) = {
    new URI(url).getPath.split("/").filter(_.nonEmpty).last
  }

  private def extractSelfProfile(doc: Document) = {
    for (
      tabs <- Option(doc.select("a[id=aboutme-tab]")).flatMap(x ⇒ Option(x.first()));
      href <- Option(tabs.attr("href"))
    ) yield {
      extractNameFromUrl(href)
    }

  }

  private def extractProfileContent(doc: Document) = {
    val prefs1 = doc.select("div.blockrow.member_blockrow").flatMap {
      block ⇒
        block.select("dl.stats > dt").map(_.text())
          .zip(
            block.select("dl.stats > dd").map(_.text())
          )
    }
    val prefs2 = doc.select("div.blockbody.subsection.userprof_content.userprof_content_border").flatMap {
      block ⇒
        block.select("dl.stats > dt").map(_.text())
          .zip(
            block.select("dl.stats > dd").map(_.text())
          )
    }
    val prefs3 = doc.select("div.subsection").flatMap {
      block ⇒
        block.select("dl").map {
          inner ⇒ inner.select("dt").text() -> inner.select("dd").text().trim
        }
    }
    val respMap = (prefs1 ++ prefs2 ++ prefs3).groupBy(_._1.toLowerCase()).map {
      case (k, v) ⇒ k -> v.head._2
    }

    val prof = extractSelfProfile(doc).map(pStr ⇒ Profile(pStr, None)).map {
      case Profile(id, username, _) ⇒
        FullProfile(id,
          username,
          0,
          0,
          respMap
        )
    }
    prof
  }

  private def extractUserProfileContent(userId: Int)(implicit ctx: HttpContext) = {
    HTTP.receiveString(profileUrl(userId, 1, "aboutme")) match {
      case Right(content) ⇒
        Some(Jsoup.parse(content))
      case Left(exc) ⇒
        None
    }
  }

  def extractFullProfile(userId: Int)(implicit ctx: HttpContext): Option[FullProfile] =
    extractUserProfileContent(userId).flatMap(extractProfileContent)


}