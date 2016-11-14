package dm.forum.vbulletin

import java.security.cert.X509Certificate

import org.apache.commons.io.IOUtils
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.config.{RegistryBuilder, SocketConfig}
import org.apache.http.conn.socket.{ConnectionSocketFactory, PlainConnectionSocketFactory}
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, SSLContexts, TrustStrategy}
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

/**
  * User: Eugene Dzhurinsky
  * Date: 10/29/16
  */
object HTTP {

  private val LOG = LoggerFactory.getLogger(getClass)

  case class HttpContext(userid: String, password: String, urlPrefix: String)

  private val TIMEOUT = 20 * 1000

  private val socketConfig = SocketConfig.custom()
    .setSoTimeout(TIMEOUT)
    .setTcpNoDelay(true)
    .build()

  private val requestConfig = RequestConfig.custom()
    .setSocketTimeout(TIMEOUT)
    .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
    .setConnectTimeout(TIMEOUT)
    .build()


  private val sslContext = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy {
    override def isTrusted(chain: Array[X509Certificate], authType: String): Boolean = true
  }).build()

  private val sslsf = new SSLConnectionSocketFactory(
    sslContext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)

  private val socketFactoryRegistry = RegistryBuilder.create[ConnectionSocketFactory]().
    register("https", sslsf).
    register("http", PlainConnectionSocketFactory.INSTANCE).build()

  private val cm = new PoolingHttpClientConnectionManager(socketFactoryRegistry)
  cm.setDefaultSocketConfig(socketConfig)
  cm.setMaxTotal(200)
  cm.setDefaultMaxPerRoute(20)

  private val client = HttpClients.custom()
    .setDefaultRequestConfig(requestConfig)
    .setConnectionManager(cm)
    .build()

  def receiveString(url: String)(implicit ctx: HttpContext): Either[Exception, String] = {
    def inner(tries: Int = 3): Either[Exception, String] = {
      var response: CloseableHttpResponse = null
      try {
        val get = new HttpGet(url)
        get.addHeader("Accept-Encoding", "gzip")
        get.addHeader("Cookie", s"bb_userid=${ctx.userid};bb_password=${ctx.password}")
        response = client.execute(get)
        val content: String = EntityUtils.toString(response.getEntity)
        Right(content)
      } catch {
        case e: Exception ⇒
          if (tries > 0) {
            LOG.warn("Caught error while processing {}", url)
            Thread.sleep(1000)
            inner(tries - 1)
          } else {
            LOG.error("Retry aborted, url {} is invalid", url)
            Left(e)
          }
      } finally {
        IOUtils.closeQuietly(response)
      }
    }

    inner()

  }


}