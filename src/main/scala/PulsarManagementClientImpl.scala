import akka.Done
import akka.actor.ActorSystem
import akka.actor.Scheduler
import akka.pattern.retry
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

import play.api.libs.json.JsValue
import play.api.libs.json.Json
import play.api.libs.json.OFormat
import play.api.libs.ws.EmptyBody
import play.api.libs.ws.WSClient
import play.api.libs.ws.WSRequest
import play.api.libs.ws.WSResponse



case class AutoTopicCreationOverride(topicType: String, allowAutoTopicCreation: Boolean, defaultNumPartitions: Int)
object AutoTopicCreationOverride {
  implicit val format: OFormat[AutoTopicCreationOverride] = Json.format

}

case class NamespacePolicies(autoTopicCreationOverride: Option[AutoTopicCreationOverride])
object NamespacePolicies {
  implicit val format: OFormat[NamespacePolicies] = Json.format
}

class PulsarManagementClientImpl(
                                  config: PulsarConfig,
                                  ws: WSClient,
                                  implicit val executionContext: ExecutionContext,
                                ) {

  // Use Pulsar's authentication provider to get the OAuth token for the headers.
  // For consistency, this will do the same for the AuthenticationToken provider, though it simply returns the token.
  private def getAuthHeaders(): Seq[(String, String)] =
    config.authentication.map { auth =>
      import scala.jdk.CollectionConverters._
      // In the OAuth provider, getAuthData may block to fetch a new token, but usually it is cached.
      blocking {
        Option(auth.getAuthData().getHttpHeaders()).toSeq.flatMap(_.asScala).map(e => e.getKey -> e.getValue)
      }
    }.getOrElse(Seq.empty)

  protected val adminPrefix = config.managementUrl + "/admin/v2/"
  private val functionsPrefix = config.managementUrl + "/admin/v3/functions/"

  // Shovels are identity Pulsar functions that by convention start with this prefix
  private final val ShovelPrefix = "shovel_"

  protected def url(url: String): WSRequest = {
    ws.url(url).withHttpHeaders(getAuthHeaders(): _*).addHttpHeaders("Content-Type" -> "application/json")
  }

  def getTenants(): Future[Seq[String]] = {
    for {
      resp <- url(adminPrefix + "tenants").get()
      _ <- validateResponse(resp, Set(200))
    } yield {
      resp.body[JsValue].as[Seq[String]]
    }
  }

  def getClusters(): Future[Seq[String]] = {
    for {
      resp <- url(adminPrefix + "clusters").get()
      _ <- validateResponse(resp, Set(200))
    } yield {
      resp.body[JsValue].as[Seq[String]]
    }
  }

  def getNamespaces(tenant: String): Future[Seq[String]] = {
    for {
      resp <- url(adminPrefix + "namespaces/" + tenant).get()
      _ <- validateResponse(resp, Set(200, 404))
    } yield {
      resp.body[JsValue].asOpt[Seq[String]].getOrElse(Seq.empty)
    }
  }

  /**
   * Used for testing only.
   */
  def createTenant(tenantName: String): Future[Done] = {
    for {
      clusters <- getClusters()
      resp <- url(adminPrefix + "tenants/" + tenantName).put(Json.obj("allowedClusters" -> clusters))
      _ <- validateResponse(resp, Set(200, 204, 409)) // 409 means already created
    } yield Done
  }

  def createNamespace(
                                tenantNamespace: String,
                                defaultNumberPartitions: Option[Int] = Some(1),
                              ): Future[Done] = {

    val topicOverride = defaultNumberPartitions.map(
      AutoTopicCreationOverride(
        "partitioned",
        true,
        _,
      )
    )

    val body = Json.toJson(NamespacePolicies(topicOverride))

    for {
      resp <- url(adminPrefix + "namespaces/" + tenantNamespace).put(body)
      _ <- validateResponse(resp, Set(200, 204, 409)) // 409 means already created
    } yield Done
  }

  def createTopic(topicName: String): Future[Boolean] = {
      parseTopicName(topicName) match {
        case Some((topicType, tenantNamespaceTopic)) =>
          for {
            resp <- url(adminPrefix + topicType + "/" + tenantNamespaceTopic).put(EmptyBody)
            _ <- validateResponse(
              resp,
              Set(200, 204, 409), // 409 indicates topic already exists
            )
          } yield {
            true
          }
        case _ =>
          Future.successful(false)
    }
  }

  def getTopics(tenantNamespace: String): Future[Seq[String]] = {
    for {
      resp <- url(adminPrefix + "persistent/" + tenantNamespace).get()
      _ <- validateResponse(resp, Set(200, 404))
    } yield {
      resp.body[JsValue].asOpt[Seq[String]].getOrElse(Seq.empty)
    }
  }

  protected def validateResponse(
                                  response: WSResponse,
                                  status: Set[Int],
                                ): Future[Done] = {
    if (status(response.status)) {
      Future.successful(Done)
    } else {
      Future.failed(
        new IllegalStateException(
          s"Expected status ${status.toSeq.sorted
            .mkString(",")} but got ${response.status} from ${response.uri.toString} with body: ${response.body}"
        )
      )
    }
  }

  protected def parseTopicName(topicName: String): Option[(String, String)] = {
    val topicNameRegex = "([non-]?persistent)://(\\S+)".r
    topicName match {
      case topicNameRegex(topicType, tenantNamespaceTopic) => Some((topicType, tenantNamespaceTopic))
      case _ => None
    }
  }
}
