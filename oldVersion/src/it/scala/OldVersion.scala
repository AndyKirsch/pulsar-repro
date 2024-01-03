import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.sksamuel.pulsar4s.Consumer
import com.sksamuel.pulsar4s.ConsumerConfig
import com.sksamuel.pulsar4s.ConsumerMessage
import com.sksamuel.pulsar4s.ProducerConfig
import com.sksamuel.pulsar4s.PulsarClient
import com.sksamuel.pulsar4s.Subscription
import com.sksamuel.pulsar4s.Topic
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionInitialPosition
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.ws.ahc.AhcWSClientProvider
import play.api.libs.ws.ahc.StandaloneAhcWSClient
import play.api.test.Helpers.{await, defaultAwaitTimeout}
import play.shaded.ahc.org.asynchttpclient.AsyncHttpClient
import play.shaded.ahc.org.asynchttpclient.DefaultAsyncHttpClient

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex

class OldVersion extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val schema: Schema[String] = Schema.STRING
  implicit val executor: ExecutionContextExecutor = system.dispatcher


  private lazy val WSClient = {
    lazy val asyncHttpClient: AsyncHttpClient = new DefaultAsyncHttpClient()
    lazy val standaloneWSClient = new StandaloneAhcWSClient(asyncHttpClient)
    new AhcWSClientProvider(standaloneWSClient.asInstanceOf[StandaloneAhcWSClient]).get
  }

  private val client = PulsarClient("pulsar://localhost:6650")
  private val iterableConfig = new PulsarConfig(url = "pulsar://localhost:6650", managementUrl = "http://localhost:9090")
  private val managementClient = new PulsarManagementClientImpl(iterableConfig, WSClient, executor)
  val tenant = "oldversion"

  def send(topicRoot: String = s"persistent://$tenant/ns1/sourcetest_"): Topic = {
    sendExact(topicRoot + UUID.randomUUID)
  }

  def sendExact(topicStr: String): Topic = {
    val topic = Topic(topicStr)
    val config = ProducerConfig(topic)
    val producer = client.producer(config)
    val receipt = producer.send("test")
    assert(receipt.isSuccess)
    producer.close()
    topic
  }

  def assertGotAMessage(result: Try[Option[ConsumerMessage[String]]]) = {
    result match {
      case Success(opt) => opt match {
        case Some(msg) => // intentionally blank
        case None => fail("No Message")
      }
      case Failure(exception) => fail(exception)
    }
  }

  def patternConsumer(regex: Regex): Consumer[String] = {
    client.consumer(
      ConsumerConfig(
        topicPattern = Some(regex),
        subscriptionName = Subscription.generate,
        subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest),
        patternAutoDiscoveryPeriod = Some(1.seconds)
      ))
  }

  def topicConsumer(topic: String): Consumer[String] = {
    client.consumer(
      ConsumerConfig(
        topics = Seq(Topic(topic)),
        subscriptionName = Subscription.generate,
        subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest),
        patternAutoDiscoveryPeriod = Some(1.seconds)
      ))
  }

  val timeout = 5.seconds

  test("regex subscription not partitioned, exact match") {
    val topic = send()
    val result = patternConsumer(topic.name.replace("persistent://", "").r).receive(timeout)
    assertGotAMessage(result)
  }

  test("regex subscription not partitioned") {
    val result = patternConsumer(s"$tenant/ns1/sourcetest_.*".r).receiveAsync
    send()

    await(result)(timeout)
  }

  test("regex subscription 1 partition, exact match") {
    val topic = send(s"persistent://$tenant/ns2/sourcetest_")
    val result = patternConsumer(topic.name.replace("persistent://", "").r).receive(timeout)
    assertGotAMessage(result)
  }

  test("regex subscription 1 partition") {
    val result = patternConsumer(s"$tenant/ns2/sourcetest_.*".r).receiveAsync
    send(s"persistent://$tenant/ns2/sourcetest_")

    await(result)(timeout)
  }

  test("regex subscription 2 partitions, exact match") {
    val topic = send(s"persistent://$tenant/ns3/sourcetest_")
    val result = patternConsumer(topic.name.replace("persistent://", "").r).receive(timeout)
    assertGotAMessage(result)
  }

  test("regex subscription 2 partitions") {
    val result = patternConsumer(s"$tenant/ns3/sourcetest_.*".r).receiveAsync
    send(s"persistent://$tenant/ns3/sourcetest_")

    await(result)(timeout)
  }

  test("regex subscription partitioned, exact match") {
    val result = patternConsumer(s"$tenant/ns4/sourcetest".r).receiveAsync
    val topic = sendExact(s"persistent://$tenant/ns4/sourcetest")

    await(result)(timeout)
  }

  test("regex subscription partitioned, produce then consume") {
    val result = patternConsumer(s"$tenant/ns5/sourc.test".r).receiveAsync
    val topic = sendExact(s"persistent://$tenant/ns5/sourcetest")

    await(result)(timeout)
  }

  test("topic sub") {
    val topic = send()

    val result = client.consumer(
      ConsumerConfig(topics = Seq(topic), subscriptionName = Subscription.generate, subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest)
      )).receive(timeout)

    assertGotAMessage(result)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    await(for {
      _ <- managementClient.createTenant(tenant)
      _ <- managementClient.createNamespace(s"$tenant/ns1", None) // Not partitioned
      _ <- managementClient.createNamespace(s"$tenant/ns2", Some(1)) // 1 partition
      _ <- managementClient.createNamespace(s"$tenant/ns3", Some(2)) // 2 partitions
      _ <- managementClient.createNamespace(s"$tenant/ns4", Some(2)) // 2 partitions
      _ <- managementClient.createNamespace(s"$tenant/ns5", Some(2)) // 2 partitions
    } yield ())(10.seconds)
  }


  def cleanupPulsarTenant(pulsarClient: PulsarManagementClientImpl, tenantName: String, deleteTenant: Boolean = false): Boolean = {
    val tenants = await(pulsarClient.getTenants())

    if (tenants.contains(tenantName)) {
      val namespaces = await(pulsarClient.getNamespaces(tenantName))
      val namespaceDeleteResults = namespaces.map { namespace =>
        val topics = await(pulsarClient.getTopics(namespace))
        val topicDeleteResults = topics.filterNot(_.contains("__change_events")).map { topic =>
          Try(await(pulsarClient.deleteTopic(topic))) match {
            case Success(_) =>
              println(s"deleted Pulsar topic $topic")
              true
            case Failure(ex) =>
              println(s"failed to delete Pulsar topic $topic", ex)
              false
          }
        }
        Try(await(pulsarClient.deleteNamespace(namespace))) match {
          case Success(_) =>
            println(s"deleted Pulsar namespace $namespace")
            topicDeleteResults.forall(x => x)
          case Failure(ex) =>
            println(s"failed to delete Pulsar namespace $namespace", ex)
            false
        }
      }
      if (deleteTenant) {
        Try(await(pulsarClient.deleteTenant(tenantName))) match {
          case Success(result) =>
            println(s"deleted Pulsar tenant $tenantName")
            namespaceDeleteResults.forall(x => x)
          case Failure(ex) =>
            println(s"failed to delete Pulsar tenant $tenantName", ex)
            false
        }
      } else {
        true
      }
    } else {
      println(s"tenant $tenantName doesn't exist, there is nothing to delete")
      true
    }
  }
  override def afterAll(): Unit = {
    super.afterAll()
    cleanupPulsarTenant(managementClient, tenant, true)
    client.close()
    WSClient.close()
  }
}
