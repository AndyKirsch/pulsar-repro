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
import play.api.test.Helpers.await
import play.shaded.ahc.org.asynchttpclient.AsyncHttpClient
import play.shaded.ahc.org.asynchttpclient.DefaultAsyncHttpClient

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.ExecutionContextExecutor
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex

class MinimalRepro extends AnyFunSuite with Matchers with BeforeAndAfterAll {
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

  def send(topicRoot: String = "persistent://repro/ns1/sourcetest_"): Topic = {
    val topic = Topic(topicRoot + UUID.randomUUID)
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
        subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest)
      ))
  }

  val timeout = 5.seconds

  test("regex subscription not partitioned") {
    send()
    val result = patternConsumer("persistent://repro/ns1/sourcetest_.*".r).receive(timeout)
    assertGotAMessage(result)
  }

  test("regex subscription 1 partition") {
    send("persistent://repro/ns2/sourcetest_")
    val result = patternConsumer("persistent://repro/ns2/sourcetest_.*".r).receive(timeout)
    assertGotAMessage(result)
  }

  test("regex subscription 2 partitions") {
    send("persistent://repro/ns3/sourcetest_")
    val result = patternConsumer("persistent://repro/ns3/sourcetest_.*".r).receive(timeout)
    assertGotAMessage(result)
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
      _ <- managementClient.createTenant("repro")
      _ <- managementClient.createNamespace("repro/ns1", None) // Not partitioned
      _ <- managementClient.createNamespace("repro/ns2", Some(1)) // 1 partition
      _ <- managementClient.createNamespace("repro/ns3", Some(2)) // 2 partitions
    } yield ())(10.seconds)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    client.close()
    WSClient.close()
  }
}
