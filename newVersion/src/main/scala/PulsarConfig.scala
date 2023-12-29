import scala.concurrent.duration._

import org.apache.pulsar.client.api.Authentication

case class PulsarConfig(
                         url: String,
                         managementUrl: String,
                         connectionsPerBroker: Int = 1,
                         listenerThreads: Int = 1,
                         ioThreads: Int = 1,
                         operationTimeout: FiniteDuration = 30.seconds,
                         connectionTimeout: FiniteDuration = 10.seconds,
                         maxBackoffInterval: FiniteDuration = 1.minute,
                         keepAliveInterval: FiniteDuration = 1.minute,
                         enableTcpNoDelay: Boolean = false,
                         authToken: Option[String] = None,
                       ) {
  lazy val authentication: Option[Authentication] = None
}