package org.wfanet.measurement.common.db.r2dbc.postgres

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryMetadata
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest
import software.amazon.awssdk.services.rds.RdsAsyncClient

fun interface ConnectionFactoryGenerator {
  // user and pass just for generic usage
  fun generate(username: String, password: String): ConnectionFactory
}

class RdsPostgresConnectionFactory(
  private val client: RdsAsyncClient,
  private val host: String,
  private val port: Int,
  private val username: String,
  private val generator: ConnectionFactoryGenerator
): ConnectionFactory {
  // ugly, I know, but you need this to be the same metadata as underlying connection
  private val metadata = generator.generate(username, "fake").metadata

  override fun create(): Publisher<Connection> =
    Mono.defer {
      GenerateAuthenticationTokenRequest.builder()
        .username(username)
        .port(port)
        .hostname(host)
        .build()
        .let(client.utilities()::generateAuthenticationToken)
        .let { generator.generate(username, it) }
        .create()
        .let { Mono.from(it) }
    }

  override fun getMetadata(): ConnectionFactoryMetadata {
    return metadata
  }
}
