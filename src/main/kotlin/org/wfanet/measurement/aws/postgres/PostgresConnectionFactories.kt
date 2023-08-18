package org.wfanet.measurement.aws.postgres

import com.google.gson.Gson
import io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider
import io.r2dbc.postgresql.client.SSLMode
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest

data class PostgresCredential(
  val username: String,
  val password: String,
)

object PostgresConnectionFactories {
  @JvmStatic
  fun buildConnectionFactory(flags: PostgresFlags): ConnectionFactory {

    val secretManagerCli = SecretsManagerClient.builder()
      .region(Region.of(flags.region))
      .credentialsProvider(ProfileCredentialsProvider.create())
      .build()

    val secret = secretManagerCli.getSecretValue{ GetSecretValueRequest.builder().secretId(flags.credentialSecretName)}.secretString()
    val postgresCredential = Gson().fromJson(secret, PostgresCredential::class.java)

    return ConnectionFactories.get(
      ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, PostgresqlConnectionFactoryProvider.POSTGRESQL_DRIVER)
        .option(ConnectionFactoryOptions.PROTOCOL, "postgresql")
        // a non-empty password is required, but the value doesn't matter
        .option(ConnectionFactoryOptions.USER, postgresCredential.username)
        .option(ConnectionFactoryOptions.PASSWORD, postgresCredential.password)
        .option(ConnectionFactoryOptions.DATABASE, flags.database)
        .option(ConnectionFactoryOptions.HOST, flags.host)
        .option(PostgresqlConnectionFactoryProvider.SSL_MODE, SSLMode.REQUIRE)
        .build()
    )
  }
}
