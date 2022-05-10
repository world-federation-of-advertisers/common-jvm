/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.gcloud.spanner.tools

import java.sql.DriverManager
import java.util.logging.Level
import java.util.logging.Logger
import liquibase.Contexts
import liquibase.Scope
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.liquibase.Liquibase
import org.wfanet.measurement.common.db.liquibase.setLogLevel
import org.wfanet.measurement.common.getJarResourcePath
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

@Command
class UpdateSchema : Runnable {
  @Mixin private lateinit var flags: SpannerFlags

  @Option(
    names = ["--changelog"],
    description = ["Liquibase changelog resource name"],
    required = true,
  )
  private lateinit var changelog: String

  override fun run() {
    val connectionString = flags.jdbcConnectionString

    val changelogPath =
      checkNotNull(Thread.currentThread().contextClassLoader.getJarResourcePath(changelog)) {
        "JAR resource $changelog not found"
      }

    logger.info("Connecting to $connectionString")
    DriverManager.getConnection(connectionString).use { connection ->
      logger.info("Loading changelog from $changelogPath")
      Liquibase.fromPath(connection, changelogPath).use { liquibase ->
        logger.info("Updating...")
        Scope.getCurrentScope().setLogLevel(Level.FINE)
        liquibase.update(Contexts())
      }
    }
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)

    @JvmStatic fun main(args: Array<String>) = commandLineMain(UpdateSchema(), args)
  }
}
