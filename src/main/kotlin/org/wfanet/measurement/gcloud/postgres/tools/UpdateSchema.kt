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

package org.wfanet.measurement.gcloud.postgres.tools

import java.sql.DriverManager
import java.util.Properties
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.db.liquibase.tools.BaseUpdateSchema
import org.wfanet.measurement.gcloud.postgres.PostgresFlags
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin

@Command
class UpdateSchema : BaseUpdateSchema() {
  @Mixin private lateinit var flags: PostgresFlags

  override fun run() {
    val connectionString = "jdbc:postgresql:///$flags.database"
    val props = Properties()
    props.setProperty("cloudSqlInstance", flags.cloudSqlInstance)
    props.setProperty("socketFactory", SOCKET_FACTORY_CLASS)
    props.setProperty("user", flags.user)
    // a non-empty password is required, but the value doesn't matter
    props.setProperty("password", "UNUSED")
    props.setProperty("enableIamAuth", "true")
    DriverManager.getConnection(connectionString, props).use { run(it) }
  }

  companion object {
    private const val SOCKET_FACTORY_CLASS = "com.google.cloud.sql.postgres.SocketFactory"
    @JvmStatic fun main(args: Array<String>) = commandLineMain(UpdateSchema(), args)
  }
}
