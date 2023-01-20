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

package org.wfanet.measurement.common.db.liquibase

import java.nio.file.Path
import java.sql.Connection
import java.util.logging.Level
import java.util.logging.Logger
import liquibase.Liquibase
import liquibase.Scope
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.logging.core.JavaLogService
import liquibase.resource.DirectoryResourceAccessor

object Liquibase {
  fun fromPath(connection: Connection, changelogPath: Path): Liquibase {
    return Liquibase(
      changelogPath.toString(),
      DirectoryResourceAccessor(changelogPath.fileSystem.rootDirectories.first()),
      DatabaseFactory.getInstance().findCorrectDatabaseImplementation(JdbcConnection(connection))
    )
  }
}

fun Scope.setLogLevel(level: Level) {
  val logService = get(Scope.Attr.logService, JavaLogService::class.java)
  if (logService.parent == null) {
    logService.parent = Logger.getLogger(this::class.java.name)
  }
  for (handler in logService.parent.handlers) {
    handler.level = level
  }
}
