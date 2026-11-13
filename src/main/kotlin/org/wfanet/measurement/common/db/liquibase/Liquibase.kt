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
import liquibase.Scope
import liquibase.UpdateSummaryOutputEnum
import liquibase.changelog.ChangeLogParameters
import liquibase.changelog.DatabaseChangeLog
import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.command.core.helpers.ShowSummaryArgument
import liquibase.database.DatabaseFactory
import liquibase.database.DatabaseList
import liquibase.database.jvm.JdbcConnection
import liquibase.exception.CommandValidationException
import liquibase.exception.ValidationErrors
import liquibase.logging.core.JavaLogService
import liquibase.resource.DirectoryResourceAccessor

object Liquibase {
  fun update(connection: Connection, changelogPath: Path) {
    DatabaseFactory.getInstance()
      .findCorrectDatabaseImplementation(JdbcConnection(connection))
      .use { database ->
        val resourceAccessor =
          DirectoryResourceAccessor(changelogPath.fileSystem.rootDirectories.first())
        val scopeObjects =
          mapOf(
            Scope.Attr.database.name to database,
            Scope.Attr.resourceAccessor.name to resourceAccessor,
          )

        Scope.child(scopeObjects) {
          Scope.getCurrentScope().setLogLevel(Level.INFO)
          val changeLogParameters = ChangeLogParameters(database)
          val databaseChangeLog =
            DatabaseChangelogCommandStep.getDatabaseChangeLog(
              changelogPath.toString(),
              changeLogParameters,
              database,
            )
          validateDbmsDefinitions(databaseChangeLog)

          CommandScope(*UpdateCommandStep.COMMAND_NAME)
            .apply {
              addArgumentValue(DbUrlConnectionArgumentsCommandStep.DATABASE_ARG, database)
              addArgumentValue(UpdateCommandStep.CHANGELOG_ARG, databaseChangeLog)
              addArgumentValue(
                DatabaseChangelogCommandStep.CHANGELOG_PARAMETERS,
                changeLogParameters,
              )
              addArgumentValue(ShowSummaryArgument.SHOW_SUMMARY_OUTPUT, UpdateSummaryOutputEnum.LOG)
            }
            .execute()
        }
      }
  }

  private fun validateDbmsDefinitions(databaseChangeLog: DatabaseChangeLog) {
    val validationErrors = ValidationErrors()
    for (changeSet in databaseChangeLog.changeSets) {
      val dbmsSet = changeSet.dbmsSet ?: continue
      val changeSetValidationErrors = ValidationErrors()
      DatabaseList.validateDefinitions(dbmsSet, changeSetValidationErrors)
      validationErrors.addAll(changeSetValidationErrors, changeSet)
    }

    if (validationErrors.hasErrors()) {
      throw CommandValidationException(
        "Invalid DBMS definitions: ${validationErrors.errorMessages.joinToString()}"
      )
    }
  }
}

private fun Scope.setLogLevel(level: Level) {
  val logService = get(Scope.Attr.logService, JavaLogService::class.java)
  if (logService.parent == null) {
    logService.parent = Logger.getLogger(this::class.java.name)
  }
  for (handler in logService.parent.handlers) {
    handler.level = level
  }
}
