-- liquibase formatted sql

-- Copyright 2022 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- changeset sanjayvas:create-cars-table dbms:postgresql
CREATE TABLE Cars (
  CarId bigint NOT NULL,
  Year integer NOT NULL,
  Make text NOT NULL,
  Model text NOT NULL,
  Owner text,

  -- Serialized google.type.LatLng protobuf message.
  CurrentLocation bytea,
  -- google.type.DayOfWeek protobuf enum number.
  WeeklyWashDay integer NOT NULL DEFAULT 0,

  PRIMARY KEY (CarId)
);

CREATE TABLE CarDescriptions (
    CarId bigint NOT NULL,
    Description bytea,

    PRIMARY KEY (CarId)
)