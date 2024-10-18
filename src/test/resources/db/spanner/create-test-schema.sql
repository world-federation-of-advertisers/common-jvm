-- liquibase formatted sql

-- Copyright 2024 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- changeset sanjayvas:create-cars-table dbms:cloudspanner

-- Set protobuf FileDescriptorSet as a base64 string. This gets applied to the next DDL batch.
SET PROTO_DESCRIPTORS = 'CqQCChtnb29nbGUvdHlwZS9kYXlvZndlZWsucHJvdG8SC2dvb2dsZS50eXBlKoQBCglEYXlPZldlZWsSGwoXREFZX09GX1dFRUtfVU5TUEVDSUZJRUQQABIKCgZNT05EQVkQARILCgdUVUVTREFZEAISDQoJV0VETkVTREFZEAMSDAoIVEhVUlNEQVkQBBIKCgZGUklEQVkQBRIMCghTQVRVUkRBWRAGEgoKBlNVTkRBWRAHQmkKD2NvbS5nb29nbGUudHlwZUIORGF5T2ZXZWVrUHJvdG9QAVo+Z29vZ2xlLmdvbGFuZy5vcmcvZ2VucHJvdG8vZ29vZ2xlYXBpcy90eXBlL2RheW9md2VlaztkYXlvZndlZWuiAgNHVFBiBnByb3RvMwrYAQoYZ29vZ2xlL3R5cGUvbGF0bG5nLnByb3RvEgtnb29nbGUudHlwZSJCCgZMYXRMbmcSGgoIbGF0aXR1ZGUYASABKAFSCGxhdGl0dWRlEhwKCWxvbmdpdHVkZRgCIAEoAVIJbG9uZ2l0dWRlQmMKD2NvbS5nb29nbGUudHlwZUILTGF0TG5nUHJvdG9QAVo4Z29vZ2xlLmdvbGFuZy5vcmcvZ2VucHJvdG8vZ29vZ2xlYXBpcy90eXBlL2xhdGxuZztsYXRsbmf4AQGiAgNHVFBiBnByb3RvMw==';

START BATCH DDL;

CREATE PROTO BUNDLE(
  `google.type.DayOfWeek`,
  `google.type.LatLng`
);

CREATE TABLE Cars (
  CarId INT64 NOT NULL,
  Year INT64 NOT NULL,
  Make STRING(MAX) NOT NULL,
  Model STRING(MAX) NOT NULL,
  Owner STRING(MAX),

  CurrentLocation `google.type.LatLng`,
  WeeklyWashDay `google.type.DayOfWeek` NOT NULL DEFAULT (0),
) PRIMARY KEY (CarId);

RUN BATCH;