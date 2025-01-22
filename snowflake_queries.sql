CREATE DATABASE OLYMPICS_DB;
USE DATABASE OLYMPICS_DB;
CREATE OR REPLACE SCHEMA AZURE_STAGE;
CREATE OR REPLACE SCHEMA FILE_FORMATS;
-- create integration object that contains the access information
CREATE STORAGE INTEGRATION azure_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '985adafb-97bd-46f6-92d8-8846e167a468'
  STORAGE_ALLOWED_LOCATIONS = ( 'azure://tokyoolympicdata.blob.core.windows.net/tokyoolympiccontainer/transformeddata/');

  -- https://snowflakepractice1702.blob.core.windows.net/snowflake/CarModels.json
-- https://snowflakepractice1702.blob.core.windows.net/snowflake/world-happiness-report-2021.csv
  
-- Describe integration object to provide access
DESC STORAGE integration azure_integration;

  
---- Create file format & stage objects ----

-- create file format
create or replace file format olympics_db.file_formats.fileformat_azure
    TYPE = PARQUET;

-- create stage object
create or replace stage olympics_db.azure_stage.stage_azure
    STORAGE_INTEGRATION = azure_integration
    URL = 'azure://tokyoolympicdata.blob.core.windows.net/tokyoolympiccontainer/transformeddata/'
    FILE_FORMAT = olympics_db.file_formats.fileformat_azure;
    

-- list files
LIST @olympics_db.azure_stage.stage_azure;


CREATE OR REPLACE TABLE athletes_transformed (
    AthleteID BIGINT,
    PersonName STRING,
    Country STRING,
    Discipline STRING
);

COPY INTO athletes_transformed
FROM @olympics_db.azure_stage.stage_azure/athletes_transformed;


CREATE OR REPLACE TABLE coaches_transformed (
    CoachID BIGINT,
    CoachName STRING,
    Country STRING,
    Discipline STRING,
    Event STRING
);

COPY INTO coaches_transformed
FROM @olympics_db.azure_stage.stage_azure/coaches_transformed;

CREATE OR REPLACE TABLE entries_transformed (
    Discipline STRING,
    Female INT,
    Male INT,
    Total INT,
    FemaleMaleRatio FLOAT,
    GenderBalance STRING
);

COPY INTO entries_transformed
FROM @olympics_db.azure_stage.stage_azure/entries_transformed;


CREATE OR REPLACE TABLE medals_transformed (
    Rank INT,
    TeamCountry STRING,
    Gold INT,
    Silver INT,
    Bronze INT,
    Total INT,
    RankByTotal INT,
    GoldPercentage FLOAT,
    SilverPercentage FLOAT,
    BronzePercentage FLOAT
);

COPY INTO medals_transformed
FROM @olympics_db.azure_stage.stage_azure/medals_transformed

CREATE OR REPLACE TABLE teams_transformed (
    TeamID BIGINT,
    TeamName STRING,
    Country STRING,
    Discipline STRING,
    TotalParticipants INT
);

COPY INTO teams_transformed
FROM @olympics_db.azure_stage.stage_azure/teams_transformed




