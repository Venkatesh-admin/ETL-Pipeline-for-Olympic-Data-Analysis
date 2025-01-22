# Olympics Data Analytics Pipeline

## Project Overview
This project demonstrates the development of an end-to-end data pipeline to process Olympics-related datasets for analytics purposes. The pipeline uses Azure Data Factory (ADF), Azure Databricks, and Snowflake to ingest, transform, and store data for analysis in tools like Power BI or Tableau.

## Architecture

1. **Data Ingestion**:
   - Used **ADF Copy Activities** to ingest multiple datasets (e.g., athletes, coaches, entries gender, medals, teams) into the **raw folder** in Azure Storage.

2. **Data Transformation**:
   - Used **Azure Databricks** to extract data from the raw folder in Azure Storage.
   - Performed transformations to clean, enrich, and prepare the data for analytics.
   - Stored the transformed data in the **output folder** in Azure Storage.

3. **Data Load**:
   - Used **Snowflake COPY** command to load the transformed datasets from the output folder into Snowflake as final tables.

## Datasets Used
1. **Athletes Dataset**:
   - Columns: `PersonName`, `Country`, `Discipline`
   - Example Transformation:
     - Added `AthleteID` as a unique identifier.
     - Grouped by `Country` and `Discipline` to calculate counts.

2. **Coaches Dataset**:
   - Columns: `Name`, `Country`, `Discipline`, `Event`
   - Example Transformation:
     - Renamed `Name` to `CoachName` and added `CoachID`.
     - Replaced `NULL` in the `Event` column with actual `NULL` values.

3. **Entries Gender Dataset**:
   - Columns: `Discipline`, `Female`, `Male`, `Total`
   - Example Transformation:
     - Added `FemaleMaleRatio` and `GenderBalance` to identify gender distribution.

4. **Medals Dataset**:
   - Columns: `Rank`, `TeamCountry`, `Gold`, `Silver`, `Bronze`, `Total`, `Rank by Total`
   - Example Transformation:
     - Calculated `GoldPercentage`, `SilverPercentage`, and `BronzePercentage`.
     - Aggregated medals by `TeamCountry`.

5. **Teams Dataset**:
   - Columns: `TeamName`, `Discipline`, `Country`, `Event`
   - Example Transformation:
     - Added `TeamID` and calculated total participants per team.

## Pipeline Steps

### Step 1: Data Ingestion
- **Tools Used**: Azure Data Factory (ADF)
- **Process**:
  - Configured ADF pipelines with Copy Activities to ingest datasets from the source into the raw folder in Azure Storage.
  - Ensured proper folder structure for each dataset (e.g., `raw/athletes`, `raw/coaches`, etc.).

### Step 2: Data Transformation
- **Tools Used**: Azure Databricks (PySpark)
- **Process**:
  - Extracted datasets from the raw folder.
  - Applied transformations for data cleaning, standardization, enrichment, and aggregation.
  - Stored the transformed data in the output folder in Azure Storage.
- **Key Transformations**:
  - Cleaned `NULL` values (e.g., replaced "NULL" strings with actual `NULL`).
  - Added calculated fields (e.g., `AthleteID`, `FemaleMaleRatio`, `GoldPercentage`).
  - Aggregated data for analytics (e.g., total medals by country).

### Step 3: Data Load
- **Tools Used**: Snowflake
- **Process**:
  - Used the Snowflake `COPY` command to load transformed datasets from the output folder into Snowflake tables.
  - Final tables were structured for direct use in BI tools.

## Final Data Schema in Snowflake

1. **Athletes Table**:
   - Columns: `AthleteID`, `Name`, `Country`, `Discipline`, `Gender`

2. **Coaches Table**:
   - Columns: `CoachID`, `CoachName`, `Country`, `Discipline`, `Event`

3. **Entries Table**:
   - Columns: `Discipline`, `Female`, `Male`, `Total`, `FemaleMaleRatio`, `GenderBalance`

4. **Medals Table**:
   - Columns: `TeamCountry`, `Gold`, `Silver`, `Bronze`, `Total`, `GoldPercentage`, `SilverPercentage`, `BronzePercentage`

5. **Teams Table**:
   - Columns: `TeamID`, `TeamName`, `Country`, `Discipline`, `Event`, `TotalParticipants`

## Technologies Used
- **Azure Data Factory (ADF)**:
  - Ingested raw datasets into Azure Storage.
- **Azure Databricks**:
  - Performed data transformations using PySpark.
- **Snowflake**:
  - Stored the final transformed datasets as analytics-ready tables.

## Benefits
- **Automation**: The pipeline automates data ingestion, transformation, and loading processes.
- **Scalability**: Can handle additional datasets or growing data volumes.
- **Analytics-Ready**: Final datasets are optimized for analytics in BI tools like Power BI or Tableau.

## How to Use
1. **Prerequisites**:
   - Azure Storage account with raw and output folders.
   - Databricks cluster configured with access to Azure Storage.
   - Snowflake account with required permissions.

2. **Run the Pipeline**:
   - Use ADF to copy datasets into the raw folder.
   - Execute the Databricks notebooks to perform transformations and write to the output folder.
   - Use Snowflake `COPY` commands to load transformed data into final tables.

3. **Analyze the Data**:
   - Connect Power BI or Tableau to Snowflake.
   - Use the final tables to create visualizations and perform analytics.

## Sample Queries for Analytics
- **Top Countries by Total Medals**:
  ```sql
  SELECT TeamCountry, Total
  FROM Medals
  ORDER BY Total DESC;
  ```
- **Gender Participation Ratio**:
  ```sql
  SELECT Discipline, FemaleMaleRatio
  FROM Entries
  ORDER BY FemaleMaleRatio DESC;
  ```
- **Medal Distribution by Country**:
  ```sql
  SELECT TeamCountry, Gold, Silver, Bronze
  FROM Medals
  ORDER BY Gold DESC;
  ```

## Future Enhancements
- Integrate streaming data sources for real-time updates.
- Add machine learning models to predict medal counts or athlete performance.
- Enable dynamic schema updates for new datasets.

