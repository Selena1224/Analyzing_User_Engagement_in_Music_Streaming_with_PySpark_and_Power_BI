# Analyzing User Engagement in Music Streaming with PySpark and Power BI

## Project Overview  
This project models an end-to-end big data pipeline for a music streaming company, designed to analyze **user engagement**, **feature adoption**, and **paid subscription conversion**.  

The business problem addressed is one faced by all streaming platforms: understanding how customers interact with the platform, who becomes a highly engaged user, and what proportion of users convert to paid tiers. These insights inform product decisions, marketing strategies, and retention efforts.  

The system ingests raw parquet data from **AWS S3**, processes it with **Databricks and PySpark**, stores curated results in **Delta Lake**, and delivers interactive dashboards in **Power BI**. It demonstrates the integration of batch analytics, real-time streaming analytics, and a simple machine learning model into one unified workflow.  

---

## Business Questions  
The pipeline was designed to answer:  
1. What percentage of customers are actively adopting the platform?  
2. How many users are highly engaged, defined as 80 or more sessions within a time window?  
3. What share of active users convert to paid subscriptions?  
4. How can streaming analytics highlight spikes or anomalies in user activity?  
5. Can historical data be used to predict which users are most likely to remain engaged?  

---

## Technology Stack  
- **AWS S3** – cloud object storage for parquet-formatted raw data (customers, artists, tracks, sessions, events).  
- **Databricks + PySpark** – scalable batch and streaming data processing.  
- **Delta Lake** – reliable data lakehouse layer for curated tables and incremental writes.  
- **Power BI** – visualization and dashboarding for business stakeholders.  

---

## Pipeline Design  
1. **Data Ingestion** – Raw parquet files (sessions, customers, artists, tracks, events) loaded from S3 into Databricks.  
2. **Batch Processing** – Session timestamps normalized, active users identified, and KPIs calculated.  
3. **Streaming Processing** – Real-time session ingestion using Databricks Auto Loader with watermarking and 5-minute windows. Outputs stored in Delta tables for monitoring.  
4. **Anomaly Detection** – Percentile-based spike detection alerts when session counts exceed recent activity baselines.  
5. **Machine Learning Demo** – Threshold classifier to predict engaged users based on historical session counts.  
6. **Visualization** – Interactive Power BI dashboards connected to Delta Lake, displaying adoption, engagement, and paid conversion trends.  

---

## Key Features  

### Batch KPIs  
- **Session Adoption Rate (%)** – Share of customers who started at least one session in July–August 2025.  
- **Engaged Users (%)** – Share of active users with at least 80 sessions in the same period.  
- **Paid Share (%)** – Percentage of active users with a paid subscription during the analysis window.  

### Streaming Analytics  
- Continuous ingestion of session data with Databricks Auto Loader.  
- Five-minute aggregate tables capturing session volume and distinct customers.  
- Alerts generated when activity exceeds the 95th percentile of recent traffic.  

### Machine Learning  
- Threshold-based classifier that predicts whether a user is likely to be “engaged” (≥80 sessions).  
- Trained and evaluated on historical session counts with metrics such as precision, recall, F1, and accuracy.  
- Demonstrates how engagement metrics can transition from descriptive analysis to predictive insights.  

---

## Repository Contents  
- `Music_Streaming_Analytics_Notebook.py` – Databricks-exported PySpark notebook containing the full data pipeline implementation.  
- `Music_Streaming_Analytics_Report.pdf` – Comprehensive report outlining project objectives, architecture, implementation, results, and key lessons learned.  
- `Music_Streaming_Analytics_Dashboard.pbix` – Power BI dashboard presenting interactive KPIs and visualizations of user engagement and adoption trends.  
- `Music_Streaming_Analytics_Architecture.drawio` – End-to-end architecture diagram mapping the data flow across AWS S3, Databricks, Delta Lake, and Power BI.    

---

## Results  
- **Session Adoption Rate:** ~89% of customers adopted the platform during the analysis window.  
- **Engaged Users:** ~77% of active users exceeded the 80-session threshold.  
- **Paid Share:** ~61% of active users were on a paid subscription.  

The streaming component successfully ingested live session data into 5-minute aggregates, and anomaly detection flagged sudden usage spikes. The machine learning demo showed how session history can be used to predict customer engagement levels.  

---

## Lessons Learned  
- Schema mismatches in large parquet files (especially `events.parquet`) require careful debugging.  
- Watermarking and windowing are critical for structured streaming pipelines to avoid dropped or duplicated data.  
- Consistent naming conventions are essential for connecting Delta outputs to Power BI dashboards.  
- Delta Lake is invaluable for managing incremental writes and ensuring reliable curated datasets.  
- Building dashboards emphasized the importance of clean, well-structured upstream data.  

---

## Academic Context  
This project was completed as the final assignment for **ISM 6362 – Big Data and Cloud-Based Tools** at Seattle Pacific University. It integrates cloud storage, distributed computing, structured streaming, Delta Lake reliability, machine learning, and dashboard visualization into a single end-to-end system.  

The work demonstrates both **technical proficiency** (Spark streaming, Delta Lake, Power BI integration) and the ability to translate analytics into **actionable business insights** for the music industry.  
