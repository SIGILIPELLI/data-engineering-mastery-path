# Level 1 · Entry <span class="level-badge">Foundations</span>

Goal: understand every moving part of a data pipeline — Python and SQL for
data work, ETL, data modeling, file formats, batch processing, orchestration
concepts, and data quality — and ship a working ETL pipeline built from those
parts in plain Python.

## Modules

1. [What Is Data Engineering?](01-what-is-data-engineering.md)
2. [Python for Data Engineering](02-python-for-data-engineering.md)
3. [SQL for Data Engineers](03-sql-for-data-engineers.md)
4. [ETL Fundamentals](04-etl-fundamentals.md)
5. [Data Modeling Basics](05-data-modeling-basics.md)
6. [Working with File Formats](06-file-formats.md)
7. [Batch Processing Basics](07-batch-processing-basics.md)
8. [Intro to Orchestration](08-intro-to-orchestration.md)
9. [Data Quality & Validation](09-data-quality-validation.md)
10. [Project — Build a Simple ETL Pipeline](10-project-simple-etl-pipeline.md)

By the end of this level you'll be able to take a folder of raw files, model
them into a clean schema, load them into a database with validation, and
recognize (and fix) the most common ways real-world pipelines break — schema
drift, duplicate loads, and late-arriving data.

!!! info "Setup for this level"
    ```bash
    pip install pandas
    ```
    Everything else — `sqlite3`, `csv`, `json`, `pathlib` — is in the Python
    standard library. No cloud account, no API key, no server to install. All
    code on this level runs fully offline.
