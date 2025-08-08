# Real‑time CDC Pipeline Framework

This repository contains a Scala/Apache Beam framework for building real‑time change data capture (CDC) pipelines on Google Dataflow.  The goal of this project is to provide a reusable set of components that the data analytics team can use to ingest data from multiple source systems, process it through a series of data zones (raw → structure → refined → analytics) and maintain a high‑fidelity source of truth while running in parallel with existing AWS pipelines.

## 🏗️ Architecture Overview

The solution is divided into two parts:

1. **Framework** – a generic library implementing core pipeline orchestration, ingestion strategies, transformation stages, reconciliation, data quality and auditing.  These components live in the `framework/` directory and can be reused across multiple business domains.
2. **Business domain pipelines** – each business domain (e.g. Member, Orders, Products) owns its own repository that references the framework and supplies domain‑specific configuration and transformation logic.  An example domain (`member`) is provided under `member-pipeline/`.

### High‑level flow

```
┌───────────────┐    ┌─────────────────┐    ┌────────────────┐
│ Pub/Sub       │──▶ │ Dataflow (Beam) │──▶ │ BigQuery/BigTable│
│ notifications │    │  Pipeline       │    │ (OLAP / OLTP)  │
└───────────────┘    └─────────────────┘    └────────────────┘
                              │
                              ▼
              ┌─────────────────────────────────────┐
              │ Audit, Reconcile and DQ results     │
              │ stored on GCS / BigQuery            │
              └─────────────────────────────────────┘
```

The pipeline consumes notifications from a Pub/Sub topic, fetches the corresponding data from BigQuery (provided by the upstream collector), determines the ingestion type (full dump, incremental, upsert or transform‑structure) from configuration, and processes the records through each zone.  At every stage the framework can reconcile against the legacy AWS S3 tables, run data quality rules, and emit audit logs.  Metrics are collected and exported to Cloud Monitoring for observability.

## ✨ Framework Modules

The framework is organised into packages under `framework/`:

* **pipeline.core** – classes for orchestrating Beam pipelines (`PipelineOrchestrator.scala`), managing windowing (`WindowManager.scala`) and handling state (`StateManager.scala`).
* **pipeline.ingestion** – concrete ingestion strategies: `FullDumpIngestion`, `IncrementalIngestion`, `UpsertIngestion` and `TransformStructureIngestion`.
* **pipeline.transformation** – domain‑agnostic transformation stages for refined and analytics zones.
* **modules.dependency** – dependency resolution and checks to ensure prerequisites are met before processing.
* **modules.reconciliation** – mapping schemas between BigQuery and AWS and producing reconciliation audit logs.
* **modules.quality** – a simple data quality engine with configurable rules and auditing.
* **modules.audit** – emit structured audit logs and persist them to GCS/BigQuery.
* **connectors** – wrappers around Google Cloud and AWS services (Pub/Sub, BigQuery, S3, GCS) to simplify I/O.
* **utils** – helpers for loading YAML configuration, registering schemas and collecting metrics.

This modular design allows you to extend or replace individual components without rewriting the entire pipeline.  For example, if a new ingestion type is required, you can implement a new class in `pipeline.ingestion` and register it in `PipelineOrchestrator`.

## 🧑‍💻 Example Member Pipeline

The `member-pipeline/` directory demonstrates how to build a domain‑specific pipeline using the framework.  It contains configuration files defining the pipeline structure (`config/pipeline_config.yaml`), any cross‑table dependencies, reconciliation mapping and data quality rules.  Domain‑specific transformation logic lives in `transformations/` and the top‑level entry point is `dataflow/MemberPipeline.scala`.

To run the Member pipeline on Dataflow you would package the framework and member code as a JAR and submit it to Google Dataflow:

```bash
sbt "project memberPipeline" assembly
gcloud dataflow flex-template build ...
```

The supplied code is meant as a starting point and illustrates how to structure your own CDC pipelines.  You should adapt the windowing strategy, transformation logic and quality rules to suit your data domain and performance requirements.

## 🛠️ Building the Project

This repository uses [sbt](https://www.scala-sbt.org/) for building.  The root `build.sbt` declares a multi‑project build: one subproject for the reusable framework and another for the member domain pipeline.

1. Install JDK 8 or 11 and [sbt](https://www.scala-sbt.org/download.html).
2. From the repository root, run:
   ```bash
   sbt compile
   ```
3. To run unit tests (if added in `src/test/scala`), run:
   ```bash
   sbt test
   ```
4. To package the member pipeline into a JAR:
   ```bash
   sbt memberPipeline/assembly
   ```

## 📂 Repository layout

```
gpt5_helper_cdc_realtime/
├── framework/
│   ├── build.sbt                 ⇦ Framework subproject definition
│   ├── src/main/scala/
│   │   ├── pipeline/
│   │   │   ├── core/
│   │   │   │   ├── PipelineOrchestrator.scala
│   │   │   │   ├── WindowManager.scala
│   │   │   │   └── StateManager.scala
│   │   │   ├── ingestion/
│   │   │   │   ├── FullDumpIngestion.scala
│   │   │   │   ├── IncrementalIngestion.scala
│   │   │   │   ├── UpsertIngestion.scala
│   │   │   │   └── TransformStructureIngestion.scala
│   │   │   └── transformation/
│   │   │       ├── RefinedTransformation.scala
│   │   │       └── AnalyticsTransformation.scala
│   │   ├── modules/
│   │   │   ├── dependency/
│   │   │   │   ├── DependencyChecker.scala
│   │   │   │   └── DependencyResolver.scala
│   │   │   ├── reconciliation/
│   │   │   │   ├── ReconciliationEngine.scala
│   │   │   │   ├── SchemaMapper.scala
│   │   │   │   └── ReconciliationAuditor.scala
│   │   │   ├── quality/
│   │   │   │   ├── DataQualityEngine.scala
│   │   │   │   ├── QualityRules.scala
│   │   │   │   └── QualityAuditor.scala
│   │   │   └── audit/
│   │   │       ├── AuditLogger.scala
│   │   │       ├── AuditStorage.scala
│   │   │       └── AuditReporter.scala
│   │   ├── connectors/
│   │   │   ├── BigQueryConnector.scala
│   │   │   ├── PubSubConnector.scala
│   │   │   ├── S3Connector.scala
│   │   │   └── GCSConnector.scala
│   │   └── utils/
│   │       ├── ConfigLoader.scala
│   │       ├── SchemaRegistry.scala
│   │       └── MetricsCollector.scala
├── member-pipeline/
│   ├── build.sbt                 ⇦ Member subproject definition
│   ├── src/main/scala/
│   │   ├── transformations/
│   │   │   ├── MemberRefinedTransform.scala
│   │   │   └── MemberAnalyticsTransform.scala
│   │   └── dataflow/
│   │       └── MemberPipeline.scala
│   └── config/
│       ├── pipeline_config.yaml
│       ├── dependency_config.yaml
│       ├── reconcile_config.yaml
│       └── quality_config.yaml
└── build.sbt                     ⇦ Root build file
```

This structure should provide a solid starting point for building out the remaining functionality.  Feel free to extend it with tests, additional helper classes, CI/CD configuration and deployment scripts as needed.


# Module 01 – Raw Zone (Notification → Fetch → CDC → Insert → Reconcile → DQ → Audit)

This drop adds the first working slice of the pipeline focused on the **Raw** zone.
It follows the steps defined in the design doc and is intended to be iterated module-by-module.

## How to run (locally with DirectRunner)
```bash
sbt "project member-pipeline" run
```

## Deploy (Dataflow)
Package a fat JAR (sbt-assembly recommended) and submit with DataflowRunner.
Set Pub/Sub subscription and BigQuery datasets before running.