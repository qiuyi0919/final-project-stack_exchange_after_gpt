# Stack Overflow After ChatGPT: A Large-Scale Analysis of Code Quality and Community Structure

## Live Resources

- **Interactive dashboard:** [https://cqi-dashboard.onrender.com/frontend/index.html](https://cqi-dashboard.onrender.com/frontend/index.html)
- **Deployment repo:** [YanjingAveryLi/cqi-dashboard](https://github.com/YanjingAveryLi/cqi-dashboard)

---

## Research Problem

Stack Overflow has been the internet's most important public programming knowledge base since its founding in 2008. When ChatGPT was publicly released in November 2022, the platform's monthly question volume dropped sharply — from roughly 87,000 questions per month at its peak to fewer than 50,000 by the end of 2024, a level last seen in the platform's earliest years. Existing research and press coverage have documented this volume collapse, but they have not answered the more consequential structural question: *which questions left, and what happened to the quality of those that remained?*

This matters for a fundamental reason. Stack Overflow is not merely a Q&A website — it is a public knowledge infrastructure that students, professionals, and software teams depend on worldwide. If AI diverts a narrow slice of low-effort questions while preserving the platform's core knowledge-generating function, that is a benign transition. But if the platform is quietly becoming inhospitable to learners and novice users, the long-run consequences for technical education and open knowledge access are significant and deserve systematic study.

We investigate this question through two competing hypotheses. The **quality-improvement hypothesis** holds that ChatGPT has filtered out the large volume of basic, repetitive, easily-answered questions that previously dominated the platform. Under this view, the posts that remain are harder, more original, and more valuable — the apparent contraction actually represents a concentration of the platform's knowledge-generating function. The **composition-shift hypothesis** holds that the apparent quality improvement is an artifact of who left: novice users, who post lower-quality questions on average, have been disproportionately absorbed by AI tools, while expert users remain. The platform is not improving; it is becoming elitist, and the public knowledge commons is quietly shrinking for those who need it most.

Distinguishing between these two hypotheses requires analyzing millions of posts across a fifteen-year time window, segmented by user experience level and topic category, with a causal identification strategy that isolates the ChatGPT cutoff from underlying trends.

---

## Why Scalable Computing Is Essential

The Stack Overflow data archive covers over 60 million posts. Even restricting to Python questions — the language most directly affected by AI coding tools — the dataset spans more than 2.6 million posts across monthly partitions from 2008 to 2024. Computing meaningful code quality metrics requires parsing raw HTML post bodies, running static analysis tools (cyclomatic complexity via `radon`, Halstead volume, AST parseability), and joining post data with user reputation records to assign experience tiers. These operations are computationally prohibitive on a single machine at this scale.

Beyond the raw ETL, the analysis pipeline involves distributed filtering and aggregation across month windows, experience tiers, and tag categories; regression discontinuity estimation across fifteen tier-category combinations; supervised ML model training on nearly 1.25 million labeled examples; and KMeans clustering on the full post corpus. Each of these tasks requires distributed computing infrastructure to run in a reasonable time frame. A pandas-only workflow would be fragile for full-scale runs and unable to support the repeated smoke/full execution cycles needed to validate results before committing to expensive cluster time. We used Apache Spark on AWS EMR for large-scale ETL, feature construction, and ML, and Dask for distributed post-processing and visualization.

---

## Team Contributions

**Qiuyi** was responsible for the data ingestion, ETL pipeline, and CQI feature construction infrastructure. **Yanjing** was responsible for exploratory data analysis, regression discontinuity estimation, machine learning, visualization, and the deployed API and dashboard. Each member's contributions are described in detail below.

---

## Part 1 — Data Ingestion and ETL Pipeline (Qiuyi)

**Code:** [`scripts/data_ingestion_&_ETL/`](scripts/data_ingestion_&_ETL/)  
**Outputs:** [`outputs/data_ingestion_&_ETL/`](outputs/data_ingestion_&_ETL/)

The raw Stack Overflow data is distributed as a 7z-compressed XML archive. Qiuyi designed and implemented a three-phase automated ingestion pipeline to convert this archive into analysis-ready partitioned Parquet.

**Phase 1 — Streaming ingest to S3.** [`stream_posts_to_s3_batches.sh`](scripts/data_ingestion_&_ETL/stream_posts_to_s3_batches.sh) streams the `Posts.xml` file out of the compressed archive using `7za`, pipes each XML row through `awk` to filter post records, and uses GNU `split` to partition the stream into batches of one million rows each. Each batch is immediately gzip-compressed and uploaded to S3, allowing ingestion and downstream processing to proceed concurrently without materializing the full decompressed archive on disk.

**Event-driven batch tracking.** [`lambda_function.py`](scripts/data_ingestion_&_ETL/lambda_function.py) implements an AWS Lambda function triggered by S3 put events. On each batch upload, Lambda writes a manifest entry to DynamoDB (recording batch ID, S3 path, size, upload timestamp, and status) and enqueues an SQS message to notify the Spark ETL job. This event-driven architecture decouples ingestion from processing and provides durable batch-level lineage tracking.

**Phase 2 — Spark ETL and CQI feature construction.** [`build_posts_cqi_batches.py`](scripts/data_ingestion_&_ETL/build_posts_cqi_batches.py) is the core PySpark job, designed to run on AWS EMR. It supports two execution modes: an `s3` mode that processes completed batches sequentially, and an `sqs` mode that consumes SQS messages in real time and can run concurrently with Phase 1 for a fully automated end-to-end chain. For each batch, the job parses raw XML attributes, filters to Python questions, joins with user reputation data to assign experience tiers (novice: <100 reputation; intermediate: 100–1000; expert: >1000), and assigns tag-based topic categories. Critically, it computes a rich set of code quality features using `pandas_udf` for distributed Python execution: code block count and primary code length extracted via `BeautifulSoup`, **cyclomatic complexity** computed by `radon`, **Halstead volume** as a measure of code information content, and **AST parseability** as a binary indicator of syntactic correctness. These features are combined into the Code Quality Index (CQI). The processed output is written as Parquet partitioned by `year_month`, producing the 2.6-million-post dataset that feeds all downstream analysis.

---

## Part 2 — Analysis, Machine Learning, and Deployment (Yanjing)

**Code:** [`scripts/analysis/`](scripts/analysis/), [`scripts/app/`](scripts/app/)

**Dask EDA.** [`eda.ipynb`](scripts/analysis/eda.ipynb) uses Dask to run distributed exploratory analysis over the full Parquet dataset. It computes monthly post volume and mean CQI trends, CQI distributions by experience tier, pre/post ChatGPT comparisons, tag-category CQI trajectories, code-length distributions, and density plots relating CQI to community score and answer count. The notebook supports both local and AWS Dask cluster execution, enabling rapid iteration on smoke samples before full runs. Outputs are in [`outputs/dask_eda/`](outputs/dask_eda/).

**Regression Discontinuity Estimation.** [`cqi_rdd_analysis.ipynb`](scripts/analysis/cqi_rdd_analysis.ipynb) uses PySpark on EMR to aggregate monthly CQI by `year_month`, `experience_tier`, and `tag_category`, then fits RDD models using the December 2022 ChatGPT release as the cutoff. Fifteen tier-category combinations are estimated. Significant positive post-cutoff effects appear for intermediate and expert users in web and data science categories, while the novice tier shows a different pattern — evidence consistent with the composition-shift hypothesis. Results are in [`outputs/emr/rdd_results_A.csv`](outputs/emr/rdd_results_A.csv).

**Supervised ML.** [`ml_pipeline.ipynb`](scripts/analysis/ml_pipeline.ipynb) trains a Spark ML logistic regression pipeline to predict whether a question receives an accepted answer, treating community resolution as the outcome variable. The full run uses 987,499 training rows and 247,327 test rows, achieving AUC = 0.688, accuracy = 0.638, and F1 = 0.634. This robustness check confirms that CQI and metadata features carry meaningful predictive signal beyond the platform's own voting signals.

**Unsupervised KMeans Clustering.** The same notebook also fits a KMeans model (k=4, silhouette = 0.474) on 1,234,826 posts using CQI, reputation, code length, complexity, parseability, and timing features. Cluster profiles reveal distinct post archetypes that differ systematically in their post-ChatGPT share, acceptance rate, and user experience composition.

**Dask ML Visualization.** [`visualization.ipynb`](scripts/analysis/visualization.ipynb) reads the Spark ML outputs and generates report-ready figures: classifier metric comparisons, confusion matrices, coefficient plots, silhouette charts, cluster-profile bar charts, and cluster-center heatmaps. Outputs are in [`outputs/ml/visualizations/`](outputs/ml/visualizations/).

**API and Dashboard.** [`scripts/app/api/main.py`](scripts/app/api/main.py) implements a FastAPI backend serving five endpoints (`/metadata`, `/trend`, `/rdd-results`, `/batch-status`, `/health`) backed by a SQLite database populated from the EMR outputs. The API also serves output figures as static files, allowing the frontend to load visualizations directly over HTTP. [`scripts/app/frontend/`](scripts/app/frontend/) implements an interactive dashboard with API-backed tier and category filters for RDD exploration, a figure explorer with module and type filters, search, featured-only mode, and large-image viewing. The full system is deployed on Render as a single public service accessible at [https://cqi-dashboard.onrender.com](https://cqi-dashboard.onrender.com).
