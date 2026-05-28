# Stack Overflow Python CQI Analysis: My Scalable Computing Contributions

## Live Links

- **Interactive dashboard:** [https://cqi-dashboard.onrender.com/frontend/index.html](https://cqi-dashboard.onrender.com/frontend/index.html)
- **Interactive RDD filters:** [dashboard#interactive](https://cqi-dashboard.onrender.com/frontend/index.html#interactive)
- **Result explorer:** [dashboard#eda](https://cqi-dashboard.onrender.com/frontend/index.html#eda)
- **ML spotlight:** [dashboard#ml](https://cqi-dashboard.onrender.com/frontend/index.html#ml)
- **API metadata endpoint:** [https://cqi-dashboard.onrender.com/metadata](https://cqi-dashboard.onrender.com/metadata)
- **Deployment repo:** [YanjingAveryLi/cqi-dashboard](https://github.com/YanjingAveryLi/cqi-dashboard)

## Research Problem

This project studies whether the public release of ChatGPT changed the quality and resolution patterns of Python questions on Stack Overflow. The social science question is how generative AI affects online knowledge communities: if programmers use AI for debugging and explanation, public help-seeking behavior may change. Some questions may become clearer because users arrive with better preliminary code, while others may become harder to answer because they include AI-generated fragments or weaker problem descriptions. This matters because Stack Overflow is a public knowledge infrastructure used by students, professionals, and software teams.

My contribution is the scalable analysis, machine learning, visualization, API, and deployment layer. A teammate handled other backend/ingestion pieces, so the sections below describe my implemented components and link directly to code and outputs.

## Why Scalable Computing Was Needed

The project uses monthly parquet partitions covering millions of Stack Overflow Python posts. Even though the final dashboard serves compact summaries, producing them requires distributed filtering, aggregation, feature construction, RDD estimation, ML model fitting, and visualization. A pandas-only workflow would be fragile for repeated smoke/full runs across month windows, experience tiers, tag categories, and feature sets. I used Spark on AWS EMR for large-scale CQI/RDD/ML computation and Dask for scalable visualization and post-processing.

## 1. Dask EDA Module

- **Dashboard section:** [Result explorer, filter by Dask EDA](https://cqi-dashboard.onrender.com/frontend/index.html#eda)
- **Main code:** [scripts/eda/eda.ipynb](scripts/eda/eda.ipynb)
- **AWS full EDA output folder:** [outputs/dask_eda/aws_full_compact](outputs/dask_eda/aws_full_compact)
- **AWS smoke EDA output folder:** [outputs/dask_eda/aws_smoke_compact](outputs/dask_eda/aws_smoke_compact)
- **Summary data:** [summary_statistics.csv](outputs/dask_eda/aws_full_compact/summary_statistics.csv), [monthly_counts_mean_cqi.csv](outputs/dask_eda/aws_full_compact/monthly_counts_mean_cqi.csv), [tag_category_monthly_cqi.csv](outputs/dask_eda/aws_full_compact/tag_category_monthly_cqi.csv), [cqi_hist_by_tier.csv](outputs/dask_eda/aws_full_compact/cqi_hist_by_tier.csv), [code_length_hist.csv](outputs/dask_eda/aws_full_compact/code_length_hist.csv), [correlation_matrix.csv](outputs/dask_eda/aws_full_compact/correlation_matrix.csv)
- **EDA figures:** [monthly counts and mean CQI](outputs/dask_eda/aws_full_compact/viz1_monthly_counts_mean_cqi.png), [CQI distribution by tier](outputs/dask_eda/aws_full_compact/viz2_cqi_distribution_by_tier.png), [pre/post CQI by tier](outputs/dask_eda/aws_full_compact/viz3_prepost_cqi_by_tier.png), [tag category trends](outputs/dask_eda/aws_full_compact/viz4_tag_category_cqi_trends.png), [code length distribution](outputs/dask_eda/aws_full_compact/viz5_code_length_distribution.png), [score vs CQI density](outputs/dask_eda/aws_full_compact/viz6_score_vs_cqi_density.png), [answer count vs CQI density](outputs/dask_eda/aws_full_compact/viz7_answer_count_vs_cqi_density.png)

I built the Dask EDA workflow to explore the structure of the CQI dataset before and alongside modeling. This includes volume trends, CQI distributions by experience tier, pre/post ChatGPT comparisons, tag-category CQI trajectories, code-length distributions, and density plots relating CQI to score and answer count. The notebook supports local Dask and AWS Dask execution, so I could validate on smaller samples and then produce full-window compact outputs for the dashboard.

## 2. Spark ML Supervised Robustness Module

- **Dashboard section:** [ML spotlight](https://cqi-dashboard.onrender.com/frontend/index.html#ml)
- **Main code:** [scripts/ml/ml_pipeline.ipynb](scripts/ml/ml_pipeline.ipynb)
- **Metrics:** [outputs/ml/flat/full_metrics.csv](outputs/ml/flat/full_metrics.csv), [outputs/ml/flat/smoke_metrics.csv](outputs/ml/flat/smoke_metrics.csv)
- **Confusion matrix:** [outputs/ml/flat/full_confusion_matrix.csv](outputs/ml/flat/full_confusion_matrix.csv)
- **Coefficients:** [outputs/ml/flat/full_coefficients.csv](outputs/ml/flat/full_coefficients.csv)
- **Visualizations:** [ml_supervised_metrics_smoke_full.png](outputs/ml/visualizations/ml_supervised_metrics_smoke_full.png), [ml_supervised_confusion_matrices.png](outputs/ml/visualizations/ml_supervised_confusion_matrices.png), [ml_supervised_top_coefficients.png](outputs/ml/visualizations/ml_supervised_top_coefficients.png)

I implemented a supervised Spark ML pipeline predicting whether a Stack Overflow question receives an accepted answer. This treats accepted-answer status as a community-resolution outcome. The full run used **987,499 training rows** and **247,327 test rows**, with **AUC = 0.688**, **accuracy = 0.638**, and **F1 = 0.634**. The model uses CQI and metadata features rather than predicting CQI from its own component variables.

## 3. Spark ML Unsupervised KMeans Module

- **Dashboard section:** [Result explorer, filter by ML Pipeline + Clustering](https://cqi-dashboard.onrender.com/frontend/index.html#eda)
- **Main code:** [scripts/ml/ml_pipeline.ipynb](scripts/ml/ml_pipeline.ipynb)
- **Metrics:** [outputs/ml/flat/full_kmeans_metrics.csv](outputs/ml/flat/full_kmeans_metrics.csv), [outputs/ml/flat/smoke_kmeans_metrics.csv](outputs/ml/flat/smoke_kmeans_metrics.csv)
- **Cluster summaries:** [outputs/ml/flat/full_kmeans_cluster_summary.csv](outputs/ml/flat/full_kmeans_cluster_summary.csv), [outputs/ml/flat/smoke_kmeans_cluster_summary.csv](outputs/ml/flat/smoke_kmeans_cluster_summary.csv)
- **Cluster centers:** [outputs/ml/flat/full_kmeans_cluster_centers.csv](outputs/ml/flat/full_kmeans_cluster_centers.csv)
- **Visualizations:** [ml_kmeans_silhouette.png](outputs/ml/visualizations/ml_kmeans_silhouette.png), [ml_kmeans_full_cluster_profiles.png](outputs/ml/visualizations/ml_kmeans_full_cluster_profiles.png), [ml_kmeans_full_centers_heatmap.png](outputs/ml/visualizations/ml_kmeans_full_centers_heatmap.png)

I added an unsupervised KMeans robustness task using CQI, reputation, code length, complexity, parseability, and timing features. The label is not used during clustering. The full KMeans run used **1,234,826 rows**, `k=4`, and produced **silhouette = 0.474**. I then profiled clusters by accepted-answer rate, post-ChatGPT share, mean CQI, code presence, and parseability.

## 4. Spark EMR RDD and CQI Output Module

- **Dashboard section:** [Interactive RDD filters](https://cqi-dashboard.onrender.com/frontend/index.html#interactive)
- **Main code:** [scripts/analysis/cqi_rdd_analysis.ipynb](scripts/analysis/cqi_rdd_analysis.ipynb)
- **Serving data:** [outputs/emr/monthly_A_flat.csv](outputs/emr/monthly_A_flat.csv), [outputs/emr/rdd_results_A.csv](outputs/emr/rdd_results_A.csv)
- **Figures:** [overall_cqi_trend.png](outputs/emr/overall_cqi_trend.png), [cqi_trend_by_tier.png](outputs/emr/cqi_trend_by_tier.png), [beta_heatmap.png](outputs/emr/beta_heatmap.png), [rdd_confidence_intervals.png](outputs/emr/rdd_confidence_intervals.png)
- **Report figure folder:** [outputs/cqi_reg](outputs/cqi_reg)

I used PySpark on AWS EMR to aggregate monthly CQI by `year_month`, `experience_tier`, and `tag_category`, then estimate RDD models around the December 2022 ChatGPT cutoff. The final RDD table contains 15 tier-category estimates. Significant positive post-cutoff beta estimates appear for groups such as intermediate web, expert web, expert data science, intermediate data science, and novice automation.

## 5. Dask ML Visualization Module

- **Dashboard section:** [Result explorer, filter by ML Pipeline](https://cqi-dashboard.onrender.com/frontend/index.html#eda)
- **ML visualization code:** [scripts/ml/visualization.ipynb](scripts/ml/visualization.ipynb)
- **Input ML tables:** [outputs/ml/flat](outputs/ml/flat)
- **Generated ML figures:** [outputs/ml/visualizations](outputs/ml/visualizations)

I also built a separate Dask visualization notebook for the Spark ML outputs. It reads the supervised and KMeans result tables, computes compact pandas summaries through Dask, and generates report-ready classifier and clustering figures. This is the module that produced the ML metric bar charts, confusion matrices, coefficient plots, silhouette plot, cluster-profile charts, and cluster-center heatmaps used in the dashboard.

## 6. API, Frontend, and Deployment Module

- **Live dashboard:** [https://cqi-dashboard.onrender.com/frontend/index.html](https://cqi-dashboard.onrender.com/frontend/index.html)
- **API code:** [scripts/api/main.py](scripts/api/main.py), [scripts/api/db.py](scripts/api/db.py)
- **Frontend code:** [scripts/frontend/index.html](scripts/frontend/index.html), [scripts/frontend/app.js](scripts/frontend/app.js), [scripts/frontend/styles.css](scripts/frontend/styles.css)
- **Serving database:** [scripts/data/serving.db](scripts/data/serving.db)
- **Deployment config:** [scripts/render.yaml](scripts/render.yaml)
- **API endpoints:** [metadata](https://cqi-dashboard.onrender.com/metadata), [batch status](https://cqi-dashboard.onrender.com/batch-status)
- **Static output routes:** `/outputs`, `/ml_outputs`, `/dask_eda_outputs`, `/outputs_from_emr` — the API mounts the corresponding `outputs/` subdirectories as static file servers so the frontend can load figures directly over HTTP.
- **Environment variable:** `CQI_DB_PATH` overrides the default SQLite path (`data/serving.db`) at runtime; useful for local development or custom Render deployments.

I built the deployed FastAPI dashboard. The API serves metadata, monthly trends, RDD results, and static outputs. The frontend includes API-backed filters for RDD exploration and an interactive figure explorer with module filters, type filters, search, featured-only mode, selected-figure preview, and large-image viewing. I deployed the system on Render as a single service so graders can use one public URL.
