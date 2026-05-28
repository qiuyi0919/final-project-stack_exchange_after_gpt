const API_BASE = window.location.port === "8010" ? "http://localhost:8001" : "";

const cutoff = new Date("2022-12-01T00:00:00");
const state = {
  monthly: [],
  rdd: null,
  tier: "",
  category: "",
  metric: "mean_CQI",
  window: "all"
};

const metricLabels = {
  mean_CQI: "Mean CQI",
  n_posts: "Post count",
  mean_score: "Mean score",
  mean_answer_count: "Mean answer count"
};

const figureItems = [
  {
    id: "eda-monthly",
    module: "eda",
    type: "trend",
    title: "Monthly Counts and Mean CQI",
    src: "../outputs/dask_eda/aws_full_compact/viz1_monthly_counts_mean_cqi.png",
    description: "Shows posting volume alongside weighted monthly CQI, useful for separating quality movement from activity changes.",
    tags: ["Dask", "CQI", "Volume"],
    featured: true
  },
  {
    id: "eda-tier-dist",
    module: "eda",
    type: "distribution",
    title: "CQI Distribution by Experience Tier",
    src: "../outputs/dask_eda/aws_full_compact/viz2_cqi_distribution_by_tier.png",
    description: "Compares CQI distributions across novice, intermediate, and expert users.",
    tags: ["Dask", "Tier", "Distribution"],
    featured: true
  },
  {
    id: "eda-prepost-tier",
    module: "eda",
    type: "distribution",
    title: "Pre/Post CQI by Tier",
    src: "../outputs/dask_eda/aws_full_compact/viz3_prepost_cqi_by_tier.png",
    description: "Contrasts CQI before and after the ChatGPT cutoff within each experience tier.",
    tags: ["Dask", "Pre/Post", "Tier"],
    featured: false
  },
  {
    id: "eda-category-trends",
    module: "eda",
    type: "trend",
    title: "Tag Category CQI Trends",
    src: "../outputs/dask_eda/aws_full_compact/viz4_tag_category_cqi_trends.png",
    description: "Tracks how CQI moved across data science, ML/AI, web, automation, and core Python categories.",
    tags: ["Dask", "Tags", "Trend"],
    featured: true
  },
  {
    id: "eda-code-length",
    module: "eda",
    type: "distribution",
    title: "Code Length Distribution",
    src: "../outputs/dask_eda/aws_full_compact/viz5_code_length_distribution.png",
    description: "Profiles code length as a core input to CQI and downstream ML features.",
    tags: ["Dask", "Code", "Distribution"],
    featured: false
  },
  {
    id: "eda-score-density",
    module: "eda",
    type: "distribution",
    title: "Score vs CQI Density",
    src: "../outputs/dask_eda/aws_full_compact/viz6_score_vs_cqi_density.png",
    description: "Shows how community voting outcomes relate to measured question quality.",
    tags: ["Dask", "Score", "Density"],
    featured: false
  },
  {
    id: "eda-answer-density",
    module: "eda",
    type: "distribution",
    title: "Answer Count vs CQI Density",
    src: "../outputs/dask_eda/aws_full_compact/viz7_answer_count_vs_cqi_density.png",
    description: "Shows how answer volume relates to CQI across the full analysis window.",
    tags: ["Dask", "Answers", "Density"],
    featured: false
  },
  {
    id: "rdd-overall",
    module: "rdd",
    type: "trend",
    title: "Overall CQI Trend",
    src: "../outputs/emr/overall_cqi_trend.png",
    description: "The main monthly CQI trajectory used to contextualize the cutoff study.",
    tags: ["Spark", "RDD", "CQI"],
    featured: true
  },
  {
    id: "rdd-tier-trend",
    module: "rdd",
    type: "trend",
    title: "CQI Trend by Tier",
    src: "../outputs/emr/cqi_trend_by_tier.png",
    description: "Compares CQI trends across user experience tiers before and after the cutoff.",
    tags: ["Spark", "Tier", "Trend"],
    featured: true
  },
  {
    id: "rdd-smoke",
    module: "rdd",
    type: "trend",
    title: "Smoke-Window CQI Trend",
    src: "../outputs/emr/smoke_cqi_trend.png",
    description: "A small-window validation run around the ChatGPT release date.",
    tags: ["Spark", "Smoke", "Cutoff"],
    featured: false
  },
  {
    id: "rdd-example",
    module: "rdd",
    type: "model",
    title: "RDD Example Group",
    src: "../outputs/emr/rdd_novice_data_science.png",
    description: "A concrete RDD fit for the novice data-science subgroup.",
    tags: ["RDD", "Novice", "Data science"],
    featured: true
  },
  {
    id: "rdd-heatmap",
    module: "rdd",
    type: "model",
    title: "RDD Beta Heatmap",
    src: "../outputs/emr/beta_heatmap.png",
    description: "Summarizes estimated post-cutoff shifts across tier and tag-category groups.",
    tags: ["RDD", "Beta", "Heatmap"],
    featured: true
  },
  {
    id: "rdd-ci",
    module: "rdd",
    type: "model",
    title: "RDD Confidence Intervals",
    src: "../outputs/emr/rdd_confidence_intervals.png",
    description: "Displays uncertainty around the group-level RDD estimates.",
    tags: ["RDD", "Uncertainty", "CI"],
    featured: false
  },
  {
    id: "ml-metrics",
    module: "ml",
    type: "model",
    title: "Supervised Metrics",
    src: "../outputs/ml/visualizations/ml_supervised_metrics_smoke_full.png",
    description: "Compares AUC, accuracy, and F1 for smoke and full accepted-answer classifiers.",
    tags: ["ML", "AUC", "Classifier"],
    featured: true
  },
  {
    id: "ml-confusion",
    module: "ml",
    type: "model",
    title: "Confusion Matrices",
    src: "../outputs/ml/visualizations/ml_supervised_confusion_matrices.png",
    description: "Shows true and predicted accepted-answer outcomes for smoke and full runs.",
    tags: ["ML", "Confusion", "Classifier"],
    featured: true
  },
  {
    id: "ml-coefficients",
    module: "ml",
    type: "model",
    title: "Top Logistic Coefficients",
    src: "../outputs/ml/visualizations/ml_supervised_top_coefficients.png",
    description: "Highlights the strongest positive and negative predictors in the logistic model.",
    tags: ["ML", "Coefficients", "Features"],
    featured: true
  },
  {
    id: "ml-silhouette",
    module: "ml",
    type: "cluster",
    title: "KMeans Silhouette",
    src: "../outputs/ml/visualizations/ml_kmeans_silhouette.png",
    description: "Reports unsupervised cluster separation for smoke and full KMeans runs.",
    tags: ["KMeans", "Silhouette", "Unsupervised"],
    featured: true
  },
  {
    id: "ml-smoke-clusters",
    module: "ml",
    type: "cluster",
    title: "Smoke Cluster Profiles",
    src: "../outputs/ml/visualizations/ml_kmeans_smoke_cluster_profiles.png",
    description: "Profiles cluster size, CQI, accepted-answer rate, and parseability in the smoke window.",
    tags: ["KMeans", "Smoke", "Profiles"],
    featured: false
  },
  {
    id: "ml-full-clusters",
    module: "ml",
    type: "cluster",
    title: "Full Cluster Profiles",
    src: "../outputs/ml/visualizations/ml_kmeans_full_cluster_profiles.png",
    description: "Profiles latent question groups across the full 2019-2025 analysis window.",
    tags: ["KMeans", "Full", "Profiles"],
    featured: true
  },
  {
    id: "ml-smoke-centers",
    module: "ml",
    type: "cluster",
    title: "Smoke Centers Heatmap",
    src: "../outputs/ml/visualizations/ml_kmeans_smoke_centers_heatmap.png",
    description: "Shows standardized feature centers for each smoke-window KMeans cluster.",
    tags: ["KMeans", "Centers", "Heatmap"],
    featured: false
  },
  {
    id: "ml-full-centers",
    module: "ml",
    type: "cluster",
    title: "Full Centers Heatmap",
    src: "../outputs/ml/visualizations/ml_kmeans_full_centers_heatmap.png",
    description: "Shows standardized feature centers for full-window KMeans clusters.",
    tags: ["KMeans", "Centers", "Heatmap"],
    featured: true
  }
];

const figureExplorer = {
  module: "all",
  type: "all",
  search: "",
  featuredOnly: false,
  selectedId: "ml-metrics"
};

const moduleLabels = {
  eda: "Dask EDA",
  rdd: "Spark RDD",
  ml: "ML Pipeline"
};

async function fetchJSON(path) {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    throw new Error(`Failed to load ${path}: ${response.status}`);
  }
  return response.json();
}

async function loadMetadata() {
  return fetchJSON("/metadata");
}

async function loadTrend(tier, category) {
  const params = new URLSearchParams({
    experience_tier: tier,
    tag_category: category
  });
  const payload = await fetchJSON(`/trend?${params}`);
  return payload.rows;
}

async function loadRddResult(tier, category) {
  const params = new URLSearchParams({
    experience_tier: tier,
    tag_category: category
  });
  const payload = await fetchJSON(`/rdd-results?${params}`);
  return payload.result;
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: digits
  }).format(value);
}

function formatBeta(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return value.toFixed(3);
}

function weightedMean(rows, valueKey, weightKey) {
  const totalWeight = rows.reduce((sum, row) => sum + Number(row[weightKey] || 0), 0);
  if (!totalWeight) return null;
  return rows.reduce((sum, row) => sum + Number(row[valueKey] || 0) * Number(row[weightKey] || 0), 0) / totalWeight;
}

function fillSelect(id, values, selected) {
  const select = document.getElementById(id);
  select.innerHTML = values.map((value) => `<option value="${value}">${value.replaceAll("_", " ")}</option>`).join("");
  select.value = selected || values[0];
}

function filteredRows() {
  let rows = state.monthly.filter((row) => row.experience_tier === state.tier && row.tag_category === state.category);
  if (state.window === "cutoff") {
    rows = rows.filter((row) => row.month >= "2021-12-01" && row.month <= "2023-12-01");
  }
  if (state.window === "post") {
    rows = rows.filter((row) => row.month >= "2022-12-01");
  }
  return rows.sort((a, b) => String(a.month).localeCompare(String(b.month)));
}

function currentRdd() {
  return state.rdd;
}

function drawTrend(rows) {
  const container = document.getElementById("trendChart");
  if (!rows.length) {
    container.innerHTML = `<svg viewBox="0 0 900 420"><text class="empty" x="450" y="210" text-anchor="middle">No data for the selected filters</text></svg>`;
    return;
  }

  const width = 900;
  const height = 430;
  const margin = { top: 28, right: 28, bottom: 52, left: 66 };
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  const dates = rows.map((row) => new Date(`${row.month}T00:00:00`));
  const values = rows.map((row) => Number(row[state.metric]));
  const minDate = Math.min(...dates.map((d) => d.getTime()));
  const maxDate = Math.max(...dates.map((d) => d.getTime()));
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const padding = (maxValue - minValue || 1) * 0.08;
  const yMin = Math.max(0, minValue - padding);
  const yMax = maxValue + padding;
  const xScale = (date) => margin.left + ((date.getTime() - minDate) / (maxDate - minDate || 1)) * innerW;
  const yScale = (value) => margin.top + innerH - ((value - yMin) / (yMax - yMin || 1)) * innerH;
  const points = rows.map((row, index) => `${xScale(dates[index]).toFixed(2)},${yScale(Number(row[state.metric])).toFixed(2)}`).join(" ");
  const cutoffX = xScale(cutoff);

  const yTicks = Array.from({ length: 5 }, (_, i) => yMin + ((yMax - yMin) * i) / 4);
  const firstYear = dates[0].getFullYear();
  const lastYear = dates[dates.length - 1].getFullYear();
  const xTicks = [];
  for (let year = firstYear; year <= lastYear; year += Math.max(1, Math.ceil((lastYear - firstYear + 1) / 6))) {
    xTicks.push(new Date(`${year}-01-01T00:00:00`));
  }

  container.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
      <rect width="${width}" height="${height}" fill="#fbfcfb"></rect>
      ${yTicks.map((tick) => `
        <line x1="${margin.left}" x2="${width - margin.right}" y1="${yScale(tick)}" y2="${yScale(tick)}" stroke="#e8edf1"></line>
        <text class="axis" x="${margin.left - 12}" y="${yScale(tick) + 4}" text-anchor="end">${formatNumber(tick, state.metric === "n_posts" ? 0 : 2)}</text>
      `).join("")}
      ${xTicks.map((tick) => `
        <text class="axis" x="${xScale(tick)}" y="${height - 18}" text-anchor="middle">${tick.getFullYear()}</text>
      `).join("")}
      ${cutoffX >= margin.left && cutoffX <= width - margin.right ? `
        <line x1="${cutoffX}" x2="${cutoffX}" y1="${margin.top}" y2="${height - margin.bottom}" stroke="#c14c3d" stroke-width="2" stroke-dasharray="7 7"></line>
        <text class="cutoff-label" x="${cutoffX + 8}" y="${margin.top + 14}">ChatGPT cutoff</text>
      ` : ""}
      <polyline points="${points}" fill="none" stroke="#137b80" stroke-width="3.2" stroke-linejoin="round" stroke-linecap="round"></polyline>
      ${rows.filter((_, index) => index % Math.ceil(rows.length / 36) === 0).map((row) => {
        const i = rows.indexOf(row);
        return `<circle cx="${xScale(dates[i])}" cy="${yScale(Number(row[state.metric]))}" r="3.1" fill="#137b80"></circle>`;
      }).join("")}
    </svg>
  `;
}

function drawRdd(row) {
  const container = document.getElementById("rddChart");
  if (!row) {
    container.innerHTML = `<svg viewBox="0 0 500 220"><text class="empty" x="250" y="110" text-anchor="middle">No RDD result</text></svg>`;
    return;
  }

  const width = 500;
  const height = 220;
  const margin = { top: 44, right: 42, bottom: 44, left: 42 };
  const min = Math.min(Number(row.ci_low), 0) - 0.025;
  const max = Math.max(Number(row.ci_high), 0) + 0.025;
  const x = (value) => margin.left + ((value - min) / (max - min || 1)) * (width - margin.left - margin.right);
  const y = height / 2;

  container.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
      <rect width="${width}" height="${height}" fill="#fbfcfb"></rect>
      <line x1="${margin.left}" x2="${width - margin.right}" y1="${y}" y2="${y}" stroke="#d9e0e6" stroke-width="2"></line>
      <line x1="${x(0)}" x2="${x(0)}" y1="${margin.top}" y2="${height - margin.bottom}" stroke="#c14c3d" stroke-width="2" stroke-dasharray="6 6"></line>
      <line x1="${x(Number(row.ci_low))}" x2="${x(Number(row.ci_high))}" y1="${y}" y2="${y}" stroke="#2f6fb0" stroke-width="8" stroke-linecap="round"></line>
      <circle cx="${x(Number(row.beta_post))}" cy="${y}" r="10" fill="#137b80"></circle>
      <text class="axis" x="${x(Number(row.ci_low))}" y="${height - 20}" text-anchor="middle">${formatBeta(Number(row.ci_low))}</text>
      <text class="axis" x="${x(0)}" y="${height - 20}" text-anchor="middle">0</text>
      <text class="axis" x="${x(Number(row.ci_high))}" y="${height - 20}" text-anchor="middle">${formatBeta(Number(row.ci_high))}</text>
      <text class="cutoff-label" x="${x(Number(row.beta_post))}" y="${margin.top - 12}" text-anchor="middle">beta ${formatBeta(Number(row.beta_post))}</text>
    </svg>
  `;
}

async function refreshData() {
  document.getElementById("dataStatus").textContent = "Fetching API data";
  const [monthly, rdd] = await Promise.all([
    loadTrend(state.tier, state.category),
    loadRddResult(state.tier, state.category)
  ]);
  state.monthly = monthly;
  state.rdd = rdd;
  document.getElementById("dataStatus").textContent = "API connected";
  document.querySelector(".dot").style.background = "#4d7f45";
}

function update() {
  const rows = filteredRows();
  const rdd = currentRdd();
  const posts = rows.reduce((sum, row) => sum + Number(row.n_posts || 0), 0);
  const meanCQI = weightedMean(rows, "mean_CQI", "n_posts");

  document.getElementById("monthsValue").textContent = formatNumber(rows.length, 0);
  document.getElementById("postsValue").textContent = formatNumber(posts, 0);
  document.getElementById("cqiValue").textContent = formatNumber(meanCQI, 3);
  document.getElementById("betaValue").textContent = rdd ? formatBeta(Number(rdd.beta_post)) : "-";
  document.getElementById("pValue").textContent = rdd ? formatNumber(Number(rdd.pvalue), 3) : "-";
  document.getElementById("ciValue").textContent = rdd ? `[${formatBeta(Number(rdd.ci_low))}, ${formatBeta(Number(rdd.ci_high))}]` : "-";
  document.getElementById("rddPosts").textContent = rdd ? formatNumber(Number(rdd.n_posts), 0) : "-";
  document.getElementById("trendSubtitle").textContent = `${metricLabels[state.metric]} for ${state.tier.replaceAll("_", " ")} / ${state.category.replaceAll("_", " ")}.`;

  drawTrend(rows);
  drawRdd(rdd);
}

function bindControls() {
  document.getElementById("tierSelect").addEventListener("change", async (event) => {
    state.tier = event.target.value;
    await refreshData();
    update();
  });
  document.getElementById("categorySelect").addEventListener("change", async (event) => {
    state.category = event.target.value;
    await refreshData();
    update();
  });
  document.getElementById("metricSelect").addEventListener("change", (event) => {
    state.metric = event.target.value;
    update();
  });
  document.getElementById("windowSelect").addEventListener("change", (event) => {
    state.window = event.target.value;
    update();
  });
}

function selectedFigure() {
  return figureItems.find((item) => item.id === figureExplorer.selectedId) || figureItems[0];
}

function filteredFigures() {
  const query = figureExplorer.search.trim().toLowerCase();
  return figureItems.filter((item) => {
    const matchesModule = figureExplorer.module === "all" || item.module === figureExplorer.module;
    const matchesType = figureExplorer.type === "all" || item.type === figureExplorer.type;
    const matchesFeatured = !figureExplorer.featuredOnly || item.featured;
    const haystack = `${item.title} ${item.description} ${item.tags.join(" ")} ${moduleLabels[item.module]}`.toLowerCase();
    const matchesSearch = !query || haystack.includes(query);
    return matchesModule && matchesType && matchesFeatured && matchesSearch;
  });
}

function tagMarkup(tags) {
  return tags.map((tag) => `<span>${tag}</span>`).join("");
}

function renderViewer(item) {
  document.getElementById("viewerKicker").textContent = `${moduleLabels[item.module]} / ${item.type}`;
  document.getElementById("viewerTitle").textContent = item.title;
  document.getElementById("viewerImage").src = item.src;
  document.getElementById("viewerImage").alt = item.title;
  document.getElementById("viewerDescription").textContent = item.description;
  document.getElementById("viewerTags").innerHTML = tagMarkup(item.tags);
}

function figureCard(item) {
  const active = item.id === figureExplorer.selectedId ? " active" : "";
  const featured = item.featured ? `<span class="tag">Featured</span>` : "";
  return `
    <button class="figure-card${active}" type="button" data-figure-id="${item.id}">
      <img src="${item.src}" alt="${item.title}">
      <span class="figure-card-body">
        <span class="figure-card-title">${item.title}</span>
        <span class="figure-card-desc">${item.description}</span>
        <span class="card-tags">
          <span class="tag">${moduleLabels[item.module]}</span>
          <span class="tag">${item.type}</span>
          ${featured}
        </span>
      </span>
    </button>
  `;
}

function renderFigures() {
  const figures = filteredFigures();
  const grid = document.getElementById("figureGrid");
  const selectedStillVisible = figures.some((item) => item.id === figureExplorer.selectedId);
  if (!selectedStillVisible && figures.length) {
    figureExplorer.selectedId = figures[0].id;
  }

  document.getElementById("figureCount").textContent = String(figures.length);
  grid.innerHTML = figures.length
    ? figures.map(figureCard).join("")
    : `<div class="empty-card">No figures match the current filters.</div>`;

  const current = selectedFigure();
  renderViewer(current);

  document.querySelectorAll("[data-figure-id]").forEach((button) => {
    button.addEventListener("click", () => {
      figureExplorer.selectedId = button.dataset.figureId;
      renderFigures();
    });
  });
}

function openFigureDialog() {
  const item = selectedFigure();
  const dialog = document.getElementById("figureDialog");
  document.getElementById("dialogImage").src = item.src;
  document.getElementById("dialogImage").alt = item.title;
  document.getElementById("dialogKicker").textContent = `${moduleLabels[item.module]} / ${item.type}`;
  document.getElementById("dialogTitle").textContent = item.title;
  document.getElementById("dialogDescription").textContent = item.description;
  if (typeof dialog.showModal === "function") {
    dialog.showModal();
  }
}

function bindFigureExplorer() {
  const moduleSelect = document.getElementById("figureModuleSelect");
  if (!moduleSelect) return;

  moduleSelect.addEventListener("change", (event) => {
    figureExplorer.module = event.target.value;
    renderFigures();
  });
  document.getElementById("figureTypeSelect").addEventListener("change", (event) => {
    figureExplorer.type = event.target.value;
    renderFigures();
  });
  document.getElementById("figureSearch").addEventListener("input", (event) => {
    figureExplorer.search = event.target.value;
    renderFigures();
  });
  document.getElementById("featuredOnly").addEventListener("change", (event) => {
    figureExplorer.featuredOnly = event.target.checked;
    renderFigures();
  });

  document.getElementById("openFigureButton").addEventListener("click", openFigureDialog);
  document.getElementById("viewerImageButton").addEventListener("click", openFigureDialog);
  document.getElementById("closeFigureDialog").addEventListener("click", () => {
    document.getElementById("figureDialog").close();
  });

  const spotlight = document.getElementById("mlSpotlightGrid");
  if (spotlight) {
    spotlight.innerHTML = figureItems
      .filter((item) => item.module === "ml" && item.featured)
      .slice(0, 6)
      .map(figureCard)
      .join("");
    spotlight.querySelectorAll("[data-figure-id]").forEach((button) => {
      button.addEventListener("click", () => {
        figureExplorer.selectedId = button.dataset.figureId;
        renderFigures();
        document.getElementById("eda").scrollIntoView({ behavior: "smooth" });
      });
    });
  }

  renderFigures();
}

async function init() {
  try {
    const metadata = await loadMetadata();
    const tiers = metadata.experience_tiers;
    const categories = metadata.tag_categories;
    state.tier = tiers.includes("novice") ? "novice" : tiers[0];
    state.category = categories.includes("data_science") ? "data_science" : categories[0];

    fillSelect("tierSelect", tiers, state.tier);
    fillSelect("categorySelect", categories, state.category);
    bindControls();

    await refreshData();
    update();
  } catch (error) {
    document.getElementById("dataStatus").textContent = "API unavailable";
    document.querySelector(".dot").style.background = "#c14c3d";
    console.error(error);
  }
}

bindFigureExplorer();
init();
