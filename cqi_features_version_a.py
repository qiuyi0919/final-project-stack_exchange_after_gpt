"""
cqi_features_version_a.py
=========================
Version A CQI (Python-only) code feature extraction module
Research project: MACS 30123 — The Impact of AI on Online Knowledge Communities

Exposes four core functions (each directly registerable as a PySpark pandas_udf):
  1. extract_code_blocks(html)           — Extract all <code> block texts via BeautifulSoup
  2. compute_cyclomatic_complexity(code) — Mean cyclomatic complexity via Radon cc_visit()
  3. compute_halstead_volume(code)       — Halstead volume V via Radon h_visit()
  4. compute_code_parseability(code)     — Syntax parseability via ast.parse() (1.0 / 0.0)

Single-row entry point:
  compute_python_code_features(html) -> dict  (all four dimensions + auxiliary fields)

DataFrame batch entry point:
  apply_features_to_df(df, body_col="Body") -> df  (appends feature columns)

Literature alignment:
  Radon cyclomatic complexity + Halstead volume align directly with
  Mondal & Roy (2025), who apply the same metrics to Stack Overflow
  answer code snippets using Radon.

Dependencies (install on EMR before running):
  pip install beautifulsoup4 lxml radon
"""

import ast
import warnings
from typing import Optional

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from radon.complexity import cc_visit
from radon.metrics import h_visit

warnings.filterwarnings("ignore")


# ──────────────────────────────────────────────────────────────
# 1. Code block extraction
# ──────────────────────────────────────────────────────────────

def extract_code_blocks(html: str) -> list[str]:
    """
    Extract the plain-text content of all <code> tags from a
    Stack Overflow HTML Body field.

    Returns
    -------
    list[str]
        One string per <code> block; returns an empty list if no
        code blocks are found or if parsing fails.
    """
    if not isinstance(html, str) or len(html.strip()) < 5:
        return []
    try:
        soup = BeautifulSoup(html, "lxml")
        return [tag.get_text() for tag in soup.find_all("code")]
    except Exception:
        return []


def extract_primary_code_block(html: str) -> Optional[str]:
    """
    Return the longest <code> block in the HTML Body (typically the
    main code snippet). Using the longest block rather than the first
    reduces noise from short import lines.

    Returns None if no code blocks are present.
    """
    blocks = extract_code_blocks(html)
    if not blocks:
        return None
    return max(blocks, key=len)


# ──────────────────────────────────────────────────────────────
# 2. Cyclomatic complexity
# ──────────────────────────────────────────────────────────────

def compute_cyclomatic_complexity(code: str) -> Optional[float]:
    """
    Compute the mean cyclomatic complexity of a Python code snippet
    using Radon cc_visit().

    - If the code defines functions or classes, returns the mean
      complexity across all detected blocks.
    - If the code is valid but contains no function definitions
      (flat top-level code, common in SO answers), returns 1.0
      (the McCabe baseline for a single linear path).
    - Returns None if Radon cannot parse the code (syntax errors, etc.).

    References
    ----------
    Mondal & Roy (2025): cyclomatic_complexity via Radon cc_visit()
    McCabe (1976): A Complexity Measure, IEEE TSE
    """
    if not isinstance(code, str) or not code.strip():
        return None
    try:
        blocks = cc_visit(code)
        if not blocks:
            return 1.0
        return float(np.mean([b.complexity for b in blocks]))
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────
# 3. Halstead volume
# ──────────────────────────────────────────────────────────────

def compute_halstead_volume(code: str) -> Optional[float]:
    """
    Compute the Halstead Volume of a Python code snippet using
    Radon h_visit().

    Formula: V = N * log2(eta)
      N   = N1 + N2    (total operator + operand occurrences)
      eta = h1 + h2    (unique operators + unique operands)

    Returns
    -------
    float or None
        Total Halstead volume; returns None for empty code or
        if parsing fails.

    References
    ----------
    Mondal & Roy (2025): Halstead volume via Radon h_visit()
    Halstead (1977): Elements of Software Science
    """
    if not isinstance(code, str) or not code.strip():
        return None
    try:
        result = h_visit(code)
        volume = result.total.volume
        return float(volume) if volume is not None and not np.isnan(volume) else None
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────
# 4. Code parseability
# ──────────────────────────────────────────────────────────────

def compute_code_parseability(code: str) -> float:
    """
    Check whether a code snippet is syntactically valid Python using
    the built-in ast.parse().

    Returns
    -------
    float
        1.0 — successfully parsed (valid syntax)
        0.0 — SyntaxError or any other parse exception

    Notes
    -----
    Approximately 40% of SO code snippets are expected to fail
    ast.parse() (consistent with Mondal & Roy 2025, who report a
    59.2% parseability rate; the gap reflects differences in post
    type and filtering). Parse-failure samples are retained in the
    dataset; CQI is computed from the remaining available dimensions.
    """
    if not isinstance(code, str) or not code.strip():
        return 0.0
    try:
        ast.parse(code)
        return 1.0
    except SyntaxError:
        return 0.0
    except Exception:
        return 0.0


# ──────────────────────────────────────────────────────────────
# Integration: single-row feature dict
# ──────────────────────────────────────────────────────────────

def compute_python_code_features(html: str) -> dict:
    """
    Extract all four code quality dimensions (Version A CQI) from
    the HTML Body of a single Stack Overflow post.

    Parameters
    ----------
    html : str
        Raw HTML Body field of a Stack Overflow post.

    Returns
    -------
    dict with the following keys:
      code_block_count      int    — number of <code> blocks
      primary_code_len      int    — character length of the longest code block (0 if none)
      cyclomatic_complexity float  — mean cyclomatic complexity (None if parse failed)
      halstead_volume       float  — Halstead volume V (None if parse failed)
      is_parseable          float  — 1.0 if ast.parse() succeeded, else 0.0
      has_code              int    — 1 if any code block exists, else 0
    """
    blocks  = extract_code_blocks(html)
    primary = max(blocks, key=len) if blocks else None

    cc        = compute_cyclomatic_complexity(primary) if primary else None
    hv        = compute_halstead_volume(primary)        if primary else None
    parseable = compute_code_parseability(primary)      if primary else 0.0

    return {
        "code_block_count":      len(blocks),
        "primary_code_len":      len(primary) if primary else 0,
        "cyclomatic_complexity": cc,
        "halstead_volume":       hv,
        "is_parseable":          parseable,
        "has_code":              int(len(blocks) > 0),
    }


# ──────────────────────────────────────────────────────────────
# DataFrame batch processing
# ──────────────────────────────────────────────────────────────

def apply_features_to_df(df: pd.DataFrame, body_col: str = "Body") -> pd.DataFrame:
    """
    Apply compute_python_code_features to every row of a DataFrame
    and append the resulting columns (does not modify the original).

    Only rows tagged with <python> are processed (Version A scope).
    Non-Python rows are retained in the result with NaN for all
    code feature columns.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing at least a Body column and
        optionally a Tags column.
    body_col : str
        Name of the column containing the HTML Body text.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with six additional feature columns appended.
    """
    if body_col not in df.columns:
        raise ValueError(
            f"Column '{body_col}' not found. "
            f"Available columns: {list(df.columns)}"
        )

    feature_cols = [
        "code_block_count", "primary_code_len",
        "cyclomatic_complexity", "halstead_volume",
        "is_parseable", "has_code",
    ]

    is_python = df.get("Tags", pd.Series(["<python>"] * len(df))).str.contains(
        "<python>", na=False
    )
    n_python = is_python.sum()
    print(f"  -> {len(df)} total rows, {n_python} with Python tag. Running feature extraction ...")

    feat_records = df.loc[is_python, body_col].apply(compute_python_code_features)
    feat_df = pd.DataFrame(feat_records.tolist(), index=df.index[is_python])

    result = df.copy()
    for col in feature_cols:
        result[col] = np.nan
    result.loc[is_python, feature_cols] = feat_df[feature_cols].values

    print(f"  -> Done. Appended columns: {feature_cols}")
    return result
