# Databricks notebook source
# DBTITLE 1,ML-PROPENSITY: LTV Prediction & Propensity-to-Buy Scoring
# MAGIC %md
# MAGIC # ML-PROPENSITY: LTV Prediction & Propensity-to-Buy Scoring
# MAGIC
# MAGIC **Task:** `ML-PROPENSITY` (remaining deliverables) — 12-month forward LTV + propensity-to-buy  
# MAGIC **Owner:** `@ml-engineer` | **Project:** CustomerLake Readiness  
# MAGIC **Catalog:** `cdm_tmforum` | **Tag:** `customerlake_project: customerlake`
# MAGIC
# MAGIC ## CustomerLake Differentiation
# MAGIC Both models demonstrate what **only CustomerLake can do**:
# MAGIC
# MAGIC | Signal | Source Systems | Legacy CDP Can? |
# MAGIC |--------|---------------|----------------|
# MAGIC | Identity resolution confidence | Identity layer (35.7K entities, 97.8K match decisions) | No — CDPs trust source keys blindly |
# MAGIC | Cross-channel digital journey | Web + mobile + self-service portal (75K events) | Partial — usually channel-siloed |
# MAGIC | Service portfolio context | Installed base (100K services) + billing (310K transactions) | No — billing is separate from engagement |
# MAGIC | ML churn integration | Churn model (AUC 0.976) feeds propensity | No — churn and marketing are siloed |
# MAGIC | Activation response history | 30K activation pushes with delivery + conversion tracking | Partial — attribution usually disconnected |
# MAGIC
# MAGIC ## Models Built
# MAGIC 1. **LTV Prediction** — LightGBM regressor predicting 12-month forward customer value from cross-source features
# MAGIC 2. **Propensity-to-Buy** — LightGBM classifier predicting likelihood of conversion/purchase from digital journey + activation signals

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install lightgbm --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration & Imports
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import (
    precision_score, recall_score, f1_score, roc_auc_score,
    average_precision_score, mean_absolute_error, mean_squared_error, r2_score,
    classification_report
)
from lightgbm import LGBMClassifier, LGBMRegressor
import json, warnings
warnings.filterwarnings('ignore')

CATALOG = "cdm_tmforum"
LTV_MODEL_NAME = f"{CATALOG}.gold.customerlake_ltv_model"
PROPENSITY_MODEL_NAME = f"{CATALOG}.gold.customerlake_propensity_model"
EXPERIMENT_NAME = "/Users/stephen.hage@databricks.com/customerlake-env-setup/ltv_propensity_models"
mlflow.set_experiment(EXPERIMENT_NAME)
mlflow.autolog(disable=True)

print(f"LTV Model: {LTV_MODEL_NAME}")
print(f"Propensity Model: {PROPENSITY_MODEL_NAME}")
print(f"Experiment: {EXPERIMENT_NAME}")