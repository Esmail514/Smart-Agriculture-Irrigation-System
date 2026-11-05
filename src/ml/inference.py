"""Shared inference contracts. Labels describe current dryness, not a forecast."""
import json
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd


def latest_zone_rows(df):
    if df.empty:
        return df.copy()
    result = df.copy()
    result["last_update"] = pd.to_datetime(result["last_update"], utc=True, errors="raise")
    if result["last_update"].isna().any():
        raise ValueError("Zone observations require last_update")
    keys = [c for c in ["farm_id", "zone_id"] if c in result.columns]
    # Late sensor updates can change a zone summary without advancing its max
    # event time. The Gold snapshot sequence determines which state is newer.
    order = ["last_update"] + (["snapshot_id"] if "snapshot_id" in result else [])
    return result.sort_values(order, kind="stable").drop_duplicates(keys, keep="last")


def chronological_split(df, test_size=0.2):
    """Keep all rows from the same observation instant on one side of the split."""
    times = pd.to_datetime(df["last_update"], utc=True, errors="raise")
    if times.isna().any():
        raise ValueError("Training observations require last_update")
    unique = times.sort_values().unique()
    if len(unique) < 2:
        return df.iloc[:0], df.copy()
    cutoff = unique[max(1, min(len(unique) - 1, int(len(unique) * (1 - test_size))))]
    return df.loc[times < cutoff], df.loc[times >= cutoff]


def build_predictions(df, probabilities, model_version, features):
    probabilities = np.asarray(probabilities, dtype=float)
    if (probabilities.ndim != 1 or len(df) != len(probabilities)
            or not np.isfinite(probabilities).all()
            or ((probabilities < 0) | (probabilities > 1)).any()):
        raise ValueError("Invalid prediction probabilities")
    now = datetime.now(timezone.utc)
    predictions = []
    # iterrows may coerce an integer identity to float when the row also has
    # numeric features, silently rounding valid 64-bit IDs above 2**53.
    for position, row in enumerate(df.to_dict(orient="records")):
        prob = float(probabilities[position])
        snapshot = {f: float(row[f]) for f in features if f in row and pd.notna(row[f])}
        # Operational readers can invalidate a stale decision after a zone's
        # crop or rule changes, even when those columns are not model features.
        for name in ["crop_id", "moisture_min", "moisture_max"]:
            if name in row and pd.notna(row[name]):
                if name == "crop_id":
                    value = row[name]
                    if isinstance(value, bool) or not isinstance(value, (int, np.integer)) or not 0 < value <= 2**63 - 1:
                        raise ValueError("Invalid prediction rule evidence: crop_id")
                    snapshot[name] = int(value)
                    continue
                value = float(row[name])
                if not np.isfinite(value):
                    raise ValueError(f"Invalid prediction rule evidence: {name}")
                snapshot[name] = value

        # Agronomic water depth & valve run duration calculation
        raw_deficit = row.get("moisture_deficit")
        deficit = float(raw_deficit) if (raw_deficit is not None and pd.notna(raw_deficit)) else 0.0
        if "nir_mm" in row and pd.notna(row["nir_mm"]):
            nir_mm = max(0.0, round(float(row["nir_mm"]), 2))
        else:
            nir_mm = max(0.0, round(deficit * 0.40, 2)) if deficit > 0 else 0.0
        # Recommended duration: e.g. standard drip system supplies ~3mm/hour (1 mm per 1200s), capped at 3600s
        rec_duration_sec = int(min(3600, max(300, round(nir_mm * 1200)))) if (prob >= 0.5 and nir_mm > 0) else 0

        # Local decision drivers based on feature deviations
        top_drivers = []
        if deficit > 0:
            top_drivers.append({"factor": "moisture_deficit", "impact": "increases_need", "value": round(deficit, 2)})
        if pd.notna(row.get("vpd_kpa")) and float(row["vpd_kpa"]) > 1.5:
            top_drivers.append({"factor": "high_vpd", "impact": "high_evaporative_demand", "value": round(float(row["vpd_kpa"]), 2)})
        if pd.notna(row.get("total_rain")) and float(row["total_rain"]) > 2.0:
            top_drivers.append({"factor": "recent_rain", "impact": "reduces_need", "value": round(float(row["total_rain"]), 2)})
        if pd.notna(row.get("moisture_depletion_rate_hourly")) and float(row["moisture_depletion_rate_hourly"]) > 1.0:
            top_drivers.append({"factor": "rapid_depletion", "impact": "accelerates_drying", "value": round(float(row["moisture_depletion_rate_hourly"]), 2)})

        snapshot.update({
            "target": "current_dryness",
            "forecast_available": False,
            "observation_time": str(row.get("last_update", "")),
            "label_source": "crop_moisture_threshold_rule",
            "irrigation_probability": round(prob, 6),
            "net_irrigation_requirement_mm": nir_mm,
            "recommended_duration_seconds": rec_duration_sec,
            "primary_drivers": top_drivers,
        })
        prediction_key = json.dumps({"farm_id": int(row["farm_id"]), "zone_id": int(row["zone_id"]),
                                    "model_version": model_version, "snapshot": snapshot}, sort_keys=True, allow_nan=False)
        predictions.append({
            "prediction_id": str(uuid.uuid5(uuid.NAMESPACE_URL, prediction_key)),
            "zone_id": int(row["zone_id"]),
            "zone_name": str(row.get("zone_name", "")),
            "farm_id": int(row["farm_id"]),
            "farm_name": str(row.get("farm_name", "")),
            "crop_name": str(row.get("crop_name", "")),
            "needs_irrigation_today": int(prob >= 0.5),
            "needs_irrigation_tomorrow": None,
            "prediction_confidence": round(max(prob, 1 - prob), 4),
            "model_version": model_version,
            "prediction_date": pd.to_datetime(row["last_update"], utc=True).date() if pd.notna(row.get("last_update")) else now.date(),
            "prediction_time": now.replace(tzinfo=None),
            "feature_snapshot": json.dumps(snapshot, allow_nan=False),
        })
    return predictions
