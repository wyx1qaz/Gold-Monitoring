from __future__ import annotations

import hashlib
import json
import math
import random
import re
import csv
import threading
import time
import traceback
from difflib import SequenceMatcher
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .config import DATA_DIR, SHANGHAI_TZ
from .db import Database


CN_TZ = ZoneInfo(SHANGHAI_TZ)
TOKEN_SPLIT_RE = re.compile(r"[^a-z0-9]+")
CLASS_LABELS = (
    "大幅利好黄金",
    "小幅利好黄金",
    "小幅利空黄金",
    "大幅利空黄金",
)
CLASS_SCORE_CENTER = (9.0, 7.0, 4.0, 2.0)
CLASS_LABEL_FALLBACK = ("大幅利好", "小幅利好", "小幅利空", "大幅利空")
CLASS_REASON_FALLBACK = (
    "重大避险驱动(大幅利好)",
    "温和避险驱动(小幅利好)",
    "温和风险偏好回升(小幅利空)",
    "风险偏好明显回升(大幅利空)",
)
LABEL_TO_CLASS_MAP = {
    "大幅利好": 0,
    "大幅利好黄金": 0,
    "小幅利好": 1,
    "小幅利好黄金": 1,
    "小幅利空": 2,
    "小幅利空黄金": 2,
    "大幅利空": 3,
    "大幅利空黄金": 3,
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _stable_hash(text: str) -> int:
    return int(hashlib.sha1(text.encode("utf-8")).hexdigest()[:8], 16)


def _tokenize(text: str) -> list[str]:
    text = (text or "").strip().lower()
    if not text:
        return []
    tokens: list[str] = []
    parts = [item for item in TOKEN_SPLIT_RE.split(text) if item]
    tokens.extend(parts)
    for ch in text:
        if "\u4e00" <= ch <= "\u9fff":
            tokens.append(ch)
    return tokens


def _normalize_title_for_csv_dedup(text: str) -> str:
    normalized = str(text or "").lower()
    replacements = {
        "船员称": "船只称",
        "船员": "船只",
        "液化天然气船": "油轮",
        "液化天然气轮": "油轮",
        "lng船": "油轮",
        "lng轮": "油轮",
    }
    for src, dst in replacements.items():
        normalized = normalized.replace(src, dst)
    normalized = re.sub(r"\s+", "", normalized)
    normalized = re.sub(r"[^\w\u4e00-\u9fff]", "", normalized)
    return normalized


def _is_near_duplicate_title(left: str, right: str) -> bool:
    if not left or not right:
        return False
    if left == right:
        return True
    max_len = max(len(left), len(right))
    min_len = min(len(left), len(right))
    if max_len <= 0:
        return False
    if (max_len - min_len) / max_len > 0.40:
        return False
    ratio = SequenceMatcher(None, left, right).ratio()
    if ratio >= 0.90:
        return True
    if min_len >= 18 and (left in right or right in left):
        return True
    return False


def _softmax(values: list[float]) -> list[float]:
    if not values:
        return []
    vmax = max(values)
    exp_values = [math.exp(v - vmax) for v in values]
    s = sum(exp_values) or 1.0
    return [v / s for v in exp_values]


def _score_to_class(target_score: float) -> int:
    score = _clamp(float(target_score), 1.0, 10.0)
    if score >= 8.0:
        return 0
    if score >= 6.0:
        return 1
    if score >= 4.0:
        return 2
    return 3


def _normalize_label_text(text: str) -> str:
    value = str(text or "").strip().lower()
    value = value.replace(" ", "").replace("_", "").replace("-", "").replace("/", "")
    return value


def _class_from_label_text(label: str) -> int | None:
    normalized = _normalize_label_text(label)
    if not normalized:
        return None
    for raw, cls in LABEL_TO_CLASS_MAP.items():
        if normalized == _normalize_label_text(raw):
            return cls
    for idx, value in enumerate(CLASS_LABELS):
        if normalized == _normalize_label_text(value):
            return idx
    for idx, value in enumerate(CLASS_LABEL_FALLBACK):
        if normalized == _normalize_label_text(value):
            return idx
    return None


@dataclass
class TrainingResult:
    model_version: str
    sample_count: int
    train_loss: float
    val_loss: float
    train_accuracy: float
    val_accuracy: float
    best_epoch: int
    max_epochs: int
    learning_rate: float
    early_stop_patience: int
    note: str
    train_curve: list[float]
    val_curve: list[float]
    train_acc_curve: list[float]
    val_acc_curve: list[float]
    class_metrics: list[dict[str, Any]]
    confusion_matrix: list[list[int]]
    labels: list[str]
    macro_f1: float


class FiveLayerClassifier:
    def __init__(self, input_dim: int, hidden_dims: list[int], class_count: int, seed: int = 42) -> None:
        self.input_dim = input_dim
        self.hidden_dims = hidden_dims
        self.class_count = class_count
        self.rng = random.Random(seed)
        layer_dims = [input_dim, *hidden_dims, class_count]
        self.weights: list[list[list[float]]] = []
        self.biases: list[list[float]] = []
        for in_dim, out_dim in zip(layer_dims[:-1], layer_dims[1:]):
            scale = math.sqrt(2.0 / max(1, in_dim))
            self.weights.append(
                [[self.rng.uniform(-scale, scale) for _ in range(in_dim)] for _ in range(out_dim)]
            )
            self.biases.append([0.0 for _ in range(out_dim)])

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_dim": self.input_dim,
            "hidden_dims": self.hidden_dims,
            "class_count": self.class_count,
            "weights": self.weights,
            "biases": self.biases,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FiveLayerClassifier":
        model = cls(
            input_dim=int(payload["input_dim"]),
            hidden_dims=[int(x) for x in payload["hidden_dims"]],
            class_count=int(payload["class_count"]),
        )
        model.weights = payload["weights"]
        model.biases = payload["biases"]
        return model

    def _forward(self, x: list[float]) -> tuple[list[list[float]], list[list[float]]]:
        activations = [x]
        pre_activations: list[list[float]] = []
        current = x
        for layer_idx, (weight, bias) in enumerate(zip(self.weights, self.biases)):
            z: list[float] = []
            for out_idx, row in enumerate(weight):
                value = bias[out_idx]
                for in_idx, w in enumerate(row):
                    value += w * current[in_idx]
                z.append(value)
            pre_activations.append(z)
            if layer_idx < len(self.weights) - 1:
                current = [max(0.0, val) for val in z]
            else:
                current = z
            activations.append(current)
        return activations, pre_activations

    def predict_proba(self, x: list[float]) -> list[float]:
        activations, _ = self._forward(x)
        return _softmax(activations[-1])

    def train(
        self,
        features: list[list[float]],
        targets: list[int],
        *,
        learning_rate: float,
        max_epochs: int,
        early_stop_patience: int,
        validation_ratio: float,
        sample_weights: list[float] | None = None,
        use_class_weight: bool = True,
        use_resample: bool = True,
        check_pause_cancel: callable | None = None,
        on_epoch_end: callable | None = None,
    ) -> tuple[float, float, float, float, int, list[float], list[float], list[float], list[float]]:
        total = len(features)
        if total < 10:
            raise ValueError("not enough samples for training")
        if total != len(targets):
            raise ValueError("features/targets length mismatch")

        indices = list(range(total))
        self.rng.shuffle(indices)
        val_size = max(10, int(total * validation_ratio))
        val_size = min(total - 1, val_size) if total > 1 else 1
        val_set = set(indices[:val_size])
        train_indices = [i for i in indices if i not in val_set]
        val_indices = [i for i in indices if i in val_set]
        class_count = self.class_count
        class_freq = [0 for _ in range(class_count)]
        for idx in train_indices:
            y = targets[idx]
            if 0 <= y < class_count:
                class_freq[y] += 1
        class_weights = [1.0 for _ in range(class_count)]
        if use_class_weight:
            total_train = max(1, len(train_indices))
            for cls in range(class_count):
                freq = class_freq[cls]
                if freq <= 0:
                    class_weights[cls] = 1.0
                else:
                    class_weights[cls] = _clamp(total_train / (class_count * freq), 0.5, 8.0)

        best_snapshot = self.to_dict()
        best_val_loss = float("inf")
        best_train_loss = float("inf")
        best_train_acc = 0.0
        best_val_acc = 0.0
        best_epoch = 1
        stale_epochs = 0
        min_lr = learning_rate * 0.05

        train_curve: list[float] = []
        val_curve: list[float] = []
        train_acc_curve: list[float] = []
        val_acc_curve: list[float] = []

        for epoch in range(1, max_epochs + 1):
            if check_pause_cancel:
                check_pause_cancel()

            cosine = 0.5 * (1.0 + math.cos(math.pi * (epoch - 1) / max(1, max_epochs - 1)))
            lr_epoch = min_lr + (learning_rate - min_lr) * cosine
            epoch_indices = list(train_indices)
            if use_resample:
                per_class: dict[int, list[int]] = {i: [] for i in range(class_count)}
                for idx in train_indices:
                    y = targets[idx]
                    per_class.setdefault(y, []).append(idx)
                non_empty = [v for v in per_class.values() if v]
                if non_empty:
                    max_size = max(len(v) for v in non_empty)
                    epoch_indices = []
                    for cls in range(class_count):
                        cls_idx = per_class.get(cls) or []
                        if not cls_idx:
                            continue
                        if len(cls_idx) < max_size:
                            add = cls_idx + [self.rng.choice(cls_idx) for _ in range(max_size - len(cls_idx))]
                            epoch_indices.extend(add)
                        else:
                            epoch_indices.extend(cls_idx[:max_size])
            self.rng.shuffle(epoch_indices)

            for step_idx, idx in enumerate(epoch_indices, start=1):
                if check_pause_cancel and step_idx % 5 == 0:
                    check_pause_cancel()
                x = features[idx]
                y = targets[idx]
                activations, pre_activations = self._forward(x)
                probs = _softmax(activations[-1])
                grad = [p for p in probs]
                grad[y] -= 1.0
                weight = class_weights[y] if 0 <= y < len(class_weights) else 1.0
                if sample_weights and idx < len(sample_weights):
                    weight *= max(0.0, float(sample_weights[idx]))
                weight = _clamp(weight, 0.05, 20.0)

                for layer_idx in range(len(self.weights) - 1, -1, -1):
                    prev_activation = activations[layer_idx]
                    current_weight = self.weights[layer_idx]
                    current_bias = self.biases[layer_idx]

                    for out_idx in range(len(current_weight)):
                        g = grad[out_idx] * weight
                        current_bias[out_idx] -= lr_epoch * g
                        row = current_weight[out_idx]
                        for in_idx in range(len(row)):
                            row[in_idx] -= lr_epoch * g * prev_activation[in_idx]

                    if layer_idx > 0:
                        new_grad = [0.0 for _ in range(len(self.weights[layer_idx][0]))]
                        for out_idx, row in enumerate(current_weight):
                            for in_idx, w in enumerate(row):
                                new_grad[in_idx] += grad[out_idx] * w
                        prev_z = pre_activations[layer_idx - 1]
                        for in_idx, z_value in enumerate(prev_z):
                            if z_value <= 0:
                                new_grad[in_idx] = 0.0
                        grad = new_grad

            train_loss, train_acc = self._evaluate(features, targets, train_indices)
            val_loss, val_acc = self._evaluate(features, targets, val_indices)
            train_curve.append(train_loss)
            val_curve.append(val_loss)
            train_acc_curve.append(train_acc)
            val_acc_curve.append(val_acc)

            if on_epoch_end:
                on_epoch_end(epoch, train_loss, val_loss, train_acc, val_acc)

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_train_loss = train_loss
                best_train_acc = train_acc
                best_val_acc = val_acc
                best_epoch = epoch
                stale_epochs = 0
                best_snapshot = self.to_dict()
            else:
                stale_epochs += 1
                if stale_epochs >= early_stop_patience:
                    break

        restored = FiveLayerClassifier.from_dict(best_snapshot)
        self.weights = restored.weights
        self.biases = restored.biases
        return (
            best_train_loss,
            best_val_loss,
            best_train_acc,
            best_val_acc,
            best_epoch,
            train_curve,
            val_curve,
            train_acc_curve,
            val_acc_curve,
        )

    def _evaluate(self, features: list[list[float]], targets: list[int], indices: list[int]) -> tuple[float, float]:
        if not indices:
            return 0.0, 0.0
        total_loss = 0.0
        correct = 0
        for idx in indices:
            probs = self.predict_proba(features[idx])
            y = targets[idx]
            p = max(1e-12, probs[y])
            total_loss += -math.log(p)
            pred = max(range(len(probs)), key=lambda i: probs[i])
            if pred == y:
                correct += 1
        return total_loss / len(indices), correct / len(indices)


class RssMlService:
    def __init__(self, db: Database, *, model_path: Path | None = None) -> None:
        self.db = db
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.model_path = model_path or (DATA_DIR / "rss_event_classifier_model.json")
        self.dataset_csv_path = DATA_DIR / "rss_ml_samples_sync.csv"
        self.fetched_csv_path = DATA_DIR / "rss_fetched_events_sync.csv"
        self.text_feature_dim = 256
        self.hidden_dims = [128, 96, 64, 48, 32]
        self.learning_rate = 0.001
        self.max_epochs = 120
        self.early_stop_patience = 25
        self.train_step_size = 100
        self.min_train_samples = 30
        self.active_window_hours = 24
        self.weak_move_pct = 0.10
        self.strong_move_pct = 0.35
        self.decay_half_life_hours = 24.0
        self.label_mode = "future_return"
        self.use_class_weight = True
        self.use_resample = True

        self.model: FiveLayerClassifier | None = None
        self.model_version: str = ""
        self.runtime_lock = threading.Lock()
        self.pause_event = threading.Event()
        self.cancel_event = threading.Event()
        self.train_thread: threading.Thread | None = None
        self.last_csv_db_sync: dict[str, Any] | None = None
        self.last_fetch_csv_sync: dict[str, Any] | None = None
        self.last_event_score_backfill: dict[str, Any] | None = None
        self.runtime: dict[str, Any] = {
            "running": False,
            "paused": False,
            "cancelled": False,
            "cancel_requested": False,
            "state": "idle",
            "message": "待机",
            "started_at": None,
            "finished_at": None,
            "epoch": 0,
            "max_epochs": 0,
            "sample_count": 0,
            "train_curve": [],
            "val_curve": [],
            "train_acc_curve": [],
            "val_acc_curve": [],
            "error": None,
        }

        self._bootstrap_from_existing_events()
        self.reload_runtime_config()
        self._load_model()
        if not self.fetched_csv_path.exists():
            try:
                self.sync_fetched_csv_from_db(overwrite=True)
            except Exception:
                pass

    def reload_runtime_config(self) -> dict[str, Any]:
        settings = self.db.get_settings()
        self.learning_rate = float(settings.get("rss_ml_learning_rate", 0.001))
        self.max_epochs = max(20, int(settings.get("rss_ml_max_epochs", 120)))
        self.early_stop_patience = max(3, int(settings.get("rss_ml_early_stop_patience", 25)))
        self.train_step_size = max(10, int(settings.get("rss_ml_train_step_size", 100)))
        self.min_train_samples = max(10, int(settings.get("rss_ml_min_train_samples", 30)))
        self.active_window_hours = min(24, max(1, int(settings.get("rss_ml_active_window_hours", 24))))
        self.weak_move_pct = max(0.01, float(settings.get("rss_ml_weak_move_pct", 0.10)))
        self.strong_move_pct = max(self.weak_move_pct + 0.01, float(settings.get("rss_ml_strong_move_pct", 0.35)))
        self.decay_half_life_hours = max(1.0, float(settings.get("rss_ml_decay_half_life_hours", 24)))
        self.label_mode = str(settings.get("rss_ml_label_mode", "future_return")).strip().lower()
        if self.label_mode not in {"future_return", "manual_score"}:
            self.label_mode = "future_return"
        return {
            "learning_rate": self.learning_rate,
            "max_epochs": self.max_epochs,
            "early_stop_patience": self.early_stop_patience,
            "train_step_size": self.train_step_size,
            "min_train_samples": self.min_train_samples,
            "active_window_hours": self.active_window_hours,
            "weak_move_pct": self.weak_move_pct,
            "strong_move_pct": self.strong_move_pct,
            "decay_half_life_hours": self.decay_half_life_hours,
            "label_mode": self.label_mode,
            "use_class_weight": self.use_class_weight,
            "use_resample": self.use_resample,
        }

    def _bootstrap_from_existing_events(self) -> None:
        missing = self.db.get_rss_events_missing_ml_samples(limit=50000)
        for row in missing:
            event_text = str(row.get("title") or "").strip()
            if not event_text:
                continue
            self.db.insert_rss_ml_sample(
                event_id=int(row["id"]),
                event_text=event_text,
                gold_price_usd_per_oz=None,
                gold_change_pct=None,
                target_score=int(row.get("impact_score") or 5),
                created_at=str(row.get("fetched_at") or datetime.now(CN_TZ).isoformat()),
            )

    def _build_feature_vector(self, title_text: str) -> list[float]:
        features = [0.0 for _ in range(self.text_feature_dim)]
        tokens = _tokenize(title_text)
        for token in tokens:
            idx = _stable_hash(token) % self.text_feature_dim
            features[idx] += 1.0
        total = sum(features)
        if total > 0:
            features = [v / total for v in features]
        return features

    def sync_csv_from_db(self, *, overwrite: bool = False) -> dict[str, Any]:
        if self.dataset_csv_path.exists() and not overwrite:
            return {
                "ok": True,
                "path": str(self.dataset_csv_path),
                "rows": self._count_csv_rows(self.dataset_csv_path),
                "overwritten": False,
            }
        rows = self.db.get_rss_ml_training_rows()
        existing_annotations = self._load_existing_csv_annotations() if self.dataset_csv_path.exists() else {}
        # CSV-level near-duplicate merge for model training stability.
        deduped_rows: list[dict[str, Any]] = []
        kept_titles: list[str] = []
        csv_near_removed = 0
        # Keep newest rows when collisions happen, then restore chronological output.
        for row in reversed(rows):
            title = str(row.get("event_text") or "").strip()
            normalized = _normalize_title_for_csv_dedup(title)
            if normalized:
                is_dup = any(_is_near_duplicate_title(normalized, kept) for kept in kept_titles if kept)
                if is_dup:
                    csv_near_removed += 1
                    continue
                kept_titles.append(normalized)
            deduped_rows.append(row)
        rows = list(reversed(deduped_rows))
        try:
            with self.dataset_csv_path.open("w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "event_id",
                        "title",
                        "class_label",
                        "D_score",
                        "E_reasoning",
                        "label_source",
                        "target_score",
                        "event_time",
                        "future_return_pct",
                        "created_at",
                    ],
                )
                writer.writeheader()
                for row in rows:
                    score = float(row.get("target_score") or 5)
                    event_time = str(row.get("published_at") or row.get("fetched_at") or row.get("created_at") or "").strip()
                    future_ret = self._calc_future_return_pct(event_time, self.active_window_hours) if event_time else None
                    if self.label_mode == "future_return" and future_ret is not None:
                        cls = self._future_return_to_class(future_ret)
                    else:
                        cls = _score_to_class(score)
                    event_id = str(row.get("event_id") or "").strip()
                    existing = existing_annotations.get(event_id, {})
                    manual_label = str(existing.get("class_label") or "").strip()
                    manual_score = str(existing.get("D_score") or "").strip()
                    manual_reason = str(existing.get("E_reasoning") or "").strip()
                    if manual_label or manual_score or manual_reason:
                        class_label = manual_label
                        d_score = manual_score
                        e_reasoning = manual_reason
                    else:
                        # New rows are appended as unlabeled so manual calibration is not overwritten.
                        class_label = ""
                        d_score = ""
                        e_reasoning = ""
                    writer.writerow(
                        {
                            "event_id": event_id,
                            "title": str(row.get("event_text") or "").strip(),
                            "class_label": class_label,
                            "D_score": d_score,
                            "E_reasoning": e_reasoning,
                            "label_source": "future_return" if (self.label_mode == "future_return" and future_ret is not None) else "manual_score",
                            "target_score": int(round(score)),
                            "event_time": event_time,
                            "future_return_pct": "" if future_ret is None else round(float(future_ret), 6),
                            "created_at": row.get("created_at"),
                        }
                    )
        except PermissionError:
            return {
                "ok": False,
                "error": "csv_locked",
                "path": str(self.dataset_csv_path),
                "message": "CSV 文件被占用（请先关闭 Excel 或其他编辑器后重试）",
            }
        return {
            "ok": True,
            "path": str(self.dataset_csv_path),
            "rows": len(rows),
            "csv_near_removed": csv_near_removed,
            "overwritten": True,
        }

    def sync_fetched_csv_from_db(self, *, overwrite: bool = True) -> dict[str, Any]:
        if self.fetched_csv_path.exists() and not overwrite:
            result = {
                "ok": True,
                "path": str(self.fetched_csv_path),
                "rows": self._count_csv_rows(self.fetched_csv_path),
                "overwritten": False,
            }
            self.last_fetch_csv_sync = result
            return result
        rows = self.db.get_rss_events_for_export()
        try:
            with self.fetched_csv_path.open("w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "event_id",
                        "fetched_at",
                        "published_at",
                        "source",
                        "feed_url",
                        "event_type",
                        "title",
                        "summary",
                        "link",
                        "matched_keywords",
                        "impact_score",
                        "impact_level",
                        "impact_note",
                        "ml_score",
                        "ml_bucket_label",
                        "ml_model_version",
                        "ml_scored_at",
                        "ml_class_probs",
                    ],
                )
                writer.writeheader()
                for row in rows:
                    writer.writerow(
                        {
                            "event_id": row.get("id"),
                            "fetched_at": row.get("fetched_at"),
                            "published_at": row.get("published_at"),
                            "source": row.get("source"),
                            "feed_url": row.get("feed_url"),
                            "event_type": row.get("event_type"),
                            "title": str(row.get("title") or "").strip(),
                            "summary": str(row.get("summary") or "").strip(),
                            "link": row.get("link"),
                            "matched_keywords": row.get("matched_keywords"),
                            "impact_score": row.get("impact_score"),
                            "impact_level": row.get("impact_level"),
                            "impact_note": row.get("impact_note"),
                            "ml_score": row.get("ml_score"),
                            "ml_bucket_label": row.get("ml_bucket_label"),
                            "ml_model_version": row.get("ml_model_version"),
                            "ml_scored_at": row.get("ml_scored_at"),
                            "ml_class_probs": row.get("ml_class_probs"),
                        }
                    )
        except PermissionError:
            result = {
                "ok": False,
                "error": "csv_locked",
                "path": str(self.fetched_csv_path),
                "message": "抓取CSV文件被占用（请先关闭 Excel 或其他编辑器后重试）",
            }
            self.last_fetch_csv_sync = result
            return result
        result = {
            "ok": True,
            "path": str(self.fetched_csv_path),
            "rows": len(rows),
            "overwritten": True,
        }
        self.last_fetch_csv_sync = result
        return result

    def score_unscored_rss_events(self, *, limit: int = 5000) -> dict[str, Any]:
        if self.model is None or not self.model_version:
            result = {
                "ok": False,
                "reason": "model_not_ready",
                "scored": 0,
                "checked": 0,
            }
            self.last_event_score_backfill = result
            return result
        rows = self.db.get_rss_events_without_ml_score(limit=limit)
        scored = 0
        now_iso = datetime.now(CN_TZ).isoformat()
        for row in rows:
            event_id = int(row["id"])
            title = str(row.get("title") or "").strip()
            summary = str(row.get("summary") or "").strip()
            event_text = title or summary
            if not event_text:
                continue
            predicted = self.predict_score(
                event_text=event_text,
                gold_price_usd_per_oz=None,
                gold_change_pct=None,
            )
            if not predicted:
                continue
            score, model_version, bucket_label, prob_map = predicted
            self.db.update_rss_event_ml_score(
                event_id=event_id,
                ml_score=score,
                ml_model_version=model_version,
                ml_scored_at=now_iso,
                ml_bucket_label=bucket_label,
                ml_class_probs=json.dumps(prob_map, ensure_ascii=False),
            )
            self.db.update_rss_ml_prediction(
                event_id=event_id,
                predicted_score=score,
                model_version=model_version,
                scored_at=now_iso,
            )
            scored += 1
        result = {
            "ok": True,
            "checked": len(rows),
            "scored": scored,
            "model_version": self.model_version,
        }
        self.last_event_score_backfill = result
        return result

    @staticmethod
    def _count_csv_rows(path: Path) -> int:
        if not path.exists():
            return 0
        last_error: Exception | None = None
        for enc in ("utf-8-sig", "gb18030", "utf-8"):
            try:
                with path.open("r", encoding=enc, newline="", errors="strict") as f:
                    reader = csv.DictReader(f)
                    return sum(1 for _ in reader)
            except UnicodeDecodeError as exc:
                last_error = exc
                continue
        if last_error:
            raise last_error
        return 0

    def _load_existing_csv_annotations(self) -> dict[str, dict[str, str]]:
        if not self.dataset_csv_path.exists():
            return {}
        last_error: Exception | None = None
        for enc in ("utf-8-sig", "gb18030", "utf-8"):
            try:
                mapping: dict[str, dict[str, str]] = {}
                with self.dataset_csv_path.open("r", encoding=enc, newline="", errors="strict") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        event_id = str(row.get("event_id") or "").strip()
                        if not event_id:
                            continue
                        mapping[event_id] = {
                            "class_label": str(row.get("class_label") or "").strip(),
                            "D_score": str(row.get("D_score") or row.get("d_score") or "").strip(),
                            "E_reasoning": str(row.get("E_reasoning") or row.get("e_reasoning") or "").strip(),
                        }
                return mapping
            except UnicodeDecodeError as exc:
                last_error = exc
                continue
        if last_error:
            raise last_error
        return {}

    def _load_labeled_rows_with_meta_from_csv(self) -> list[dict[str, Any]]:
        if not self.dataset_csv_path.exists():
            return []
        rows: list[dict[str, Any]] = []
        last_error: Exception | None = None
        for enc in ("utf-8-sig", "gb18030", "utf-8"):
            try:
                with self.dataset_csv_path.open("r", encoding=enc, newline="", errors="strict") as f:
                    reader = csv.DictReader(f)
                    fieldnames = {str(name or "").strip().lower() for name in (reader.fieldnames or [])}
                    has_manual_v2 = ("d_score" in fieldnames) or ("e_reasoning" in fieldnames)
                    for row in reader:
                        title = str(row.get("title") or row.get("event_text") or "").strip()
                        if not title:
                            continue
                        label = str(row.get("class_label") or "").strip()
                        event_id_raw = str(row.get("event_id") or "").strip()
                        event_id = None
                        if event_id_raw:
                            try:
                                event_id = int(float(event_id_raw))
                            except ValueError:
                                event_id = None
                        d_score_raw = row.get("D_score")
                        if d_score_raw in (None, ""):
                            d_score_raw = row.get("d_score")
                        mapped_cls = None
                        if d_score_raw not in (None, ""):
                            try:
                                d_score = int(float(str(d_score_raw)))
                                if 1 <= d_score <= 4:
                                    mapped_cls = d_score - 1
                            except ValueError:
                                mapped_cls = None
                        score_raw = row.get("target_score")
                        score = None
                        if score_raw not in (None, ""):
                            try:
                                score = float(score_raw)
                            except ValueError:
                                score = None
                        event_time = str(row.get("event_time") or row.get("published_at") or row.get("fetched_at") or "").strip()
                        created_at = str(row.get("created_at") or "").strip()
                        future_ret_raw = row.get("future_return_pct")
                        future_ret = None
                        if future_ret_raw not in (None, ""):
                            try:
                                future_ret = float(future_ret_raw)
                            except ValueError:
                                future_ret = None

                        cls = None
                        if mapped_cls is not None:
                            cls = mapped_cls
                        if cls is None:
                            cls = _class_from_label_text(label)
                        if cls is None and not has_manual_v2 and self.label_mode == "future_return":
                            if future_ret is None and event_time:
                                future_ret = self._calc_future_return_pct(event_time, self.active_window_hours)
                            if future_ret is not None:
                                cls = self._future_return_to_class(future_ret)
                        if cls is None and not has_manual_v2 and score is not None:
                            cls = _score_to_class(score)
                        if cls is None:
                            continue
                        rows.append(
                            {
                                "event_id": event_id,
                                "title": title,
                                "target_class": int(cls),
                                "event_time": event_time or None,
                                "created_at": created_at or None,
                            }
                        )
                return rows
            except UnicodeDecodeError as exc:
                last_error = exc
                rows = []
                continue
        if last_error:
            raise last_error
        return rows

    def _load_rows_from_csv(self) -> list[dict[str, Any]]:
        rows = self._load_labeled_rows_with_meta_from_csv()
        return [
            {
                "title": item["title"],
                "target_class": int(item["target_class"]),
                "event_time": item.get("event_time"),
            }
            for item in rows
        ]

    def sync_db_from_csv(self) -> dict[str, Any]:
        if not self.dataset_csv_path.exists():
            result = {
                "ok": False,
                "reason": "csv_not_found",
                "csv_path": str(self.dataset_csv_path),
                "total_labeled_rows": 0,
                "synced_samples": 0,
            }
            self.last_csv_db_sync = result
            return result

        rows = self._load_labeled_rows_with_meta_from_csv()
        synced_samples = 0
        skipped_without_event_id = 0
        touched_event_ids: set[int] = set()
        now_iso = datetime.now(CN_TZ).isoformat()
        for item in rows:
            event_id = item.get("event_id")
            if event_id is None:
                skipped_without_event_id += 1
                continue
            cls = int(item["target_class"])
            target_score = int(round(CLASS_SCORE_CENTER[cls]))
            self.db.insert_rss_ml_sample(
                event_id=int(event_id),
                event_text=str(item["title"]).strip(),
                gold_price_usd_per_oz=None,
                gold_change_pct=None,
                target_score=target_score,
                created_at=str(item.get("created_at") or now_iso),
            )
            synced_samples += 1
            touched_event_ids.add(int(event_id))

        result = {
            "ok": True,
            "csv_path": str(self.dataset_csv_path),
            "total_labeled_rows": len(rows),
            "synced_samples": synced_samples,
            "synced_unique_event_ids": len(touched_event_ids),
            "skipped_without_event_id": skipped_without_event_id,
        }
        self.last_csv_db_sync = result
        return result

    @staticmethod
    def _parse_dt(raw: str | None) -> datetime | None:
        if not raw:
            return None
        value = str(raw).strip()
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=CN_TZ)
        return dt.astimezone(CN_TZ)

    def _calc_time_decay_weight(self, event_time_raw: str | None) -> float:
        event_dt = self._parse_dt(event_time_raw)
        if event_dt is None:
            return 1.0
        now_dt = datetime.now(CN_TZ)
        age_hours = max(0.0, (now_dt - event_dt).total_seconds() / 3600.0)
        decay = math.exp(-math.log(2) * age_hours / max(1.0, self.decay_half_life_hours))
        return 0.2 + 0.8 * decay

    def _future_return_to_class(self, future_return_pct: float) -> int:
        value = float(future_return_pct)
        if value >= self.strong_move_pct:
            return 0
        if value >= self.weak_move_pct:
            return 1
        if value <= -self.strong_move_pct:
            return 3
        if value <= -self.weak_move_pct:
            return 2
        return 1 if value >= 0 else 2

    def _calc_future_return_pct(self, event_time_raw: str, horizon_hours: int) -> float | None:
        event_dt = self._parse_dt(event_time_raw)
        if event_dt is None:
            return None
        event_iso = event_dt.isoformat()
        future_iso = (event_dt + timedelta(hours=max(1, int(horizon_hours)))).isoformat()
        p0 = self.db.get_gold_price_at_or_before(event_iso)
        p1 = self.db.get_gold_price_at_or_after(future_iso)
        if p0 in (None, 0) or p1 is None:
            return None
        return (float(p1) - float(p0)) / float(p0) * 100.0

    def _build_dataset(self) -> tuple[list[list[float]], list[int], list[float], int, str]:
        csv_rows = self._load_rows_from_csv()
        if csv_rows:
            features = [self._build_feature_vector(item["title"]) for item in csv_rows]
            targets = [int(item["target_class"]) for item in csv_rows]
            time_weights = [self._calc_time_decay_weight(item.get("event_time")) for item in csv_rows]
            return features, targets, time_weights, len(features), "csv"

        rows = self.db.get_rss_ml_training_rows()
        features: list[list[float]] = []
        targets: list[int] = []
        time_weights: list[float] = []
        for row in rows:
            title_only = str(row.get("event_text") or "").strip()
            if not title_only:
                continue
            event_time = str(row.get("published_at") or row.get("fetched_at") or row.get("created_at") or "").strip()
            cls = None
            if self.label_mode == "future_return":
                future_ret = self._calc_future_return_pct(event_time, self.active_window_hours) if event_time else None
                if future_ret is not None:
                    cls = self._future_return_to_class(future_ret)
            if cls is None:
                cls = _score_to_class(float(row.get("target_score") or 5))
            features.append(self._build_feature_vector(title_only))
            targets.append(int(cls))
            time_weights.append(self._calc_time_decay_weight(event_time))
        # initialize csv template once if absent so user can calibrate manually
        if not self.dataset_csv_path.exists():
            self.sync_csv_from_db(overwrite=True)
        return features, targets, time_weights, len(features), "db"

    def _calc_class_metrics(
        self,
        model: FiveLayerClassifier,
        features: list[list[float]],
        targets: list[int],
    ) -> tuple[list[dict[str, Any]], list[list[int]]]:
        n = len(CLASS_LABELS)
        matrix = [[0 for _ in range(n)] for _ in range(n)]
        if not features:
            return [], matrix
        for x, y in zip(features, targets, strict=False):
            probs = model.predict_proba(x)
            pred = max(range(len(probs)), key=lambda i: probs[i])
            matrix[y][pred] += 1

        metrics: list[dict[str, Any]] = []
        for idx, label in enumerate(CLASS_LABELS):
            tp = matrix[idx][idx]
            fp = sum(matrix[r][idx] for r in range(n)) - tp
            fn = sum(matrix[idx][c] for c in range(n)) - tp
            support = sum(matrix[idx])
            precision = tp / (tp + fp) if (tp + fp) else 0.0
            recall = tp / (tp + fn) if (tp + fn) else 0.0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
            metrics.append(
                {
                    "label": label,
                    "precision": round(float(precision), 6),
                    "recall": round(float(recall), 6),
                    "f1": round(float(f1), 6),
                    "support": int(support),
                }
            )
        return metrics, matrix

    @staticmethod
    def _macro_f1(class_metrics: list[dict[str, Any]]) -> float:
        if not class_metrics:
            return 0.0
        return float(sum(float(item.get("f1") or 0.0) for item in class_metrics) / len(class_metrics))

    def _check_pause_cancel(self) -> None:
        while self.pause_event.is_set():
            with self.runtime_lock:
                self.runtime["paused"] = True
                self.runtime["state"] = "paused"
                self.runtime["message"] = "训练已暂停"
            if self.cancel_event.is_set():
                raise RuntimeError("Training cancelled by user")
            time.sleep(0.2)
        if self.cancel_event.is_set():
            raise RuntimeError("Training cancelled by user")
        with self.runtime_lock:
            self.runtime["paused"] = False
            self.runtime["state"] = "running"

    def _on_epoch_end(self, epoch: int, train_loss: float, val_loss: float, train_acc: float, val_acc: float) -> None:
        with self.runtime_lock:
            self.runtime["epoch"] = epoch
            self.runtime["train_curve"].append(float(train_loss))
            self.runtime["val_curve"].append(float(val_loss))
            self.runtime["train_acc_curve"].append(float(train_acc))
            self.runtime["val_acc_curve"].append(float(val_acc))
            self.runtime["message"] = (
                f"Epoch {epoch}/{self.runtime.get('max_epochs', 0)} "
                f"val_loss={val_loss:.5f} val_acc={val_acc * 100:.2f}%"
            )

    def maybe_train(self, *, force: bool = False, min_samples_override: int | None = None) -> TrainingResult | None:
        self.reload_runtime_config()
        features, targets, time_weights, sample_count, _ = self._build_dataset()
        min_samples = self.min_train_samples if min_samples_override is None else max(10, int(min_samples_override))
        if sample_count < min_samples:
            return None

        current_bucket = max(1, sample_count // self.train_step_size)
        if not force:
            latest = self.db.get_latest_rss_ml_training_run()
            latest_bucket = int(latest.get("sample_count", 0)) // self.train_step_size if latest else 0
            if current_bucket <= latest_bucket:
                return None

        model = FiveLayerClassifier(
            input_dim=self.text_feature_dim,
            hidden_dims=list(self.hidden_dims),
            class_count=4,
            seed=42 + sample_count,
        )
        train_loss, val_loss, train_acc, val_acc, best_epoch, train_curve, val_curve, train_acc_curve, val_acc_curve = model.train(
            features,
            targets,
            learning_rate=self.learning_rate,
            max_epochs=self.max_epochs,
            early_stop_patience=self.early_stop_patience,
            validation_ratio=0.2,
            sample_weights=time_weights,
            use_class_weight=self.use_class_weight,
            use_resample=self.use_resample,
        )

        trained_at = datetime.now(CN_TZ).isoformat()
        model_version = f"ml-cls-v{current_bucket}-{trained_at.replace(':', '').replace('-', '')}"
        self.model = model
        self.model_version = model_version
        self._save_model(trained_at=trained_at)
        class_metrics, confusion_matrix = self._calc_class_metrics(model, features, targets)
        macro_f1 = self._macro_f1(class_metrics)

        note = (
            f"标题单列神经网络四类聚类完成；lr={self.learning_rate}；"
            f"余弦退火；早停{self.early_stop_patience}轮"
        )
        if force:
            note = f"{note}；手动触发"

        self.db.insert_rss_ml_training_run(
            trained_at=trained_at,
            sample_count=sample_count,
            model_version=model_version,
            learning_rate=self.learning_rate,
            max_epochs=self.max_epochs,
            early_stop_patience=self.early_stop_patience,
            train_loss=float(train_loss),
            val_loss=float(val_loss),
            train_accuracy=float(train_acc),
            val_accuracy=float(val_acc),
            best_epoch=best_epoch,
            notes=note,
            epoch_history_json=json.dumps(
                {
                    "train_curve": train_curve,
                    "val_curve": val_curve,
                    "train_acc_curve": train_acc_curve,
                    "val_acc_curve": val_acc_curve,
                    "class_metrics": class_metrics,
                    "confusion_matrix": confusion_matrix,
                    "labels": list(CLASS_LABELS),
                    "macro_f1": macro_f1,
                },
                ensure_ascii=False,
            ),
        )
        self.score_unscored_rss_events(limit=5000)
        self.sync_fetched_csv_from_db(overwrite=True)
        return TrainingResult(
            model_version=model_version,
            sample_count=sample_count,
            train_loss=float(train_loss),
            val_loss=float(val_loss),
            train_accuracy=float(train_acc),
            val_accuracy=float(val_acc),
            best_epoch=best_epoch,
            max_epochs=self.max_epochs,
            learning_rate=self.learning_rate,
            early_stop_patience=self.early_stop_patience,
            note=note,
            train_curve=train_curve,
            val_curve=val_curve,
            train_acc_curve=train_acc_curve,
            val_acc_curve=val_acc_curve,
            class_metrics=class_metrics,
            confusion_matrix=confusion_matrix,
            labels=list(CLASS_LABELS),
            macro_f1=macro_f1,
        )

    def _async_train_worker(self, *, force: bool, min_samples_override: int | None) -> None:
        try:
            self.reload_runtime_config()
            features, targets, time_weights, sample_count, _ = self._build_dataset()
            min_samples = self.min_train_samples if min_samples_override is None else max(10, int(min_samples_override))
            if sample_count < min_samples:
                with self.runtime_lock:
                    self.runtime.update(
                        {
                            "running": False,
                            "state": "idle",
                            "cancel_requested": False,
                            "message": f"样本不足：{sample_count}/{min_samples}",
                            "finished_at": datetime.now(CN_TZ).isoformat(),
                        }
                    )
                return

            model = FiveLayerClassifier(
                input_dim=self.text_feature_dim,
                hidden_dims=list(self.hidden_dims),
                class_count=4,
                seed=42 + sample_count,
            )
            with self.runtime_lock:
                self.runtime.update(
                    {
                        "running": True,
                        "paused": False,
                        "cancelled": False,
                        "cancel_requested": False,
                        "state": "running",
                        "message": "训练开始",
                        "sample_count": sample_count,
                        "max_epochs": self.max_epochs,
                        "epoch": 0,
                        "train_curve": [],
                        "val_curve": [],
                        "train_acc_curve": [],
                        "val_acc_curve": [],
                        "error": None,
                    }
                )

            train_loss, val_loss, train_acc, val_acc, best_epoch, train_curve, val_curve, train_acc_curve, val_acc_curve = model.train(
                features,
                targets,
                learning_rate=self.learning_rate,
                max_epochs=self.max_epochs,
                early_stop_patience=self.early_stop_patience,
                validation_ratio=0.2,
                sample_weights=time_weights,
                use_class_weight=self.use_class_weight,
                use_resample=self.use_resample,
                check_pause_cancel=self._check_pause_cancel,
                on_epoch_end=self._on_epoch_end,
            )

            trained_at = datetime.now(CN_TZ).isoformat()
            model_version = f"ml-cls-manual-{trained_at.replace(':', '').replace('-', '')}"
            self.model = model
            self.model_version = model_version
            self._save_model(trained_at=trained_at)
            class_metrics, confusion_matrix = self._calc_class_metrics(model, features, targets)
            macro_f1 = self._macro_f1(class_metrics)
            note = "标题单列神经网络四类聚类（手动训练）"
            self.db.insert_rss_ml_training_run(
                trained_at=trained_at,
                sample_count=sample_count,
                model_version=model_version,
                learning_rate=self.learning_rate,
                max_epochs=self.max_epochs,
                early_stop_patience=self.early_stop_patience,
                train_loss=float(train_loss),
                val_loss=float(val_loss),
                train_accuracy=float(train_acc),
                val_accuracy=float(val_acc),
                best_epoch=best_epoch,
                notes=note,
                epoch_history_json=json.dumps(
                    {
                        "train_curve": train_curve,
                        "val_curve": val_curve,
                        "train_acc_curve": train_acc_curve,
                        "val_acc_curve": val_acc_curve,
                        "class_metrics": class_metrics,
                        "confusion_matrix": confusion_matrix,
                        "labels": list(CLASS_LABELS),
                        "macro_f1": macro_f1,
                    },
                    ensure_ascii=False,
                ),
            )
            self.score_unscored_rss_events(limit=5000)
            self.sync_fetched_csv_from_db(overwrite=True)
            with self.runtime_lock:
                self.runtime.update(
                    {
                        "running": False,
                        "state": "completed",
                        "cancel_requested": False,
                        "message": f"训练完成：Macro-F1={macro_f1 * 100:.2f}%",
                        "finished_at": trained_at,
                    }
                )
        except Exception as exc:
            details = self.format_exception_details(exc)
            with self.runtime_lock:
                self.runtime.update(
                    {
                        "running": False,
                        "state": "cancelled" if "cancelled" in str(exc).lower() else "failed",
                        "cancelled": "cancelled" in str(exc).lower(),
                        "cancel_requested": False,
                        "message": str(exc),
                        "error": details,
                        "finished_at": datetime.now(CN_TZ).isoformat(),
                    }
                )
        finally:
            self.pause_event.clear()
            self.cancel_event.clear()

    def start_training_async(self, *, force: bool = True, min_samples_override: int | None = None) -> dict[str, Any]:
        if self.train_thread and self.train_thread.is_alive():
            return {"started": False, "message": "已有训练任务在运行中"}
        csv_db_sync = self.sync_db_from_csv()
        sync_msg = (
            f"CSV->DB 同步: {csv_db_sync.get('synced_samples', 0)} 条"
            if csv_db_sync.get("ok")
            else f"CSV->DB 同步跳过: {csv_db_sync.get('reason', 'unknown')}"
        )
        with self.runtime_lock:
            self.runtime.update(
                {
                    "running": True,
                    "paused": False,
                    "cancelled": False,
                    "cancel_requested": False,
                    "state": "starting",
                    "message": f"任务已启动；{sync_msg}",
                    "started_at": datetime.now(CN_TZ).isoformat(),
                    "finished_at": None,
                    "epoch": 0,
                    "train_curve": [],
                    "val_curve": [],
                    "train_acc_curve": [],
                    "val_acc_curve": [],
                    "error": None,
                }
            )
        self.pause_event.clear()
        self.cancel_event.clear()
        self.train_thread = threading.Thread(
            target=self._async_train_worker,
            kwargs={"force": force, "min_samples_override": min_samples_override},
            daemon=True,
        )
        self.train_thread.start()
        return {"started": True, "message": f"训练任务已启动；{sync_msg}", "csv_db_sync": csv_db_sync}

    def control_training(self, action: str) -> dict[str, Any]:
        action = (action or "").strip().lower()
        if not self.train_thread or not self.train_thread.is_alive():
            return {"ok": False, "message": "当前没有运行中的训练任务"}
        if action == "pause":
            self.pause_event.set()
            with self.runtime_lock:
                self.runtime["state"] = "pausing"
                self.runtime["message"] = "收到暂停请求"
            return {"ok": True, "message": "已发送暂停请求"}
        if action == "resume":
            self.pause_event.clear()
            with self.runtime_lock:
                self.runtime["state"] = "running"
                self.runtime["message"] = "已恢复训练"
            return {"ok": True, "message": "已恢复训练"}
        if action == "cancel":
            self.cancel_event.set()
            self.pause_event.clear()
            with self.runtime_lock:
                self.runtime["cancel_requested"] = True
                self.runtime["state"] = "cancelling"
                self.runtime["message"] = "收到取消请求"
            return {"ok": True, "message": "已发送取消请求"}
        return {"ok": False, "message": f"未知控制动作: {action}"}

    def get_live_status(self) -> dict[str, Any]:
        with self.runtime_lock:
            runtime = dict(self.runtime)
        return runtime

    def predict_score(
        self,
        *,
        event_text: str,
        gold_price_usd_per_oz: float | None,
        gold_change_pct: float | None,
    ) -> tuple[float, str, str, dict[str, float]] | None:
        del gold_price_usd_per_oz
        del gold_change_pct
        if self.model is None or not self.model_version:
            return None
        vector = self._build_feature_vector(event_text)
        probs = self.model.predict_proba(vector)
        pred_idx = max(range(len(probs)), key=lambda i: probs[i])
        label = CLASS_LABELS[pred_idx]
        score = sum(p * s for p, s in zip(probs, CLASS_SCORE_CENTER, strict=False))
        prob_map = {CLASS_LABELS[i]: float(round(probs[i], 6)) for i in range(len(CLASS_LABELS))}
        return float(round(score, 2)), self.model_version, label, prob_map

    def get_status(self) -> dict[str, Any]:
        self.reload_runtime_config()
        db_sample_count = self.db.get_rss_ml_sample_count()
        rss_event_count = self.db.get_rss_event_count()
        rss_scored_event_count = self.db.get_rss_scored_event_count()
        _, targets, _, sample_count, dataset_source = self._build_dataset()
        training_csv_rows = self._count_csv_rows(self.dataset_csv_path) if self.dataset_csv_path.exists() else 0
        fetched_csv_rows = self._count_csv_rows(self.fetched_csv_path) if self.fetched_csv_path.exists() else 0
        class_distribution = {label: 0 for label in CLASS_LABELS}
        for cls in targets:
            class_distribution[CLASS_LABELS[int(cls)]] += 1
        latest = self.db.get_latest_rss_ml_training_run()
        recent = self.db.get_recent_rss_ml_training_runs(limit=30)
        latest_epoch_history = {}
        if latest and latest.get("epoch_history_json"):
            try:
                latest_epoch_history = json.loads(str(latest.get("epoch_history_json")))
            except json.JSONDecodeError:
                latest_epoch_history = {}
        latest_macro_f1 = 0.0
        metrics = latest_epoch_history.get("class_metrics", []) if isinstance(latest_epoch_history, dict) else []
        if isinstance(metrics, list) and metrics:
            latest_macro_f1 = self._macro_f1(metrics)
        next_auto_train_at = (
            ((sample_count // self.train_step_size) + 1) * self.train_step_size
            if sample_count >= self.train_step_size
            else self.train_step_size
        )
        return {
            "config": {
                "learning_rate": self.learning_rate,
                "max_epochs": self.max_epochs,
                "early_stop_patience": self.early_stop_patience,
                "train_step_size": self.train_step_size,
                "min_train_samples": self.min_train_samples,
                "hidden_layers": list(self.hidden_dims),
                "feature_columns": ["title"],
                "cluster_labels": list(CLASS_LABELS),
                "active_window_hours": self.active_window_hours,
                "weak_move_pct": self.weak_move_pct,
                "strong_move_pct": self.strong_move_pct,
                "decay_half_life_hours": self.decay_half_life_hours,
                "label_mode": self.label_mode,
                "window_options_hours": [1, 4, 24],
                "use_class_weight": self.use_class_weight,
                "use_resample": self.use_resample,
            },
            "sample_count": sample_count,
            "training_sample_count": sample_count,
            "db_sample_count": db_sample_count,
            "rss_event_count": rss_event_count,
            "rss_scored_event_count": rss_scored_event_count,
            "train_data_source": dataset_source,
            "dataset_csv_path": str(self.dataset_csv_path),
            "dataset_csv_exists": self.dataset_csv_path.exists(),
            "dataset_csv_rows": training_csv_rows,
            "dataset_csv_mtime": (
                datetime.fromtimestamp(self.dataset_csv_path.stat().st_mtime, tz=CN_TZ).isoformat()
                if self.dataset_csv_path.exists()
                else None
            ),
            "fetched_csv_path": str(self.fetched_csv_path),
            "fetched_csv_exists": self.fetched_csv_path.exists(),
            "fetched_csv_rows": fetched_csv_rows,
            "fetched_csv_mtime": (
                datetime.fromtimestamp(self.fetched_csv_path.stat().st_mtime, tz=CN_TZ).isoformat()
                if self.fetched_csv_path.exists()
                else None
            ),
            "model_loaded": bool(self.model),
            "model_version": self.model_version or None,
            "latest_training_run": latest,
            "latest_epoch_history": latest_epoch_history,
            "latest_macro_f1": latest_macro_f1,
            "recent_training_runs": recent,
            "next_auto_train_at": next_auto_train_at,
            "class_distribution": class_distribution,
            "last_csv_db_sync": self.last_csv_db_sync,
            "last_fetch_csv_sync": self.last_fetch_csv_sync,
            "last_event_score_backfill": self.last_event_score_backfill,
            "runtime": self.get_live_status(),
        }

    def clear_samples(self, *, remove_model_file: bool = True) -> dict[str, Any]:
        if self.train_thread and self.train_thread.is_alive():
            return {"error": "training_running"}
        cleared = self.db.clear_rss_ml_samples()
        self.model = None
        self.model_version = ""
        removed_model_file = False
        if remove_model_file and self.model_path.exists():
            try:
                self.model_path.unlink()
                removed_model_file = True
            except Exception:
                removed_model_file = False
        return {**cleared, "removed_model_file": removed_model_file}

    @staticmethod
    def format_exception_details(exc: Exception) -> dict[str, Any]:
        tb = traceback.format_exc()
        lines = [line for line in tb.splitlines() if line.strip()]
        return {
            "error_type": exc.__class__.__name__,
            "error_message": str(exc),
            "traceback_tail": lines[-14:],
        }

    def _save_model(self, *, trained_at: str) -> None:
        if self.model is None:
            return
        payload = {
            "trained_at": trained_at,
            "model_version": self.model_version,
            "text_feature_dim": self.text_feature_dim,
            "learning_rate": self.learning_rate,
            "max_epochs": self.max_epochs,
            "early_stop_patience": self.early_stop_patience,
            "class_labels": list(CLASS_LABELS),
            "model": self.model.to_dict(),
        }
        self.model_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def _load_model(self) -> None:
        if not self.model_path.exists():
            return
        try:
            payload = json.loads(self.model_path.read_text(encoding="utf-8"))
            self.text_feature_dim = int(payload.get("text_feature_dim", self.text_feature_dim))
            self.model_version = str(payload.get("model_version") or "")
            model_payload = payload.get("model")
            if isinstance(model_payload, dict):
                self.model = FiveLayerClassifier.from_dict(model_payload)
        except Exception:
            self.model = None
            self.model_version = ""

