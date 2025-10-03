import logging
import json
import sys
import os
from pathlib import Path
from uuid import UUID
from decimal import Decimal
from datetime import datetime, timezone, date, time
from typing import Any


LOG_MAP = {"DEBUG": logging.DEBUG,
           "INFO": logging.INFO,
           "WARNING": logging.WARNING,
           "WARN": logging.WARNING,
           "ERROR": logging.ERROR}


type JSONScalar = str | int | float | bool | None
type JSON = JSONScalar | list[JSON] | dict[str, JSON]


def time_now_iso() -> str:
    return (datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"))


def coerce_val(val: Any) -> JSON:
    if isinstance(val, (tuple, set)):
        return coerce_val(list(val))
    if isinstance(val, list):
        return [coerce_val(e) for e in val]
    if isinstance(val, dict):
        return {str(k): coerce_val(v) for k, v in val.items()}
    if isinstance(val, (str, int, float, bool)):
        return val
    if val is None:
        return None
    if isinstance(val, (Path, UUID, Decimal)):
        return str(val)
    if isinstance(val, datetime):
        dt = val
        if val.tzinfo is None:
            dt = val.replace(tzinfo=timezone.utc)
        dt = (dt.astimezone(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"))
        return dt
    if isinstance(val, (date, time)):
        return val.isoformat()

    else:
        return str(val)


class JsonFormatter(logging.Formatter):
    LEVEL_MAP = {"WARNING": "WARN"}

    def format(self, rec: logging.LogRecord) -> str:
        level = self.LEVEL_MAP.get(rec.levelname, rec.levelname)
        obj: dict[str, JSON] = {"ts": time_now_iso(),
                               "level": level}
        if isinstance(rec.msg, dict):
            if rec.msg:
                obj.update(rec.msg)
                if "event" not in rec.msg:
                    obj["event"] = "message"
                    obj["message"] = "<payload-without-event>"
            else:
                obj["event"] = "message"
                obj["message"] = "<dict-without-event>"
        else:
            obj["event"] = "message"
            obj["message"] = rec.getMessage()

        run_id = getattr(rec, "run_id", None)
        component = getattr(rec, "component", None)
        payload = getattr(rec, "payload", None)
        if run_id:    obj["run_id"] = run_id
        if component: obj["component"] = component
        if isinstance(payload, dict):
            obj.update(payload)

        obj = coerce_val(obj)
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def get_json_logger(name: str, level: str | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    env_lvl = os.getenv("LOG_LEVEL", "INFO").upper()
    if level is None:
        lvl = LOG_MAP.get(env_lvl, logging.INFO)
    else: lvl = LOG_MAP.get(level.upper(), logging.INFO)
    logger.setLevel(lvl)

    if getattr(logger, "_json_inited", False):
        for h in logger.handlers:
            if getattr(h, "_is_json_stdout", False):
                h.setLevel(lvl)
        logger.propagate = False

        return logger

    logger.propagate = False
    if not any(isinstance(h, logging.StreamHandler)
               and getattr(h, "_is_json_stdout", False)
               for h in logger.handlers):
        h = logging.StreamHandler(sys.stdout)
        h.setLevel(lvl)
        h.setFormatter(JsonFormatter())
        h._is_json_stdout = True
        logger.addHandler(h)
    logger._json_inited = True

    return logger


def make_emit(logger: logging.Logger, run_id: str, component: str):
    def emit(level: str = "INFO", event: str = "message", **payload) -> None:
        lvl = LOG_MAP.get(level.upper(), logging.INFO)
        event = event if event.strip() else "message"
        extra = {
                       "run_id": run_id,
                       "component": component,
                       "payload": {"event": event, **payload}
                }
        logger.log(lvl, {}, extra=extra)

    return emit
