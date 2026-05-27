"""
Small JSON state helper for runtime files.

Writes are atomic: data is written to a temporary file, flushed, and then
replaces the target file. If a state file is corrupted, the bad file is kept as
``*.corrupt`` and callers get their default value instead of crashing the bot.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

log = logging.getLogger("Scout.StateStore")


def read_json(path: str, default: Any) -> Any:
    state_path = Path(path)
    try:
        with state_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except json.JSONDecodeError as e:
        _backup_corrupt_file(state_path)
        log.warning(f"Korruptoitunut state-tiedosto {path}: {e}; käytetään oletusta")
        return default
    except Exception as e:
        log.warning(f"State-tiedoston luku epäonnistui {path}: {e}")
        return default


def write_json(path: str, data: Any, *, indent: int | None = None) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = None
    try:
        with NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=str(state_path.parent or Path(".")),
            delete=False,
        ) as tmp:
            tmp_name = tmp.name
            json.dump(data, tmp, indent=indent)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp_name, state_path)
    except Exception:
        if tmp_name:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
        raise


def _backup_corrupt_file(path: Path) -> None:
    try:
        if path.exists():
            backup = path.with_suffix(path.suffix + ".corrupt")
            os.replace(path, backup)
    except Exception as e:
        log.debug(f"Korruptoituneen state-tiedoston backup epäonnistui {path}: {e}")
