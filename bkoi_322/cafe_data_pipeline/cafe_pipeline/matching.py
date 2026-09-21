"""Seed -> Google Maps match classification.

A candidate place found on Google Maps is only merged into the cafe record
when the evidence is good enough; otherwise the record keeps its seed data
and a ``low`` confidence instead of a wrong merge.
"""

from __future__ import annotations

from .settings import Settings

HIGH = "high"
MEDIUM = "medium"
LOW = "low"


def classify_match(name_score: float, distance_m: float | None,
                   cfg: Settings) -> tuple[str, str]:
    """Returns (confidence, status).

    status is one of:
      ok           -- usable match (confidence high or medium)
      no_match     -- signals too weak; keep the record, do not merge
      geo_mismatch -- the place Google returned is simply elsewhere
    """
    tol = float(cfg.matching.geo_tolerance_m)
    far = distance_m is None or distance_m > tol
    if far:
        return LOW, ("no_match" if distance_m is None else "geo_mismatch")

    high, medium = cfg.matching.high, cfg.matching.medium
    if name_score >= high.name and distance_m <= high.dist_m:
        return HIGH, "ok"
    if name_score >= medium.name and distance_m <= medium.dist_m:
        return MEDIUM, "ok"
    if name_score < float(cfg.matching.reject_name_below):
        return LOW, "geo_mismatch"
    return LOW, "ok"
