"""Gulshan cafe data pipeline.

Collects, normalises and consolidates cafe data from multiple platforms
(Google Maps first) into one platform-tagged record per cafe, following the
M-1..M-5 source-tagging scheme. See README.md for the full workflow.
"""

__version__ = "0.1.0"

# Platform identifiers and source tags used across the pipeline.
SOURCE_GOOGLE_MAPS = "GOOGLE_MAPS"
TAG_M1 = "M-1"
