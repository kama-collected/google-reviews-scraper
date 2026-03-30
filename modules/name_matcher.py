"""
Doctor name matching utilities for the Supabase testimonials pipeline.

Two entry points
----------------
find_matching_doctor()
    Match pre-extracted name *candidates* against a doctor roster using a
    two-step exact → WRatio fuzzy strategy. Use this when candidate names have
    already been pulled from a review (e.g. via an NER step).

find_all_doctors_in_review()
    Scan a full review text for *all* doctor names using partial_ratio substring
    matching. Returns every doctor whose normalised name scores above the
    threshold. Use this when only the raw review text is available.

Title normalisation
-------------------
Strips Malaysian / regional honorifics and common medical prefixes before any
comparison, so "Dato' Dr. Chan Wei Ming" matches "Chan Wei Ming" and
"Dr. Ahmad" matches "Ahmad".
"""

import re
import logging
from typing import Optional

from rapidfuzz import fuzz, process

log = logging.getLogger("scraper")

# ---------------------------------------------------------------------------
# Title prefix stripping
# ---------------------------------------------------------------------------

# Handles chains of known titles, e.g. "Tan Sri Datuk Dr", "Prof. Dr.",
# "Assoc. Prof. Dr.", "Datin Dr."
_TITLE_PREFIX = re.compile(
    r"^(?:"
    r"(?:tan\s+sri|tun)\s+"
    r"|(?:datuk\s+seri|datuk)\s+"
    r"|(?:dato'\s+seri|dato'?\s+seri|dato'?)\s+"
    r"|datin\s+"
    r"|associate\s+professor\s+"
    r"|assoc\.?\s+prof\.?\s+"
    r"|asst\.?\s+prof\.?\s+"
    r"|a\/prof\.?\s+"
    r"|professor\s+"
    r"|prof\.?\s+"
    r"|dr\.?\s+"
    r"|mr\.?\s+"
    r"|mrs\.?\s+"
    r"|ms\.?\s+"
    r")+",
    re.IGNORECASE,
)


def _normalise(name: str) -> str:
    """Lowercase, strip title prefixes, and collapse whitespace."""
    stripped = _TITLE_PREFIX.sub("", name).strip()
    return " ".join(stripped.lower().split())


# ---------------------------------------------------------------------------
# Candidate-based matching (pre-extracted names)
# ---------------------------------------------------------------------------

def find_matching_doctor(
    candidates: list[str],
    doctors: list[dict],
    threshold: int = 85,
) -> "tuple[str, str, Optional[float]] | None":
    """
    Match a list of extracted candidate names against a doctor roster.

    Strategy
    --------
    1. Exact match (case-insensitive, title-stripped, whitespace-normalised)
       — match_score returned as None.
    2. Fuzzy match via rapidfuzz WRatio above *threshold*
       — match_score returned as a float (0–100).

    This function is best suited for comparing two short strings of similar
    length (extracted name vs. doctor name). For matching against full review
    text use :func:`find_all_doctors_in_review` instead.

    Args:
        candidates: Name strings extracted from a review.
        doctors:    Doctor dicts from Supabase; each must have ``id`` and ``name``.
        threshold:  Minimum WRatio score (0–100) for a fuzzy match.

    Returns:
        ``(doctor_id, matched_name, match_score)`` for the first match found,
        or ``None`` if no candidate matches any doctor.
    """
    if not candidates or not doctors:
        return None

    doctor_names: list[str] = [d["name"] for d in doctors]
    normalised_to_doctor: dict[str, dict] = {
        _normalise(d["name"]): d for d in doctors
    }

    for candidate in candidates:
        norm_candidate = _normalise(candidate)

        # Step 1: exact match
        if norm_candidate in normalised_to_doctor:
            doctor = normalised_to_doctor[norm_candidate]
            log.debug(
                "Exact match: %r → %r (id=%s)",
                candidate, doctor["name"], doctor["id"],
            )
            return (doctor["id"], doctor["name"], None)

        # Step 2: fuzzy match (WRatio — best for comparing two short strings)
        result = process.extractOne(
            norm_candidate,
            [_normalise(n) for n in doctor_names],
            scorer=fuzz.WRatio,
            score_cutoff=threshold,
        )
        if result:
            _, score, idx = result
            doctor = doctors[idx]
            log.debug(
                "Fuzzy match: %r → %r (score=%.1f, id=%s)",
                candidate, doctor["name"], score, doctor["id"],
            )
            return (doctor["id"], doctor["name"], float(score))

    return None


# ---------------------------------------------------------------------------
# Full-text scanning (raw review text)
# ---------------------------------------------------------------------------

def find_all_doctors_in_review(
    review_text: str,
    doctors: list[dict],
    threshold: int = 85,
) -> "list[tuple[str, str, int]]":
    """
    Find every doctor whose name appears in *review_text*.

    Uses ``partial_ratio`` (substring matching), which is well-suited for
    detecting whether a short name string appears within a longer text. Doctor
    names are normalised (titles stripped) before matching; the review text is
    lowercased.

    Args:
        review_text: Full plain-text content of a review.
        doctors:     Doctor dicts from Supabase; each must have ``id`` and ``name``.
        threshold:   Minimum ``partial_ratio`` score (0–100) for a match.

    Returns:
        List of ``(doctor_id, doctor_name, score)`` tuples for every doctor
        that matched, sorted by score descending. Empty list if no matches.
    """
    if not review_text or not doctors:
        return []

    text_lower = review_text.lower()
    matches: list[tuple[str, str, int]] = []

    for doctor in doctors:
        raw_name = doctor.get("name", "")
        if not raw_name:
            continue

        norm_name = _normalise(raw_name)
        if not norm_name:
            continue

        # Skip names too short to match reliably — avoids single letters and
        # short tokens (e.g. "goon", "sook") matching common English words.
        if len(norm_name) < 5:
            log.debug(
                "Skipping doctor %r — normalised name too short: %r",
                raw_name, norm_name,
            )
            continue

        score = fuzz.partial_ratio(norm_name, text_lower)
        if score >= threshold:
            # Whole-word guard: at least one name token must appear as a
            # standalone word — prevents "goon" matching "good", etc.
            name_tokens = norm_name.split()
            if not any(
                re.search(r"\b" + re.escape(tok) + r"\b", text_lower)
                for tok in name_tokens
            ):
                continue
            log.debug(
                "Text match: doctor %r in review (score=%d, id=%s)",
                raw_name, score, doctor["id"],
            )
            matches.append((doctor["id"], raw_name, score))

    matches.sort(key=lambda t: t[2], reverse=True)
    return matches
