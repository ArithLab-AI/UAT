def normalize_priority(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    return normalized or None


def priority_score_boost(priority: str | None) -> int:
    normalized = normalize_priority(priority)
    if normalized == "high":
        return 5
    if normalized == "medium":
        return 3
    if normalized == "low":
        return 1
    return 1 if normalized else 0


def coalesce_priority_clean_quality_score(
    current_score: int,
    *,
    raw_score: int | None,
    previous_clean_score: int | None,
    changes_detected: bool,
    suggestion_priority: str | None,
) -> int:
    baseline_score = max(
        current_score,
        raw_score if raw_score is not None else current_score,
    )
    if previous_clean_score is not None and previous_clean_score <= baseline_score:
        baseline_score = max(baseline_score, previous_clean_score)
    if not changes_detected or previous_clean_score is not None:
        return min(100, baseline_score)

    boost = priority_score_boost(suggestion_priority)
    if boost <= 0:
        return min(100, baseline_score)

    return min(100, baseline_score + boost)


def cap_quality_score_by_suggestions(
    current_score: int,
    *,
    suggestions: list,
) -> int:
    if not suggestions:
        return max(0, min(100, current_score))

    priority_penalties = {
        "high": 6,
        "medium": 3,
        "low": 1,
    }
    penalty = 0
    for suggestion in suggestions[:12]:
        priority = getattr(suggestion, "priority", None)
        normalized = normalize_priority(priority)
        penalty += priority_penalties.get(normalized or "", 2)

    max_allowed_score = max(0, 100 - penalty)
    return max(0, min(100, min(current_score, max_allowed_score)))
