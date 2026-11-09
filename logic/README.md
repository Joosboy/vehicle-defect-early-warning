# Rules

This is the rules side of the Signal rules pipeline, written out so you don't have to dig through the [generated Java](../exports/signal-rules-java/PipelineLogic.java). Each section links to the line where that logic starts. [review-rules.json](review-rules.json) has the same rules in JSON.

## Recall matching

The pipeline takes four inputs: the enriched complaints, the recalls, the vehicles, and a join table of which recall covers which vehicle. First an LLM step tags each recall with a component and failure mode ([prompt](../prompts/recall-tagging-system-prompt.txt)). Then:

1. Join the recall-vehicle table to vehicles, then to the tagged recalls.
2. Group by `(vehicle_id, recall_failure_mode)` to get `covered_campaign` and `covered_since` (earliest campaign and date).
3. Left-join each complaint on `vehicle_id` and `failure_mode`.
4. Do the same at the model level, grouping by `(model, recall_failure_mode)` to get `related_campaign` and `related_since`.
5. Set `recall_status`: **Covered** if there's a vehicle-level match, **Possible scope gap** if there's only a model-level match, otherwise **Not covered**.
6. `matched_campaign` is the covered campaign if there is one, otherwise the related one.

[Coverage](../exports/signal-rules-java/PipelineLogic.java#L237) · [vehicle level](../exports/signal-rules-java/PipelineLogic.java#L303) · [model level](../exports/signal-rules-java/PipelineLogic.java#L432) · [status](../exports/signal-rules-java/PipelineLogic.java#L541)

## Which complaints become signals

```text
IF recall_status = "Covered"   THEN null
ELSE IF failure_mode = "other" THEN null
ELSE IF failure_mode IS NULL   THEN null
ELSE concat_ws("|", model, failure_mode)
```

Rows with a null `signal_id` are left out, so a signal like `ACCORD|unexpected_braking` only counts complaints that a recall doesn't already cover. [Java](../exports/signal-rules-java/PipelineLogic.java#L550)

## Severity weights

| Severity | Weight |
| --- | ---: |
| 4 | 15 |
| 3 | 5 |
| 2 | 2 |
| anything else | 1 |

Severity comes from the [extraction prompt](../prompts/extraction-system-prompt.txt): 4 for a crash, fire, injury, or death; 3 for losing power, braking, steering, or an airbag function while moving; 2 for warning lights or reduced function; 1 for everything else. The extraction pipeline also bumps severity to 4 whenever NHTSA's structured crash, fire, injury, or death fields say so, in case the LLM missed it ([extraction-normalization.json](extraction-normalization.json)).

I weighted severity 4 much more heavily on purpose. One crash report should matter more than several warning-light complaints. [Java](../exports/signal-rules-java/PipelineLogic.java#L579)

## Signal totals

Complaints are grouped by `signal_id`, `make`, `model`, `failure_mode`, `recall_status`, and `matched_campaign`.

| Field | Calculation |
| --- | --- |
| `complaint_count` | `count(complaint_id)` |
| `severity_score` | `sum(severity_weight)` |
| `serious_count` | complaints with severity 3 or 4 |
| `crash_or_fire_count` | complaints where `crash` or `fire` is true |
| `injury_count` / `death_count` | `sum(injuries)` / `sum(deaths)` |
| `first_seen` / `last_seen` | earliest / latest `date_filed` |
| `model_years` | distinct model years, sorted, comma-separated |

[Java](../exports/signal-rules-java/PipelineLogic.java#L604)

## Review status

```text
IF severity_score >= 30 AND complaint_count >= 5
THEN "Needs review"
ELSE "Monitoring"
```

Both conditions have to hold. Without the complaint minimum, two crash reports (30 points) would be enough on their own. [Java](../exports/signal-rules-java/PipelineLogic.java#L900)

## Above threshold since

For each signal, the pipeline orders its complaints by filing date (then complaint ID), keeps a running count and running score, and records the first date where both thresholds are met. That's the "Above threshold since" date in the app. It's done as a self-join on `signal_id`. [Java](../exports/signal-rules-java/PipelineLogic.java#L1003)

This uses the recalls as they exist today, so it isn't a true historical backtest yet. See [evaluation.md](../docs/evaluation.md).

## Evidence sample

Each signal keeps its top 8 complaints as evidence lines (complaint ID, date, model year, severity, and quote), highest severity first. That's what the AIP Logic function reads when it drafts a proposal. [Java](../exports/signal-rules-java/PipelineLogic.java#L671)

## Spot check

`spot_check = complaint_id % 80 == 0`. It's a simple, repeatable way to pull a sample of complaints for checking the extraction by hand. [Java](../exports/signal-rules-java/PipelineLogic.java#L586)

## Proposal writeback and automation

- [save-proposal-mapping.json](save-proposal-mapping.json): how the AIP Logic output maps to the Signal's AIP fields. The Save proposal step never touches the decision fields.
- [automation-configuration.json](automation-configuration.json): the Automate rule that drafts a proposal for every new signal that hits Needs review.

Getting into the review queue doesn't mean an investigation gets opened. That's a separate call, first by AIP and then by the engineer.
