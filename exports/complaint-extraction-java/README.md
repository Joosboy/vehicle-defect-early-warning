# Complaint extraction pipeline (generated Java)

Exported from the Complaint extraction pipeline (`ri.eddie.main.pipeline.a668c908-2c2f-4d47-84f7-67a32ac42ffb`, Main branch) with Pipeline Builder's Export code on November 6, 2026.

- [PipelineLogic.java](PipelineLogic.java): the steps around the LLM calls
- [PipelineOutputs.java](PipelineOutputs.java): the two outputs

## What it does

It takes `complaints` and `recalls` and has two LLM steps:

| LLM step | Output |
| --- | --- |
| `extractComplaintFactsAip` | `complaintsEnriched`: each complaint with its seven extracted fields, plus `llm_error` |
| `tagRecallsAip` | `datasetOutput`: recalls with their component and failure-mode tags |

After extraction, it:

- Sets severity to 4 if NHTSA's structured data shows a crash, fire, injury, or death, so the LLM can't under-rate a report with real harm. Otherwise it keeps the LLM's severity, or uses 1 if it's missing.
- Turns a mileage of 0 (what the prompt uses for "not stated") into null.
- Drops the temporary `extraction` struct column.

[extraction-normalization.json](../../logic/extraction-normalization.json) has the same thing in JSON.

## Placeholders

Both LLM steps and the null-value expression for mileage come out as placeholders that throw an error, so this won't run as is. The extraction prompt and its settings are in [prompts/](../../prompts/README.md). This pipeline and the Signal rules export use the same Java package and class names, so they're two separate snapshots, not one project you can compile together.
