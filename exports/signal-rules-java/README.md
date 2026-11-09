# Signal rules pipeline (generated Java)

Exported from the Signal rules pipeline (`ri.eddie.main.pipeline.c8a80a1a-6dd1-4693-8487-2fbab5249187`, Main branch) with Pipeline Builder's Export code on November 6, 2026.

- [PipelineLogic.java](PipelineLogic.java): all the joins, rules, and aggregations
- [PipelineOutputs.java](PipelineOutputs.java): the four outputs (Recall, Vehicle, Signal, Complaint)

This is the code version of the rules in [logic/README.md](../../logic/README.md), which walks through it section by section with line numbers. That's the easier place to start.

## Placeholders

Foundry couldn't generate these, so they're placeholders that throw an error:

| Placeholder | What it stands in for |
| --- | --- |
| `unsupportedTransform("tagRecallsAip")` | The LLM step that tags recalls ([prompt](../../prompts/recall-tagging-system-prompt.txt)) |
| `unsupportedTransform("windowedProject")` | Ranking each signal's evidence complaints |
| `unsupportedExpression("regexReplace")` | Part of formatting `signal_name` |
| `unsupportedExpression("createNullValue")` | Setting 10 fields (proposal, decision, and timestamps) to null at the start |

Because the recall-tagging step is the first thing `transform(...)` calls, the file won't run as is. It also needs Spark, and there's no build setup here. It's meant to be read, not run.
