# Exports

What I could pull out of Foundry on November 6, 2026.

| File | How I got it | What's in it |
| --- | --- | --- |
| [ontology-project.json](ontology-project.json) | Ontology Manager > Export, trimmed to this project | 4 object types, 74 properties, 4 links, 6 Actions |
| [signal-rules-java/](signal-rules-java/) | Pipeline Builder > Settings > Export code on the Signal rules pipeline | Generated Java for recall matching, scoring, grouping, and outputs |
| [complaint-extraction-java/](complaint-extraction-java/) | Same export on the Complaint extraction pipeline | Generated Java for the extraction cleanup and outputs |

I exported the pipelines into a separate Foundry code repository (one branch per pipeline) so the real pipelines weren't changed, then copied the Java over as is.

## Limits

- **The Java won't run on its own.** Foundry can't generate code for the LLM steps and a few other operations, so it puts in `unsupportedTransform(...)` and `unsupportedExpression(...)` placeholders that throw an error. I left those as generated instead of writing my own replacements. The full prompts for the LLM steps are in [prompts/](../prompts/README.md). Palantir's [export docs](https://www.palantir.com/docs/foundry/pipeline-builder/export-pipeline) explain what can and can't be exported.
- **No data.** The pipelines read Foundry datasets, which aren't in this repo.
- **The ontology file is the schema only.** I haven't tried importing it into another Foundry environment.
- **Workshop, AIP Logic, and Automate don't export to code**, so those are covered by the prompts and the [logic/](../logic/README.md) folder.
