# Prompts

The three LLM prompts in the app, copied from Foundry as they are, with their settings. In the input templates, `{{...}}` marks where Foundry fills in a field.

| Step | Prompt | Input | Settings |
| --- | --- | --- | --- |
| Extract complaint facts (Complaint extraction pipeline) | [extraction-system-prompt.txt](extraction-system-prompt.txt) | [extraction-input-template.txt](extraction-input-template.txt) | [extraction-output-configuration.json](extraction-output-configuration.json) |
| Tag recalls (Signal rules pipeline) | [recall-tagging-system-prompt.txt](recall-tagging-system-prompt.txt) | [recall-tagging-input-template.txt](recall-tagging-input-template.txt) | [recall-tagging-output-configuration.json](recall-tagging-output-configuration.json) |
| Draft investigation proposal (AIP Logic) | [proposal-system-prompt.txt](proposal-system-prompt.txt) | [proposal-input-template.txt](proposal-input-template.txt) | [proposal-output-configuration.json](proposal-output-configuration.json) |

## Extraction

GPT-4.1 mini, temperature 0, structured output. It gets the complaint text and NHTSA's component tags, and returns the component, one of 24 failure modes, severity 1 to 4, whether it happened while driving, mileage, whether the owner mentions a recall, and a word-for-word quote under 40 words. It's told to use only what the owner wrote and never guess. "Include errors" is on, so a failed row gets an error message instead of breaking the build.

## Recall tagging

Same model and settings, same component and failure-mode lists, so a recall and a complaint about the same problem end up with the same label. The main rule is to tag the defective part, not its consequence. The seat sensor recalls are why: the sensor cracks, and the result is an airbag that may not deploy correctly. Tagged by consequence, that recall could land in `airbag_deployment_problem` and miss the complaints about the sensor and the airbag warning light.

## Proposal

GPT-6.1 Sol, medium reasoning effort, strict structured output. It gets one signal's totals and its top 8 evidence complaints, and returns a recommended action, a 3 to 5 sentence summary, and 3 to 5 cited complaint IDs. The prompt is explicit that it's a draft and the engineer makes the final decision. It also says not to call a defect confirmed, and that "no recall covers this" is never a reason by itself to open an investigation, since every signal it sees is already uncovered.

v1.0 of this prompt recommended Open investigation for 15 of 16 signals, so I tightened the rules for each action in v1.1.0. Before the demo, v1.1.0 gave 8 Open investigation, 1 Review recall scope, and 7 Keep monitoring.
