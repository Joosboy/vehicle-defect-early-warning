# Vehicle Defect Early Warning

I built this for Palantir's Build with AIP challenge. It's a Foundry app that reads car owners' complaints to NHTSA, finds patterns that aren't covered by an existing recall, and puts them in a ranked queue for a safety engineer. AIP drafts a recommendation for each pattern, and the engineer makes the actual call.

The data is 2,930 NHTSA complaints filed since January 2021 about 2018 to 2022 Honda Accords, plus the recalls that cover those cars.

**In numbers:** 2,930 complaints → 2,622 not already covered by a recall → 2,270 with a clear failure mode, grouped into 20 signals → 17 in the review queue for an engineer. In a hand check of 41 complaints, the AI picked the right failure mode for 36 of them.

## Why this problem

Owners describe problems in their own words: the car braked on its own, the airbag didn't go off. Those reports are often the first sign of a defect that ends in a recall, but there are thousands of them and nobody has time to read them all. If a pattern gets missed, people keep driving cars with a known problem. I wanted a tool that does the reading and leaves the decision with an engineer.

One example, as a look-back on today's data: NHTSA recalled an airbag seat sensor on 2020 to 2022 Accords in February 2024, when 2018 and 2019 Accords already had 27 complaints about the same sensor, but the recall wasn't extended to those years until May 2026, about 27 months later ([details](docs/evaluation.md#the-recall-example)).

## How it works

1. **AI reads the complaints.** A Pipeline Builder step with an LLM pulls out the component, one of 24 failure modes, a severity from 1 to 4, and a quote from the complaint that backs it up. A second LLM step tags each recall with the same failure-mode list so complaints and recalls can be matched.
2. **Rules decide what needs review.** Complaints that match an existing recall are filtered out. The rest are grouped by model and failure mode into signals and scored (15 points for a severity 4 report, 5 for a 3, 2 for a 2, 1 otherwise). A signal goes into the review queue once it has a score of at least 30 and at least 5 complaints. I used rules here instead of AI because they're cheap, consistent, and easy to explain to a regulator.
3. **AIP drafts a proposal.** An AIP Logic function looks at one signal and recommends Open investigation, Review recall scope, or Keep monitoring, with a short explanation and the complaint IDs it relied on. My first version recommended Open investigation for 15 of 16 signals, so I tightened the rules in the prompt (v1.1.0).
4. **The engineer decides.** In Workshop, the engineer can accept the proposal or record a different decision with a reason. The AIP recommendation and the human decision are stored in separate fields, so you can always see where they disagreed.

There's also an Automate rule that drafts a proposal whenever a new signal enters the review queue.

## What's in this repo

The app itself lives in Foundry and is mostly built with low-code tools, so this repo is the documentation and everything I could export:

- [`docs/`](docs/): [architecture](docs/architecture.md), [decision workflow](docs/decision-workflow.md), [ontology](docs/ontology.md), and [evaluation](docs/evaluation.md)
- [`prompts/`](prompts/): the three LLM prompts exactly as they are in Foundry, with their settings
- [`logic/`](logic/): the scoring, matching, and threshold rules written out, plus the writeback and automation setup
- [`exports/`](exports/): the ontology export and the Java that Foundry generates from the two pipelines

## Limitations and next steps

- The 27-month number is a look-back on today's data. I haven't rebuilt it as a proper point-in-time backtest yet, which is the first thing I'd do to make it a real metric.
- The accuracy number comes from me checking 41 complaints by hand (every complaint ID divisible by 80, in the Spot check tab), which is a small sample. 6 of the 2,930 complaints came back with an extraction error.
- The Automate rule is active but hasn't fired yet, since no new signal has crossed the threshold since I set it up. Every proposal in the demo was drafted by hand.
- Complaints are owner reports, not confirmed defects, and the proposals are written that way.
- With more time, I'd add warranty and dealer data, which usually shows problems months before NHTSA complaints do, and use engineers' decisions to tune the rules.

## Running it

You can read everything here without Foundry. Running the app needs my Foundry project (datasets, ontology, AIP Logic, Workshop, Automate). The exported Java isn't a standalone version of the pipelines; Foundry can't export the LLM steps, so it leaves placeholders for them. More on that in [`exports/`](exports/README.md).
