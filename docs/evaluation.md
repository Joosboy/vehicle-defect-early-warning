# Evaluation

This is where the project stands on whether it actually works, and what I'd do to test it properly.

## What I've checked

- **The pipeline runs end to end** on all 2,930 complaints. 6 came back with an extraction error, which are kept in the `llm_error` column instead of being dropped silently.
- **The prompt needed tuning.** The first version of the AIP Logic function recommended Open investigation for 15 of 16 signals, which isn't useful. Version 1.1.0 spells out when each action applies. Before the demo its 16 proposals were 8 Open investigation, 1 Review recall scope, and 7 Keep monitoring.
- **Spot check.** The Spot check tab has 41 complaints (every complaint ID divisible by 80) so I can check the extraction by hand and mark each one Correct, Wrong failure mode, or Not a vehicle defect. I haven't finished recording that review yet.

## The recall example

| | Original recall | Expansion |
|---|---|---|
| Campaign | 24V064000 | 26V332000 |
| Report received | February 1, 2024 | May 21, 2026 |
| Accord model years | 2020 to 2022 | Adds 2018 and 2019 |
| Failure mode tag | `airbag_sensor_or_warning` | `airbag_sensor_or_warning` |

Once the first recall existed, complaints about the same seat sensor problem on 2018 and 2019 Accords would have shown up as a Possible scope gap: a recall for that failure exists on the model, just not for those years. That's exactly the case the Review recall scope action is for. The expansion came about 27 months later.

I want to be upfront that this is a look-back using today's data. To turn it into a real result, I'd:

1. Rebuild the signals at a series of past cutoff dates, using only the complaints and recalls that existed at each date.
2. Find the first cutoff where the older-car signal shows up as a possible scope gap and is over the threshold.
3. Compare that date with the expansion date.

The pipeline already calculates when each signal first crossed the threshold, so most of the logic is there. What's missing is limiting the recall data to what was known at each date.

## What I'd test next

- Extraction accuracy on a hand-labeled sample, starting with the spot check.
- Whether the recall matching gets things wrong in either direction (complaints marked Covered that shouldn't be, or the reverse).
- Whether the complaint IDs AIP cites actually appear in the evidence it was given.
- How many of the signals in the queue an engineer would agree are worth looking at.
- A new signal going through the automation end to end.
