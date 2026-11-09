# Decision workflow

The main idea is that AIP's recommendation and the engineer's decision are two separate things. AIP drafts, the engineer decides, and both are saved so you can see when they disagreed. The demo goes through three cases: agreeing with AIP, drafting a new proposal, and overruling it.

## Reviewing a signal

The Review queue lists the signals ranked by severity score. Clicking one shows its complaint count, serious complaints, crashes or fires, injuries, model years, recall status, and the date it crossed the review threshold. Below that are AIP's proposal and the complaints it cited, with the quote pulled from each one.

## 1. Accepting a proposal: Unexpected Braking

| Field | Value |
|---|---:|
| Complaints | 489 |
| Serious complaints | 449 |
| Crashes or fires | 20 |
| Injuries | 35 |
| Above threshold since | February 10, 2021 |
| AIP proposal | Open investigation |

One of the cited complaints describes the car braking hard on its own on Burbank Boulevard, with two injuries reported. I accept the proposal, and the decision shows up in the Decisions tab with my name and the time.

## 2. Drafting a proposal: Airbag Deployment Problem

This signal only has 17 complaints, but 13 involve a crash or fire and there are 19 injuries. It starts without a proposal, so I click Draft proposal with AIP and the recommendation comes back in a few seconds. I accept that one too.

## 3. Overruling AIP: Driver Assist Fault

AIP recommends Keep monitoring because the complaints describe different failures. I disagree. Three of the four crash reports are the driver-assist system itself failing: the car veered, the collision warning didn't go off, or emergency braking didn't engage. These use the same camera and radar system as the Unexpected Braking complaints, which already has a lot of harm behind it. So I record a different decision: Open investigation.

The Decisions tab then shows my decision next to AIP's original Keep monitoring.

## What gets saved

| From AIP | From the engineer |
|---|---|
| Proposed action | Decision |
| Explanation | Reason |
| Cited complaint IDs | Notes (optional) |
| When it was drafted | Who decided and when |

Two Actions keep these apart. `save-proposal` only writes the AIP fields, and `record-decision` only writes the decision fields, filling in the current user and time automatically. `reopen-signal` clears a decision so the signal goes back in the queue. Decision reasons are a fixed list: Agrees with AIP proposal, Not a safety issue, Already covered by a recall, Duplicate of another signal, Needs more data, or Other.

## Automatic drafting

The Automate rule watches the Signals set for new objects with status Needs review and runs Draft proposal with AIP on each one, retrying every 10 seconds up to 3 times. The signals in the demo were already in the queue before I set it up, so it hasn't fired yet.
