


Saturday 9:46 PM

MYPYBITE_V2_Implementation_Blueprint_Rev2.docx
Document
Hello I a need to work on mypybite version 2.0 We wont be using codex we have chosen you as we ran out of codex. Do you have the capacity to establish a working google shates data phase? I can show you my code and my github stuff is in place. This is for the news river portion. I can provide my programing controls pdf that works out the plan for it. we are on phase 5.

Yes. I can build the Google Sheets editorial-data phase and connect it to the Newsriver pipeline using your existing GitHub structure.

One correction: under the uploaded blueprint, Phase 5 is the breaking-relay prototype using hardcoded fixture stories. Google Sheets begins at Phase 6, where the schema and editorial views are frozen. 


The intended architecture is:

Scraper/output → Google Sheets private editorial desk → validation/publishing script → sanitized breaking.json and newsriver.json → Zoho Newsriver renderer

The Sheet must control approval, scheduling, duplicates, eligibility, expiry, pinning and paywall treatment, while private fields such as editor notes never enter the public JSON. 


I can handle:

Auditing the existing Phase 5 Newsriver code

Verifying whether the Phase 5 acceptance gate has actually passed

Defining the Google Sheets columns and validation rules

Creating the Incoming, Duplicate Review, Live & Scheduled, and Audit views

Writing the Google Apps Script or Python publisher

Connecting GitHub Actions securely through repository secrets

Generating and validating breaking.json and newsriver.json

Preserving the last known-good feed when Sheets or validation fails

Testing approval, hold, rejection, scheduling, expiry, correction and retraction

We will follow the blueprint’s rule: one verified component at a time; no phase advances until its gate passes. 


First step
Send the current Phase 5 Newsriver code—preferably the relevant HTML, CSS and JavaScript files or the public GitHub repository link. I will inspect only the existing implementation first and determine whether Phase 5 passes before touching Google Sheets.



MYPYBITE_V2_Implementation.pdf
PDF
Here is my pdf file for implementation. I think we will continue as you advised. 

The PDF confirms the same gated sequence. We should remain on Phase 5 until its acceptance gate passes, then proceed to the Google Sheets work in Phase 6. On page 11, Phase 5 is the fixture-based breaking relay; Phase 6 freezes the Sheet schema and editorial controls. 


The eventual data path shown on page 8 is:

Scrapers → normalization and clustering → private Google Sheet → approval/timing/retention → publisher → public JSON → Breaking Relay and Newsriver. 


Current task: verify Phase 5
The prototype must demonstrate:

Several hardcoded factual stories

Synopses of 25 words or fewer

A quiet one-story-at-a-time transition

Pause and keyboard-focus behaviour

Reduced-motion support

No clipping on mobile

No invented information

First action
Provide the current Newsriver/Breaking Relay code by either:

Uploading the relevant HTML, CSS and JavaScript files or a repository ZIP, or

Sending the public GitHub repository link and identifying the branch and relevant files.

Do not include API keys, Google credentials, repository tokens or other secrets. I will inspect the existing implementation before changing anything.



###MYPBITE V2.txt
Document

e18bf30d-d294-4919-b970-9c7e35a56a81.png
fc9654c9-5cf7-4aca-85f1-36d15af57d1b.png
62d40c6a-a131-4940-bf64-a27248ee6a59.png
So far it looks good. 

59140a9f-2a13-4651-b0c1-b70b01aea0af.png
im not clear. lets go back to 02/06 Because i remember doing all the pause and check if it plays stuff. There was no issue. ill show you where we left off on codex. 

10feda62-49b1-41a3-b230-7f34f6318991.png
ok im in googlesheets. note that in 2025 i tried to do this project and it failed. i suspect that these items will be distracting. But I wont delete them if you have an issue witht hat.

blank sheet started. is this the process where Ideally links will be added and i will then approve some but if i dont approve them some will show up and also it will mitigate duplications as a 24/7 newsriver?

But I need a failsafe in the event I am incommunicado. I need something that still populates things into the newsriver even if I dont act.

Quick Question. How many hours of inactivity before it will revert to automated publishing?

I believe it should be longer than 8 hours. The reason is its likely that in most extreme cases Id still check the google sheet inside of 12 hours, so in that case I would assume that a 15 hour rule would be more reasonable. Consider this. Under the normal run of things I expect to be working in water operation in the future. At least 40 hours a week. If I work overtime the most might be 12 hours a day or 60 a week and that would be rare too, so in respect to that do you think it would be reasonable my idea of 15 hours. You can counter. Perhaps use the council of Picard Dukat Cisco Seven Quark Janeway Riker Worf Tuvok Data and Odo. provide their take and their vote and we will decide on it that way. thank you

Data can you explain the fall back warning? How would that operate?

Then lets proceed. That satisfies most of everyones concerns. I also want you to understand my example is extreme. I doubt I would be working 60 hours a week often. I just know due to overtime pay it may be desireable at times; especially if there is a holiday and time and half is already being paid out.

And isn't it likely that I would still be able to complete review from a smartphone tablet or laptop? 

5d401f3f-84fe-4edd-b957-10b2a5ec2206.png
wait be more specific im not clear if its the A:A area or the area that says fx story_id

3c037c35-a656-4fc7-bab7-a063400db2bb.png
im not clear please be more specific.  

1404fc2b-936c-467a-9fd5-f98b6ad188ed.png
We got this but we couldn't find or understand the If data is invalid to: selection

b80b4ce5-89d3-4299-a2fd-9cbffff53078.png

3ac540bb-7881-4552-87c4-10909002c7c3.png
it just did that? 

478ca527-b1b7-41f3-bdbc-f080f5f457e4.png

ce4162f8-096f-48c4-903a-71c559dbe28f.png
 like that?

65e7a886-bc89-40e0-9d0a-185de5b9d498.png
i selected what you said but i dont know if its applied? I saw a checkmark

db066390-8ed9-43ba-8638-5a331ea25309.png
access checked at is in x see 

2d7edeb8-ed92-45ad-bf1f-82951200faee.png
i only see this 

its selected

Ok I did it Please give me the final steps in detail all at once. Then we complete for the day. It appears you have limits for how quickly we follow your steps. Thank you for accomodation i understand we should be done very soon

547e6a9f-f8b6-4646-ad49-0e204298fd12.png
i got stuck on this step 181. because i couldnt open apps script 

3e572a88-0e27-4c3e-a06a-d86e23db1f76.png
for step 185 should i delete the conditional rule that says "Cell is not empty" i think that was misapplied.

1ede498d-a3ae-43b6-829f-5a022cef5c1c.png
31f02f40-0d6e-4624-bf60-23bf90d5d6ca.png
89beaa0a-0bc0-4ab6-b2c5-fcc55bea85e3.png
looks right to me.

Its there.

Excellent. Ive completed the task. Remember what was accomplished today. Id like a 1 page synopsis of what was worked on and what the file does and quick input on what the sheets do and their protocols. After that we will go to bed and hope you have a good night too

Yesterday 11:34 AM
Hi, Im back. Do you remember what we worked on and what phase we need to do next?

Im not clear on next phase 7. I have created a newsriver that already runs and the repos are on github. Is this what we are rebuilding? Because tbh the newsriver is fine. I was thinking incorporating what we have from ver 1.0 into ver 2.0 and then refining it. Please clarify what phase 7 aims to do. Remember I am a newbie.

ok thank you for the explanation. We before we begin please provide the amount of steps this will take?

If you are able to provide me all the steps to be efficient you may do so. It was effective to do that in the last part in phase 6. I think I can follow along

You misunderstand I need the instructions not a list of the workflow. Please provide the direction for all 30 steps

5b0d6b88-c2d6-405f-8383-2a6a853bc02e.png
for step 15 i think it completed correctly 

f06bf202-c3ea-4689-bdcc-0383ff947203.png
is that what you mean?

c8871c66-2cc9-4ad6-a19b-bfa2cf04e034.png
but i see them here 

f279f7db-0dcb-4056-963c-f2e65eafc680.png
sorry i dont see what youre asking 

500bf63d-7f66-497e-b463-3949c332f8a0.png

f78cefeb-fcc4-4979-bf9c-c619306e5fe6.png
Looks as you described 

cb639be5-fe51-409e-93ad-66f7380a571c.png
looks fine 

it loads

ok so far so good. Now what i gather is this is jus tgithub stuff as in the backend. But inevitably it will manage the ver2.0 of https://www.mypybite.com/ because i went to ver 1.0 on mypybite.com and it looks the same

Ok can you give me the check list and check off what has been completed and leave blank whats not complete. My understanding the next phase is 8

490e2c41-e541-469c-855f-287ced1a9eca.png
I dont see what youre asking me to select

e4bf5630-07d8-4b41-b031-b1bbf75e742e.png


Pasted text(12).txt
Document
what>? you meant paste this 

Today 3:16 PM
fa8192dc-cce6-487a-a074-e1abbde19e41.png
This is a bit of a sidequest. On the pureave.com webpage I wanted to know if this profile reveals an email in dev tools or somewhere on the profile or website 

30b23797-7cd4-4856-abfd-2d3c5b7e56e9.png
where? 

e376a72d-7d7d-4e32-bba0-524fb3a57610.png

2dda7577-a121-4b67-b22f-794d76e5758e.png
ok i did that

fb819442-4064-4a3a-9515-b0281defab11.png
i think im there 

d5f907de-fe2f-4522-9363-bd6c7b9cfeb7.png
 ok did it. how many steps will we be doing today? If you can write out the instructions in under 80 steps go ahead. IF you have to break it up then give me the first 40.

c9655ec0-208f-403c-9428-941388f9ff91.png
wait am i doing this right? Will it cost me money?

610b607b-9518-417c-bf85-8dac226ce9cf.png
um for step 10 i cant find my google sheets 

a7037d23-4948-49e3-a673-e5baf2739479.png
um i get this result 

The problem is that the field labels were pasted into the boxes. Nothing has been created yet.

Enter the fields exactly like this:

Service account name

MYPYBITE Sheet Importer
Service account ID

mypybite-sheet-importer
Service account description

Imports V1 Newsriver headlines into the private V2 Editorial Desk.
Delete the email address currently entered in the description box.

The resulting email shown underneath should resemble:

mypybite-sheet-importer@arched-catwalk-503021-k3.iam.gserviceaccount.com
Then click Create and continue.

One separate issue: the project at the top is named My First Project, rather than MYPYBITE V2 Editorial Automation. This does not prevent the service account from working, but we should rename the project later for clarity.


ok that worked

Next step
On the Permissions section, do not select any role.

Click:

Continue
Then, on Principals with access, leave everything blank and click:

Done
Stop once you return to the Service Accounts list.


c7d09445-2e08-4ad9-b9b6-a0ddf0e224e1.png
did i do something wrong stepp22 is where im at step 21 says it would be shared directly. did i miss something ? 

You did not miss anything. The service account was created correctly.

The wording in Step 21 meant:

Do not give the service account a broad Google Cloud project role.

Later, you will grant it access only to the specific Editorial Desk spreadsheet by sharing that Sheet with its service-account email. Google Sheets sharing works by entering the account’s email address and assigning permission. 

The Key ID — Failed to load message does not mean the account failed. You have not created the separate JSON key yet; service-account keys are created afterward. 

Step 22
Click the blue email address:

mypybite-sheet-importer@arched-catwalk-503021-k3.iam.gserviceaccount.com
This opens the service-account details page.

Stop once you see tabs such as:

Details
Permissions
Keys
Metrics
Logs

The workflow itself started correctly. It failed because GitHub could not find:

scripts/import_headlines_to_sheet.py
The dependencies installed successfully. This is a file-location problem, not a Google credential problem.

Next step
Return to the repository’s Code tab.

Select branch:

phase-8-sheet-import
Open the top-level:

scripts
Do not open the nested scripts/scripts folder.

Check whether this file appears there:

import_headlines_to_sheet.py
Tell me whether it is visible in that exact folder.


Today 9:27 PM
i think it appears
c8d80d47-791f-48a0-b2d2-c03b8973919f.png

Yes. The screenshot confirms the file is in the correct location:

scripts/import_headlines_to_sheet.py
The failure happened because that GitHub Actions run checked out a different branch or an older commit. The log also installed the old requirements.txt, which confirms the run did not use the current Phase 8 branch.

Next check
Return to the failed Actions run.

Near the top, find the branch label beside the commit information. It should say either:

phase-8-sheet-import
or:

main
Tell me which branch is displayed. Do not rerun it yet.


a0a7355a-e2dd-48a2-8026-5c150ab25adb.png
what? im in action

Worked for 1m 2s
You are in the correct place. The screenshot confirms the failed workflow ran on:

main
That is the problem. main does not yet contain the Phase 8 importer script, so GitHub could not find it.

Run the correct branch
Click Import headlines into editorial sheet in the left sidebar.

Click Run workflow near the upper-right.

In the branch dropdown, choose:

phase-8-sheet-import
Click the green Run workflow button.

Do not choose Re-run jobs on the failed run; that would run main again and fail for the same reason.

Stop when a new run appears with the branch label:

phase-8-sheet-import

7b07623e-679b-43f3-aa09-1fc0fcb1ba81.png

This confirms the problem:

Workflow does not exist or does not have a workflow_dispatch trigger in this branch.
The workflow file exists on main, but not on phase-8-sheet-import. Meanwhile, the importer script exists on the Phase 8 branch. The files ended up split between branches.

Do not click Run workflow.

Next step
Click Code at the top.

Select:

phase-8-sheet-import
Open:

.github/workflows
Check whether this file appears:

import_headlines_to_sheet.yml
Tell me whether that file is visible there.


51331a4a-5e70-4bc2-a4e7-b6f1e6795716.png
i think i did what you asked. 

Yes. The screenshot confirms the Phase 8 branch now contains all three required pieces:

scripts/import_headlines_to_sheet.py

.github/workflows/import_headlines_to_sheet.yml

Updated requirements.txt

Next step
Click Actions.

Select Import headlines into editorial sheet.

Click Run workflow.

Choose:

phase-8-sheet-import
Click the green Run workflow button.

Stop when the new run appears with the branch label phase-8-sheet-import.


bbed8c8d-8092-4aeb-a805-bd98d2985223.png
i think it is working as described 

bafeb301-417e-447d-bbf6-d2d20d2c3075.png
yes it resembles 

The first Phase 8 import passed correctly.

The log confirms:

PLAN input=40 add=40 update=0 unchanged=0
DONE added=40 updated=0 unchanged=0
That means:

40 headlines were read from headlines.json

40 new rows were added

No existing rows were overwritten

No duplicate input records were detected

Next step
Open MYPYBITE V2 Editorial Desk and select the Incoming sheet.

Scroll below the seven TEST- rows and confirm the imported real headlines appear. Do not edit anything yet.


ef31dbd5-0ab8-473c-8a82-a8fe4ea564ae.png
i do not see anything other than the test entries. 

beeaaefc-4030-47a7-90a1-9906af7b2baf.png
ok the headlines appeared there.  

Worked for 38s
The first import is confirmed successful. The rows started at 1001 because the prepared checkbox/dropdown range made rows 9–1000 appear occupied to the Google Sheets API.

Do not move or delete them yet. We will correct the insertion location before production use.

Next step — duplicate-safety test
Return to the successful GitHub Actions run.

Click Re-run all jobs.

Confirm Re-run jobs.

Wait for the run to finish.

Open Import latest headlines into Google Sheets.

Expected result:

PLAN input=40 add=0 update=0 unchanged=40
DONE added=0 updated=0 unchanged=40
The critical result is:

added=0
This proves the importer will not create duplicate rows.


38be9687-9f03-46b0-870f-fb0dc2b50f78.png


1cc12579-dd30-4d7d-92e8-4188248fa5b7.png

Worked for 1m 53s
The duplicate-safety test passed.

added=0
updated=1
unchanged=39
updated=1 means one existing record had a machine-managed field refreshed. It did not create a duplicate. The critical result is:

added=0
However, we still need to correct the problem that placed imported rows at 1001 instead of row 9. Do not merge Phase 8 yet.

Next step — replace the importer
Download the corrected file:


Then:

Open the phase-8-sheet-import branch.

Open the top-level scripts folder.

Open the existing import_headlines_to_sheet.py.

Click the pencil icon.

Select all existing code and replace it with the contents of the corrected downloaded file.

Commit with:

Fix Sheet insertion row detection
Commit directly to phase-8-sheet-import.

The revised importer looks specifically for the first blank story_id cell, so preformatted checkboxes and dropdowns will no longer force new records down to row 1001.


Library
/
import_headlines_to_sheet.py


#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "Incoming"

HEADERS = [
    "story_id", "source", "source_url", "canonical_url", "source_story_id",
    "published_at", "scraped_headline", "editor_headline", "candidate_synopsis",
    "editor_synopsis", "status", "cluster_id", "duplicate_of", "ticker_eligible",
    "newsriver_eligible", "auto_eligible", "go_live_at", "expires_at", "keep_until",
    "priority", "pin_until", "access_type", "free_alternative_url",
    "access_checked_at", "rights_note", "editor_notes", "source_trust_tier",
    "risk_class", "auto_candidate", "auto_block_reason", "validation_state",
    "selection_mode", "correction_of", "retraction_note", "imported_at",
    "last_updated_at",
]

MACHINE_REFRESH_FIELDS = [
    "source", "source_url", "canonical_url", "source_story_id",
    "scraped_headline", "cluster_id", "validation_state",
]


class ImportFailure(RuntimeError):
    pass


def utc_now_sheet() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def to_sheet_datetime(value: Any) -> str:
    if not value:
        return ""
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return text


def stable_story_id(item: dict[str, Any]) -> str:
    existing = str(item.get("canonical_id") or "").strip()
    if existing:
        return existing

    url = str(item.get("canonical_url") or item.get("url") or "").strip()
    if not url:
        raise ImportFailure("A headline item has no canonical_id or URL.")

    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return f"u:{digest}"


def load_input(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ImportFailure(f"Input file does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ImportFailure(f"Malformed JSON in {path}: {exc}") from exc

    items = payload.get("items")
    if not isinstance(items, list):
        raise ImportFailure(f"{path} does not contain an items list.")

    return [item for item in items if isinstance(item, dict)]


def load_credentials():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise ImportFailure("GOOGLE_SERVICE_ACCOUNT_JSON is missing.")

    try:
        info = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ImportFailure(
            "GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON."
        ) from exc

    return service_account.Credentials.from_service_account_info(
        info,
        scopes=SCOPES,
    )


def make_new_row(item: dict[str, Any], now: str) -> list[Any]:
    story_id = stable_story_id(item)
    url = str(item.get("url") or "").strip()
    canonical_url = str(item.get("canonical_url") or url).strip()

    values: dict[str, Any] = {
        "story_id": story_id,
        "source": str(item.get("source") or "").strip(),
        "source_url": url or canonical_url,
        "canonical_url": canonical_url or url,
        "source_story_id": story_id,
        "published_at": to_sheet_datetime(item.get("published_utc")),
        "scraped_headline": str(item.get("title") or "").strip(),
        "editor_headline": "",
        "candidate_synopsis": "",
        "editor_synopsis": "",
        "status": "Review",
        "cluster_id": str(item.get("cluster_id") or "").strip(),
        "duplicate_of": "",
        "ticker_eligible": False,
        "newsriver_eligible": False,
        "auto_eligible": False,
        "go_live_at": "",
        "expires_at": "",
        "keep_until": "",
        "priority": "",
        "pin_until": "",
        "access_type": "UNKNOWN",
        "free_alternative_url": "",
        "access_checked_at": "",
        "rights_note": "",
        "editor_notes": "",
        "source_trust_tier": "",
        "risk_class": "",
        "auto_candidate": False,
        "auto_block_reason": "",
        "validation_state": "VALID",
        "selection_mode": "MANUAL",
        "correction_of": "",
        "retraction_note": "",
        "imported_at": now,
        "last_updated_at": now,
    }

    return [values[header] for header in HEADERS]


def merge_existing_row(
    current: list[Any],
    candidate: list[Any],
    now: str,
) -> tuple[list[Any], list[str]]:
    merged = list(current)
    changed_fields: list[str] = []
    index = {name: position for position, name in enumerate(HEADERS)}

    for field in MACHINE_REFRESH_FIELDS:
        position = index[field]
        if str(merged[position]) != str(candidate[position]):
            merged[position] = candidate[position]
            changed_fields.append(field)

    if changed_fields:
        merged[index["last_updated_at"]] = now

    return merged, changed_fields


def pad_row(row: list[Any]) -> list[Any]:
    return list(row[: len(HEADERS)]) + [""] * max(0, len(HEADERS) - len(row))


def import_items(
    spreadsheet_id: str,
    items: list[dict[str, Any]],
    limit: int,
    dry_run: bool,
) -> None:
    credentials = load_credentials()
    service = build(
        "sheets",
        "v4",
        credentials=credentials,
        cache_discovery=False,
    )
    values_api = service.spreadsheets().values()

    result = values_api.get(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_NAME}!A1:AJ",
    ).execute()

    values = result.get("values", [])
    if not values:
        raise ImportFailure(f"{SHEET_NAME} has no header row.")

    actual_headers = pad_row(values[0])
    if actual_headers != HEADERS:
        mismatches = [
            f"{position + 1}: expected {expected!r}, found {actual!r}"
            for position, (expected, actual) in enumerate(
                zip(HEADERS, actual_headers)
            )
            if expected != actual
        ]
        raise ImportFailure(
            "Incoming header mismatch:\n" + "\n".join(mismatches)
        )

    existing_rows = [pad_row(row) for row in values[1:]]
    row_by_story_id: dict[str, tuple[int, list[Any]]] = {}

    for sheet_row_number, row in enumerate(existing_rows, start=2):
        story_id = str(row[0]).strip()
        if story_id:
            row_by_story_id[story_id] = (sheet_row_number, row)

    now = utc_now_sheet()
    additions: list[list[Any]] = []
    updates: list[dict[str, Any]] = []
    unchanged = 0
    skipped_duplicate_input = 0
    seen_input_ids: set[str] = set()

    selected = items[:limit] if limit > 0 else items

    for item in selected:
        candidate = make_new_row(item, now)
        story_id = str(candidate[0])

        if story_id in seen_input_ids:
            skipped_duplicate_input += 1
            continue
        seen_input_ids.add(story_id)

        existing = row_by_story_id.get(story_id)
        if existing is None:
            additions.append(candidate)
            continue

        row_number, current = existing
        merged, changed_fields = merge_existing_row(current, candidate, now)

        if changed_fields:
            print(
                "REFRESH "
                f"story_id={story_id} "
                f"fields={','.join(changed_fields)}"
            )
            updates.append(
                {
                    "range": f"{SHEET_NAME}!A{row_number}:AJ{row_number}",
                    "values": [merged],
                }
            )
        else:
            unchanged += 1

    print(
        "PLAN "
        f"input={len(selected)} "
        f"add={len(additions)} "
        f"update={len(updates)} "
        f"unchanged={unchanged} "
        f"input_duplicates={skipped_duplicate_input}"
    )

    if dry_run:
        print("DRY RUN: no Google Sheet values were changed.")
        return

    writes = list(updates)

    if additions:
        # Google Sheets append() treats preformatted checkbox/dropdown rows as
        # occupied and can place records around row 1001. Instead, place new
        # records in the first rows whose story_id cell (column A) is blank.
        blank_story_rows = [
            row_number
            for row_number, row in enumerate(existing_rows, start=2)
            if not str(row[0]).strip()
        ]

        next_new_row = len(existing_rows) + 2
        while len(blank_story_rows) < len(additions):
            blank_story_rows.append(next_new_row)
            next_new_row += 1

        for row_number, row_values in zip(blank_story_rows, additions):
            writes.append(
                {
                    "range": f"{SHEET_NAME}!A{row_number}:AJ{row_number}",
                    "values": [row_values],
                }
            )

    if writes:
        values_api.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": writes,
            },
        ).execute()

    print(
        "DONE "
        f"added={len(additions)} "
        f"updated={len(updates)} "
        f"unchanged={unchanged}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Import duplicate-safe V1 headline records into "
            "the MYPYBITE V2 Incoming sheet."
        )
    )
    parser.add_argument(
        "--input",
        default="headlines.json",
        help="Path to the existing V1 headlines JSON.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=40,
        help="Maximum number of newest items to consider; 0 means all.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and calculate changes without writing to Sheets.",
    )
    return parser.parse_args()


def main() -> int:
    spreadsheet_id = os.environ.get("MYPYBITE_SHEET_ID", "").strip()
    if not spreadsheet_id:
        print("FAIL: MYPYBITE_SHEET_ID is missing.", file=sys.stderr)
        return 1

    args = parse_args()

    try:
        items = load_input(Path(args.input))
        import_items(
            spreadsheet_id=spreadsheet_id,
            items=items,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    except (ImportFailure, HttpError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
