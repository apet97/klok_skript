# Clockify User Sync (VS Code)

This folder contains a local Python script that syncs Clockify user data from a CSV.

## Why this exists
Clockify does not expose a simple "manager" field you can import. Managers are visible
on a user's profile only when the manager is assigned directly to that user, even if
the manager is already assigned to a group. The API also requires internal user IDs,
not just emails, so a bulk update needs a mapping step. This script makes the CSV the
source of truth and automates the steps that are otherwise manual and error-prone:
fetch user IDs, sync groups, assign managers both ways, update custom fields, and
optionally deactivate users that are no longer in the CSV.

## Design decisions
- Email is the primary identifier for mapping users because it is stable across systems.
- Managers are assigned to both the user and the manager group to keep profiles accurate.
- Country groups are treated as protected to preserve holiday calendar behavior.
- Cleanup and deactivation are opt-in and explicitly confirmed to avoid accidents.
- Rate limiting and retries are built in to avoid API throttling.
- A fallback manager/group prevents orphaned users when manager data is missing.

## Requirements
- Python 3.10+
- `pip install pandas requests`

## Usage
1. Open a terminal in this folder.
2. Run `python3 production_csv_clockify_sync.py`.
3. Follow the prompts to confirm and enter:
   - Clockify API key
   - Workspace ID
   - Group cleanup option
   - CSV file path

## CSV expectations
The script reads these columns when present:
- `NTID email` (required)
- `Manager NTID email` (used for manager mapping)
- `Weekly Working Hours` (used for capacity; defaults to 40 if missing)
- `Company (Label)`, `Country (Label)`, `Global Id` (custom fields)

`Country (Label)` values are treated as protected group names so holiday calendar
groups are not touched.

## What the script does (detailed)
1. Confirms intent and collects API key, workspace ID, cleanup option, and CSV path.
2. Fetches users, groups, and existing custom fields from the workspace.
3. Ensures required custom fields exist for user profiles.
4. Builds a protected group list from the CSV's country values.
5. Identifies managed groups and safely wipes only those group memberships and team
   manager roles (protected groups are skipped).
6. For each CSV row:
   - Updates work capacity as `Weekly Working Hours / 5`.
   - Updates the Company, Country, and Global ID custom fields.
   - Determines the manager:
     - If the manager email exists and is active, use it.
     - Otherwise, fall back to `FALLBACK_MANAGER_EMAIL` and `FALLBACK_GROUP_NAME`.
   - Creates the manager group if missing.
   - Adds the user to the manager group.
   - Assigns the manager role directly to the user and to the manager group so the
     manager appears on the user's profile and has group-level visibility.
7. Verifies that every managed group has a team manager; auto-fixes missing managers
   using the fallback manager if needed.
8. Optionally deletes non-managed groups (protected groups are never touched).
9. Optionally deactivates users who are active in Clockify but missing from the CSV,
   after reassigning any managed entities to the fallback manager.
10. Writes success and error logs to CSV files for audit and troubleshooting.

## Outputs
- `sync_success_log.csv`
- `sync_error_log.csv`

## Configuration
Update `FALLBACK_MANAGER_EMAIL` and `FALLBACK_GROUP_NAME` in
`production_csv_clockify_sync.py` to match your workspace.

## Notes
- The script can deactivate users not present in the CSV after confirmation.
- The optional group deletion step is destructive; answer carefully.
