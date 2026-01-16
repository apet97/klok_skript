# Enterprise User Sync & Governance (Clockify)

This production-ready automation engine synchronizes user profiles, custom fields, and complex manager hierarchies from a CSV source-of-truth into a Clockify workspace.

It is designed as strong evidence of **"enterprise automation + operational safety"**, going beyond simple API calls to implement production-grade reconciliation logic, idempotent safety, and auditable governance.

## What this script proves

*   **Enterprise Identity & Workspace Sync**: Synchronizes CSV data (HRIS export) to a SaaS workspace, mapping emails to internal IDs, updating profile metadata (custom fields, work capacity), and rebuilding manager/group structures.
*   **Idempotent Design**: Implements "safe rerun" logic, treating "already exists" or "already assigned" states as success to ensure reliability in recurring automated workflows.
*   **Operational Safety Controls**:
    *   **Preflight Permission Probe**: Fails fast if the API key lacks necessary scopes (e.g., group management).
    *   **Explicit Phased Execution**: Segregates destructive cleanup from updates and verification for maximum control.
    *   **Protected Groups Logic**: Preserves mission-critical country-based groups (used for holiday calendars) from accidental cleanup.
    *   **API Hygiene**: Implements rate limiting and automatic 429 exponential backoff.
    *   **Gated Deactivation**: Requires explicit operator confirmation before deactivating users, with automated entity reassignment to a fallback manager.
    *   **Structured Audit Logs**: Exports detailed success/error logs to CSV for compliance and troubleshooting.

## Roles this script strengthens

*   **Implementation / Technical Onboarding Engineer**
*   **Customer Success / Support Engineer (L2/L3)**
*   **Integration / Automation Engineer**
*   **CS Ops / Support Ops Engineer**
*   **Business Systems Engineer**

## Key Technical Features

*   **Source-of-Truth Reconciliation**: Replaces manual multi-step Clockify updates with a single "reconcile" loop.
*   **Manager-Group Mapping**: Automatically builds and maintains manager groups, assigning roles to both users and groups to ensure profile visibility/hierarchy integrity.
*   **Custom Field Governance**: Ensures required user metadata (Company, Country, Global ID) is provisioned and updated.
*   **Dry-Run & Confirmation Flow**: Built-in safety checks for destructive operations (group deletion, user deactivation).

## Quick Start

1.  **Configure**: Copy `config.yaml.example` to `config.yaml` and set your fallback manager and field mappings.
2.  **Prepare CSV**: Ensure your CSV follows the schema in `examples/sample_users.csv`.
3.  **Run**:
    ```bash
    pip install -r requirements.txt
    python3 production_csv_clockify_sync.py
    ```

## CLI Parameters & Interaction

The script uses a guided CLI interaction (or optional flags) to ensure operator awareness:
*   **Credentials**: Secure API Key and Workspace ID input.
*   **Cleanup Selection**: Opt-in to deleting non-managed groups.
*   **Safety Trigger**: Requires explicit string confirmation ("I UNDERSTAND") for destructive phases.

## Engineering Standards

*   **Language**: Python 3.10+
*   **Libraries**: `pandas`, `requests`
*   **Audit**: Generates `sync_success_log.csv` and `sync_error_log.csv` after every run.
