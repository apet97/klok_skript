# Clockify User Sync & Identity Automation

This automation engine synchronizes user profiles, custom fields, and manager hierarchies from a CSV source-of-truth into a Clockify workspace. It is built for operational reliability, incorporating reconciliation logic, idempotent safety, and auditable logging.

## Technical Capabilities & Operational Safety

*   **Identity & Workspace Sync**: Maps CSV data to SaaS internal IDs, updating profile metadata (custom fields, work capacity) and rebuilding manager/group structures.
*   **Idempotent Execution**: Infrastructure-as-code approach to API state; treating "already exists" or "already assigned" as success for reliable recurring runs.
*   **Operational Controls**:
    *   **Preflight Permission Probe**: Validates API key scopes before execution.
    *   **Phased Execution**: Segregates cleanup from updates and verification.
    *   **Protected Group logic**: Skips mission-critical groups (e.g., country-based holiday calendars).
    *   **API Hygiene**: Rate limiting and automatic 429 exponential backoff.
    *   **Gated Deactivation**: Explicit confirmation required before user deactivation, with automated entity reassignment.
    *   **Audit Logging**: Exports success/error logs to CSV for compliance and troubleshooting.

## Implementation Details

*   **Source-of-Truth Reconciliation**: Aggregates multiple API calls into a single declarative sync loop.
*   **Manager-Group Mapping**: Dynamically maintains manager groups, assigning roles to both users and groups for profile visibility.
*   **Custom Field Automation**: Provisions and updates required user metadata (Company, Country, Global ID).
*   **Dry-Run Support**: Preview mutations without modifying the live workspace.

## Quick Start

1.  **Configure**: Copy `config.yaml` and set your fallback manager and field mappings.
2.  **Set credentials**: Copy `.env.example` to `.env` and fill in your API key and workspace ID, or provide them interactively when prompted.
3.  **Prepare CSV**: Use the schema in `examples/sample_users.csv`.
4.  **Run**:
    ```bash
    pip install -r requirements.txt
    python3 -m src.main path/to/users.csv
    ```

## CLI Interaction

The tool uses a guided CLI (via `typer`) to ensure operator awareness:
*   **Credentials**: Secure API Key and Workspace ID collection.
*   **Cleanup Selection**: Optional deletion of non-managed groups.
*   **Safety Confirmation**: Requires "I UNDERSTAND" input for destructive phases.

## Engineering Standards

*   **Language**: Python 3.10+
*   **Libraries**: `pandas`, `requests`, `typer`, `pyyaml`
*   **Output**: Generates `sync_success_log.csv` and `sync_error_log.csv`.
