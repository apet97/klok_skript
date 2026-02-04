# Security Guidelines

## Secrets Handling

- **Never commit API keys or tokens.** Use environment variables (`.env` file) or provide credentials interactively at runtime.
- The `.gitignore` excludes `.env` files by default.
- If you suspect a key has been exposed, rotate it immediately in Clockify workspace settings.

## Least Privilege

- Use a workspace-admin-scoped API key. The tool requires admin permissions to manage users, groups, and custom fields.
- Do not use organization-level keys unless necessary.

## Dry-Run Mode

- Use `--dry-run` on first runs to preview all mutations without modifying the live workspace:
  ```bash
  python3 -m src.main path/to/users.csv --dry-run
  ```

## Data Handling

- CSV input files contain PII (names, emails, employee IDs). Never commit real CSV files to the repository.
- The `.gitignore` excludes `*.csv` (except the example file) and log outputs (`sync_success_log.csv`, `sync_error_log.csv`).
- Treat sync log outputs as sensitive — they record which users were modified.
