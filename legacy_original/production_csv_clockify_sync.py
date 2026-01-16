"""Clockify user sync script.

Reads a CSV as the source of truth, maps emails to Clockify user IDs, updates
profiles and custom fields, rebuilds manager groups, assigns team managers, and
optionally cleans up groups and deactivates users. The flow is designed to be
safe to rerun and to preserve protected country groups.
"""

# Cell 1: Imports
import getpass
from pathlib import Path
import time

import pandas as pd
import requests

print("✅ Imports loaded successfully")

# Cell 2: Configuration
BASE_URL = "https://api.clockify.me/api/v1"
FALLBACK_MANAGER_EMAIL = "example@example.com"
FALLBACK_GROUP_NAME = "Other functions"

# CSV column to Clockify custom field mapping.
FIELD_MAPPING = {
    "Company (Label)": "Company",
    "Country (Label)": "Country",
    "Global Id": "Global ID"
}

# In-memory logs saved to CSV at the end of the run.
success_log = []
error_log = []

print("✅ Configuration loaded")

# Cell 3: Helper Functions

HELPERS_VERSION = "2025-12-19-country-groups2"
DEFAULT_PAGE_SIZE = 1000

def log_api(email, action, details, response=None, error_msg=None):
    """Print and record API actions, marking idempotent/cleanup outcomes as success."""
    status_code = response.status_code if response is not None else 0
    if response is None:
        if action == "Fallback Assignment":
            status_code = 204
        else:
            status_code = 599
        if not error_msg:
            error_msg = "No Response"
    response_text = response.text if response is not None else (error_msg or "No Response")
    response_text_lower = response_text.lower() if response_text else ""
    is_cleanup_success = (action.startswith("Wipe") or action.startswith("Remove") or action.startswith("Deactivate")) and status_code in [400, 404]
    idempotent_actions = {
        "Add to Group",
        "Assign Direct Mgr",
        "Assign Group Mgr",
        "Create Group",
        "Create Custom Field",
        "Auto-Fix Group Mgr",
        "Pre-Deactivation Reassign"
    }
    is_idempotent_conflict = action in idempotent_actions and status_code in [400, 409] and any(
        k in response_text_lower for k in ["already", "exists", "duplicate"]
    )
    is_info_log = status_code == 0 and not error_msg

    if (200 <= status_code < 300) or is_cleanup_success or is_idempotent_conflict:
        icon = "✅"
        if is_cleanup_success: details += " (Already clean)"
        if is_idempotent_conflict: details += " (Already set)"
    elif is_info_log:
        icon = "ℹ️"
    else:
        icon = "❌"

    print(f"{icon} [{status_code}] {email}: {action} - {details}")
    if status_code >= 400 and not is_cleanup_success and not is_idempotent_conflict:
        print(f"      ↳ Msg: {response_text}")

    entry = {"Email": email, "Action": action, "Details": details, "Status": status_code, "Response": response_text}
    if (200 <= status_code < 300) or is_cleanup_success or is_idempotent_conflict or is_info_log:
        success_log.append(entry)
    else:
        error_log.append(entry)

def rate_limit_sleep():
    """Sleep between API calls to stay under 10 req/s limit. Using 0.15s = ~6.6 req/s for safety."""
    time.sleep(0.15)

def handle_rate_limit(response, retry_count=0, max_retries=3):
    """Handle 429 rate limit errors with exponential backoff."""
    if response is not None and response.status_code == 429:
        if retry_count < max_retries:
            wait_time = (2 ** retry_count) * 1.0  # 1s, 2s, 4s
            print(f"      ⚠️ Rate limited (429). Waiting {wait_time}s before retry {retry_count + 1}/{max_retries}...")
            time.sleep(wait_time)
            return True  # Should retry
    return False  # Don't retry

def clean_number(value):
    """Parse a numeric value from CSV input, tolerating commas and blanks."""
    if pd.isna(value): return 0.0
    s_val = str(value).replace(',', '.')
    try:
        return float(s_val)
    except (ValueError, TypeError):
        return 0.0

def convert_to_iso8601(daily_hours):
    """Convert a daily hours float into Clockify ISO 8601 duration."""
    try:
        hours = int(daily_hours)
        minutes = int(round((daily_hours - hours) * 60))
        return f"PT{hours}H{minutes}M"
    except (ValueError, TypeError):
        return "PT8H"

def get_user_display_name(user_obj):
    """Return a user's display name, falling back to email when name is missing."""
    name = str(user_obj.get('name') or '').strip()
    if name:
        return name
    email = str(user_obj.get('email') or '').strip()
    return email

def pick_preflight_group(groups_raw, protected_group_ids=None):
    """Pick a non-protected group to probe permissions safely."""
    if protected_group_ids is None:
        protected_group_ids = set()
    for group in groups_raw:
        if group.get('id') in protected_group_ids:
            continue
        name = str(group.get('name') or '').strip()
        user_ids = group.get('userIds') or []
        if name and user_ids:
            return group
    for group in groups_raw:
        if group.get('id') in protected_group_ids:
            continue
        name = str(group.get('name') or '').strip()
        if name:
            return group
    return None

def preflight_permission_check(workspace_id, headers, groups_raw, protected_group_ids=None):
    """Check whether the API key can manage users/groups in this workspace."""
    group = pick_preflight_group(groups_raw, protected_group_ids)
    if not group:
        print("⚠️  Preflight: no groups found; skipping permission check.")
        return True
    group_id = group.get('id')
    group_name = str(group.get('name') or '').strip()
    user_ids = group.get('userIds') or []
    if group_id and user_ids:
        probe_user_id = user_ids[0]
        rate_limit_sleep()
        res = requests.post(f"{BASE_URL}/workspaces/{workspace_id}/user-groups/{group_id}/users", headers=headers, json={"userId": probe_user_id})
        if res is None:
            print("⚠️  Preflight: no response from permission probe; continuing.")
            return True
        if res.status_code == 401:
            print("❌ Preflight: API key not authorized for group/user management in this workspace.")
            print(f"   ↳ Response: {res.text}")
            return False
        if res.status_code in [400, 409]:
            print("✅ Preflight: API key has group/user management permission.")
            return True
        if 200 <= res.status_code < 300:
            print("✅ Preflight: API key has group/user management permission.")
            return True
        if res.status_code in [403]:
            print("❌ Preflight: API key forbidden for group/user management.")
            print(f"   ↳ Response: {res.text}")
            return False
        print(f"⚠️  Preflight: unexpected status {res.status_code}: {res.text}")
        return True
    if group_name:
        rate_limit_sleep()
        res = requests.post(f"{BASE_URL}/workspaces/{workspace_id}/user-groups", headers=headers, json={"name": group_name})
        if res is None:
            print("⚠️  Preflight: no response from group-name probe; continuing.")
            return True
        if res.status_code == 401:
            print("❌ Preflight: API key not authorized for group/user management in this workspace.")
            print(f"   ↳ Response: {res.text}")
            return False
        if res.status_code in [400, 409]:
            print("✅ Preflight: API key has group/user management permission.")
            return True
        if 200 <= res.status_code < 300:
            print("✅ Preflight: API key has group/user management permission.")
            return True
        if res.status_code in [403]:
            print("❌ Preflight: API key forbidden for group/user management.")
            print(f"   ↳ Response: {res.text}")
            return False
        print(f"⚠️  Preflight: unexpected status {res.status_code}: {res.text}")
        return True
    print("⚠️  Preflight: no valid group found; skipping permission check.")
    return True

def get_all_items(endpoint, headers):
    """Fetch all items from a paginated Clockify endpoint."""
    items = []
    page = 1
    PAGE_SIZE = DEFAULT_PAGE_SIZE
    print(f"   ↳ Fetching {endpoint}...")
    while True:
        rate_limit_sleep()
        try:
            separator = "&" if "?" in endpoint else "?"
            url = f"{BASE_URL}{endpoint}{separator}page={page}&page-size={PAGE_SIZE}"
            if "users" in endpoint and "user-groups" not in endpoint:
                url += "&memberships=WORKSPACE&include-roles=true"

            # Try with retry on 429
            for retry in range(3):
                res = requests.get(url, headers=headers)
                if res.status_code == 429:
                    wait_time = (2 ** retry) * 1.0
                    print(f"   ⚠️ Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                break

            if res.status_code != 200:
                print(f"   ❌ [{res.status_code}] Error: {res.text}")
                return []
            data = res.json()
            if not data: break
            items.extend(data)
            if len(data) < PAGE_SIZE: break
            page += 1
        except Exception as e:
            print(f"   ❌ Connection error: {e}")
            return []
    return items

def is_user_active_in_workspace(user_obj, workspace_id):
    """Return True if the user is ACTIVE for the target workspace."""
    memberships = user_obj.get('memberships', [])
    for m in memberships:
        if m.get('membershipType') == 'WORKSPACE' and m.get('targetId') == workspace_id:
            return m.get('membershipStatus') == 'ACTIVE'
    return user_obj.get('status') == 'ACTIVE'

def is_workspace_owner(user_obj):
    """Return True if the user has the OWNER role."""
    roles = user_obj.get('roles', [])
    for role in roles:
        if role.get('role') == 'OWNER':
            return True
    return False

def ensure_custom_fields_exist(existing_cfs, workspace_id, headers):
    """Ensure required user custom fields exist; return name->id mapping."""
    cf_map = {}
    required_fields = list(FIELD_MAPPING.values())

    for cf in existing_cfs:
        if cf.get('entityType') != 'USER':
            continue
        name = cf.get('name')
        if name in required_fields and name not in cf_map:
            cf_map[name] = cf.get('id')

    for cf_name in required_fields:
        if cf_name in cf_map:
            continue
        print(f"➕ Creating Custom Field: {cf_name}...")
        rate_limit_sleep()
        payload = {"name": cf_name, "type": "TXT", "entityType": "USER", "status": "VISIBLE", "description": f"Imported field for {cf_name}"}
        try:
            res = requests.post(f"{BASE_URL}/workspaces/{workspace_id}/custom-fields", headers=headers, json=payload)
            log_api("System", "Create Custom Field", f"Created {cf_name}", res)
            if res and res.status_code in [200, 201]:
                cf_map[cf_name] = res.json()['id']
        except Exception as e:
            log_api("System", "Create Custom Field", f"Exception: {cf_name}", None, str(e))
    return cf_map

def get_manager_group_ids_from_roles(users_raw, group_id_map):
    """Collect group IDs where a manager is assigned to their own named group."""
    managed_ids = set()
    for user in users_raw:
        manager_name = get_user_display_name(user)
        if not manager_name:
            continue
        roles = user.get('roles', [])
        for role in roles:
            if role.get('role') != 'TEAM_MANAGER':
                continue
            entities = role.get('entities', [])
            for entity in entities:
                group_id = entity.get('id')
                if not group_id:
                    continue
                source = entity.get('source')
                is_group_role = (source and isinstance(source, dict) and source.get('type') == 'USER_GROUP') or (source == 'USER_GROUP')
                if not is_group_role:
                    continue
                group = group_id_map.get(group_id)
                if group and group.get('name', '').strip() == manager_name:
                    managed_ids.add(group_id)
    return managed_ids

print(f"✅ Helper functions loaded (HELPERS_VERSION={HELPERS_VERSION})")

# Cell 4: API Functions

def api_remove_role(workspace_id, user_id, entity_id, role_name, source_type, headers):
    """Remove a role with 429 retry logic."""
    rate_limit_sleep()
    payload = {"entityId": entity_id, "role": role_name}
    if source_type:
        payload["sourceType"] = source_type

    for retry in range(3):
        try:
            res = requests.delete(f"{BASE_URL}/workspaces/{workspace_id}/users/{user_id}/roles", headers=headers, json=payload)
            if res.status_code == 429:
                wait_time = (2 ** retry) * 1.0
                print(f"      ⚠️ Rate limited (429). Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            return res
        except Exception:
            return None
    return None

def api_add_role(workspace_id, user_id, entity_id, role_name, source_type, headers):
    """Add a role with 429 retry logic."""
    rate_limit_sleep()
    payload = {"entityId": entity_id, "role": role_name}
    if source_type:
        payload["sourceType"] = source_type

    last_response = None
    for retry in range(3):
        try:
            res = requests.post(f"{BASE_URL}/workspaces/{workspace_id}/users/{user_id}/roles", headers=headers, json=payload)
            last_response = res
            if res.status_code == 429:
                wait_time = (2 ** retry) * 1.0
                print(f"      ⚠️ Rate limited (429). Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            return res, None
        except Exception as e:
            return None, str(e)
    if last_response is not None:
        return last_response, None
    return None, "No response after retries"


def api_add_user_to_group(workspace_id, group_id, user_id, headers):
    """Add user to group with 429 retry logic."""
    rate_limit_sleep()

    for retry in range(3):
        try:
            res = requests.post(f"{BASE_URL}/workspaces/{workspace_id}/user-groups/{group_id}/users", headers=headers, json={"userId": user_id})
            if res.status_code == 429:
                wait_time = (2 ** retry) * 1.0
                print(f"      ⚠️ Rate limited (429). Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            return res
        except Exception:
            return None
    return None

def reassign_managed_users(workspace_id, manager_to_deactivate, all_users, fallback_manager_id, fallback_group_id, group_map, headers, protected_group_ids=None):
    """Reassign managed entities to a fallback manager before deactivation."""
    manager_id = manager_to_deactivate['id']
    manager_email = manager_to_deactivate['email']
    if protected_group_ids is None:
        protected_group_ids = set()
    reassigned_count = 0
    print(f"   🔄 Checking for users managed by {manager_email}...")
    manager_roles = manager_to_deactivate.get('roles', [])
    for role in manager_roles:
        if role.get('role') == 'TEAM_MANAGER':
            entities = role.get('entities', [])
            for entity in entities:
                target_id = entity.get('id')
                if not target_id: continue
                source = entity.get('source')
                is_group_role = (source and isinstance(source, dict) and source.get('type') == 'USER_GROUP') or (source == 'USER_GROUP')
                if is_group_role:
                    if target_id in protected_group_ids:
                        continue
                    if fallback_manager_id:
                        res, res_err = api_add_role(workspace_id, fallback_manager_id, target_id, "TEAM_MANAGER", "USER_GROUP", headers)
                        log_api(manager_email, "Pre-Deactivation Reassign", f"Group {target_id} reassigned", res, res_err)
                        reassigned_count += 1
                else:
                    if fallback_manager_id:
                        res, res_err = api_add_role(workspace_id, fallback_manager_id, target_id, "TEAM_MANAGER", None, headers)
                        reassigned_count += 1
                        if fallback_group_id:
                            api_add_user_to_group(workspace_id, fallback_group_id, target_id, headers)
    if reassigned_count > 0:
        print(f"   ✅ Reassigned {reassigned_count} entities to fallback manager")
    return reassigned_count

def wipe_roles_protected(workspace_id, all_users, managed_group_ids, headers):
    """Remove TEAM_MANAGER roles only for managed (non-protected) groups."""
    print("\n☢️  PHASE 1: WIPING TEAM MANAGER ROLES (SAFE MODE)...")
    count = 0
    for user in all_users:
        user_id = user['id']
        email = user['email']
        roles = user.get('roles', [])
        for role in roles:
            if role.get('role') == 'TEAM_MANAGER':
                entities = role.get('entities', [])
                if not entities:
                    rate_limit_sleep()
                    payload = {"entityId": user_id, "role": "TEAM_MANAGER"}
                    res = requests.delete(f"{BASE_URL}/workspaces/{workspace_id}/users/{user_id}/roles", headers=headers, json=payload)
                    log_api(email, "Wipe Role (Ghost)", "Removed empty entity role", res)
                    count += 1
                    continue
                for entity in entities:
                    target_id = entity.get('id')
                    if not target_id: continue
                    source = entity.get('source')
                    is_group_role = (source and isinstance(source, dict) and source.get('type') == 'USER_GROUP') or (source == 'USER_GROUP')
                    if is_group_role:
                        if target_id not in managed_group_ids: continue
                        res = api_remove_role(workspace_id, user_id, target_id, "TEAM_MANAGER", "USER_GROUP", headers)
                        log_api(email, "Wipe Group Role", f"Unassigned from {target_id}", res)
                        count += 1
                    else:
                        res = api_remove_role(workspace_id, user_id, target_id, "TEAM_MANAGER", None, headers)
                        log_api(email, "Wipe Direct Role", f"Unassigned from user {target_id}", res)
                        count += 1
    print(f"✅ Role Wipe Complete. Removed {count} roles.\n")

def wipe_groups_protected(workspace_id, all_groups, managed_group_names, headers):
    """Remove memberships only for managed (non-protected) groups."""
    print("\n☢️  PHASE 2: WIPING GROUP MEMBERSHIPS (SAFE MODE)...")
    count = 0
    for group in all_groups:
        group_id = group['id']
        group_name = group['name']
        if group_name not in managed_group_names: continue
        user_ids = group.get('userIds', [])
        if not user_ids: continue
        print(f"   > Emptying managed group: '{group_name}' ({len(user_ids)} users)...")
        for user_id in user_ids:
            rate_limit_sleep()
            res = requests.delete(f"{BASE_URL}/workspaces/{workspace_id}/user-groups/{group_id}/users/{user_id}", headers=headers)
            if res.status_code in [200, 204]: count += 1
    print(f"✅ Group Wipe Complete. Removed {count} memberships.\n")

# Compatibility flag for api_add_role return type.
API_ADD_ROLE_RETURNS_TUPLE = True

print("✅ API functions loaded")

# Cell 5: MAIN EXECUTION - Run this cell to start the sync

print("="*70)
print("       CLOCKIFY USER SYNC SCRIPT (V16) - VS CODE")
print("="*70)
print("""
THIS SCRIPT WILL PERFORM THE FOLLOWING ACTIONS:

PHASE 1 - CLEANUP (Safe Wipe)
   - Remove all TEAM_MANAGER role assignments for managed groups
   - Empty all managed group memberships
   - Preserves external/manual groups untouched

PHASE 2 - PROFILE UPDATES
   - Update work capacity (Weekly Hours / 5)
   - Update custom fields: Company, Country, Global ID

PHASE 3 - MANAGER ASSIGNMENT
   - Create groups named after each manager
   - Assign users to their manager's group
   - Assign manager role BOTH directly to user AND to group
   - Fallback: Users with invalid/inactive managers -> "Other functions"

PHASE 4 - VERIFICATION
   - Verify all managed groups have team managers assigned
   - Auto-fix any groups missing managers with fallback manager

PHASE 5 - DEACTIVATION (Requires Confirmation)
   - Users ACTIVE in Clockify but NOT in CSV will be deactivated
   - Managed users are reassigned to fallback BEFORE deactivation
   - You will be prompted to confirm before any deactivation

IMPORTANT:
   - This script is IDEMPOTENT - safe to run multiple times
   - Rate limit: ~6.6 req/s with automatic 429 retry
   - All actions are logged and saved at the end
""")
print("="*70)

if globals().get("HELPERS_VERSION") != "2025-12-19-country-groups2":
    raise RuntimeError("Please re-run Cell 3 to load updated helpers.")
if not globals().get("API_ADD_ROLE_RETURNS_TUPLE"):
    raise RuntimeError("Please re-run Cell 4 to load updated API helpers.")

print(f"Loaded helper version: {HELPERS_VERSION}")
print(f"API helpers tuple mode: {API_ADD_ROLE_RETURNS_TUPLE}")

# Safety confirmation
confirm = input("\nType 'I UNDERSTAND' to proceed: ")
if confirm.strip() != "I UNDERSTAND":
    print("❌ Aborted.")
else:
    # Credentials
    print("\n🔐 CREDENTIAL INPUT")
    api_key = getpass.getpass("Enter your Clockify API Key: ")
    workspace_id = input("Enter your Workspace ID: ")

    if not api_key or not workspace_id:
        print("❌ Missing credentials. Aborted.")
    else:
        headers = {"X-Api-Key": api_key, "content-type": "application/json"}

        # Group deletion option
        print("\n🗑️  GROUP CLEANUP OPTION")
        print("Delete groups NOT managed by this script?")
        delete_input = input("Type 'YES' to delete, or press Enter to skip: ").strip().upper()
        DELETE_NON_MANAGED_GROUPS = (delete_input == "YES")

        # CSV input
        print("\n📂 CSV INPUT")
        csv_path = input("Enter path to CSV file: ").strip()
        if not csv_path:
            print("❌ No CSV path provided. Aborted.")
        else:
            csv_file = Path(csv_path).expanduser()
            if not csv_file.is_file():
                print(f"❌ CSV file not found: {csv_file}")
            else:
                print(f"✅ Using CSV: {csv_file}")

                try:
                    df = pd.read_csv(csv_file)
                    # Normalize column names to avoid whitespace mismatches.
                    df.columns = df.columns.str.strip()
                    print(f"✅ Loaded {len(df)} rows.")

                    # Fetch workspace data
                    print("\n⏳ Fetching workspace data...")
                    # Cache users and groups for mapping and cleanup logic.
                    users_raw = get_all_items(f"/workspaces/{workspace_id}/users", headers)
                    user_map = {u['email'].strip().lower(): u for u in users_raw if u.get('email')}

                    groups_raw = get_all_items(f"/workspaces/{workspace_id}/user-groups", headers)
                    for g in groups_raw:
                        g['name'] = str(g.get('name', '')).strip()
                    group_map = {g['name']: g for g in groups_raw}
                    group_id_map = {g['id']: g for g in groups_raw}

                    # Protect country-named groups so holiday calendars remain intact.
                    if 'Country (Label)' in df.columns:
                        csv_country_names = set(df['Country (Label)'].dropna().astype(str).str.strip().str.lower())
                        csv_country_names = {c for c in csv_country_names if c and c != 'nan'}
                    else:
                        csv_country_names = set()
                    protected_group_names = {g_name for g_name in group_map if g_name.lower() in csv_country_names}
                    protected_group_names_lower = {g.lower() for g in protected_group_names}
                    protected_group_ids = {group_map[g]['id'] for g in protected_group_names}
                    if protected_group_names:
                        print(f"INFO: Protected country groups: {len(protected_group_names)}")

                    # Preflight permission check for group/user management
                    if not preflight_permission_check(workspace_id, headers, groups_raw, protected_group_ids):
                        raise RuntimeError("Preflight permission check failed. Use a workspace admin/owner API key.")

                    cfs_raw = get_all_items(f"/workspaces/{workspace_id}/custom-fields?entity-type=USER", headers)
                    cf_id_map = ensure_custom_fields_exist(cfs_raw, workspace_id, headers)

                    # Identify managed groups
                    # Managed groups include the fallback group and each manager's name.
                    desired_group_names = {FALLBACK_GROUP_NAME}
                    if FALLBACK_MANAGER_EMAIL.lower() in user_map:
                        desired_group_names.add(get_user_display_name(user_map[FALLBACK_MANAGER_EMAIL.lower()]))

                    csv_manager_emails = set(df['Manager NTID email'].dropna().astype(str).str.strip().str.lower())
                    csv_manager_emails = {e for e in csv_manager_emails if e and e != 'nan'}

                    for email in csv_manager_emails:
                        if email in user_map:
                            desired_group_names.add(get_user_display_name(user_map[email]))  # Strip whitespace

                    cleanup_group_ids = set()
                    for g_name in desired_group_names:
                        if g_name in group_map:
                            cleanup_group_ids.add(group_map[g_name]['id'])
                    cleanup_group_ids.update(get_manager_group_ids_from_roles(users_raw, group_id_map))
                    cleanup_group_ids = {g_id for g_id in cleanup_group_ids if g_id not in protected_group_ids}

                    cleanup_group_names = {group_id_map[g_id]['name'].strip() for g_id in cleanup_group_ids if g_id in group_id_map}
                    cleanup_group_names.update(desired_group_names)
                    if protected_group_names_lower:
                        cleanup_group_names = {name for name in cleanup_group_names if name.lower() not in protected_group_names_lower}

                    print(f"ℹ️  Identified {len(desired_group_names)} target groups. Cleanup will touch {len(cleanup_group_names)} groups.")

                    # Execute wipe
                    # Safe cleanup for managed groups only (protected groups skipped).
                    wipe_roles_protected(workspace_id, users_raw, cleanup_group_ids, headers)
                    wipe_groups_protected(workspace_id, groups_raw, cleanup_group_names, headers)

                    # Reconstruction
                    print("\n🚀 Starting Reconstruction...")
                    csv_safe_emails = set(df['NTID email'].dropna().astype(str).str.strip().str.lower())
                    csv_safe_emails = {e for e in csv_safe_emails if e and e != 'nan'}

                    # Track which groups have already had their manager assigned
                    groups_with_manager_assigned = set()

                    for index, row in df.iterrows():
                        raw_email = str(row.get('NTID email', '')).strip()
                        email = raw_email.lower()

                        if not email or email == 'nan' or email not in user_map:
                            continue

                        user_obj = user_map[email]
                        user_id = user_obj['id']

                        # Profile update - Weekly hours / 5 (hours already account for employment %)
                        try:
                            weekly_hours = clean_number(row.get('Weekly Working Hours', 40))
                            daily_hours = weekly_hours / 5
                            capacity_iso = convert_to_iso8601(daily_hours)

                            custom_fields_payload = []
                            for csv_header, cf_name in FIELD_MAPPING.items():
                                if csv_header in row and cf_name in cf_id_map:
                                    value = str(row[csv_header]) if not pd.isna(row[csv_header]) else ""
                                    custom_fields_payload.append({"customFieldId": cf_id_map[cf_name], "value": value})

                            profile_payload = {"workCapacity": capacity_iso, "userCustomFields": custom_fields_payload}
                            rate_limit_sleep()
                            res = requests.patch(f"{BASE_URL}/workspaces/{workspace_id}/member-profile/{user_id}", headers=headers, json=profile_payload)
                            log_api(email, "Update Profile", "Capacity/CFs", res)
                        except Exception as e:
                            print(f"Error updating profile for {email}: {e}")

                        # Manager sync
                        manager_email = str(row.get('Manager NTID email', '')).strip().lower()
                        target_manager_id = None
                        target_group_name = FALLBACK_GROUP_NAME
                        target_group_id = None
                        actual_manager_email_used = "None"

                        manager_is_valid = False
                        if manager_email and manager_email != 'nan' and manager_email in user_map:
                            mgr_obj = user_map[manager_email]
                            if is_user_active_in_workspace(mgr_obj, workspace_id):
                                manager_is_valid = True

                        if manager_is_valid:
                            target_manager_id = user_map[manager_email]['id']
                            target_group_name = get_user_display_name(user_map[manager_email])  # Strip whitespace
                            actual_manager_email_used = manager_email
                        else:
                            # Fallback triggered - determine reason with VISIBLE console output
                            if not manager_email or manager_email == 'nan':
                                fallback_reason = "No manager specified in CSV"
                                print(f"      ⚠️ FALLBACK: {email} - No manager email in CSV")
                            elif manager_email not in user_map:
                                fallback_reason = f"CSV manager '{manager_email}' not found"
                                print(f"      ⚠️ FALLBACK: {email} - Manager '{manager_email}' NOT FOUND in Clockify workspace")
                            else:
                                fallback_reason = f"CSV manager '{manager_email}' is INACTIVE"
                                print(f"      ⚠️ FALLBACK: {email} - Manager '{manager_email}' is INACTIVE")

                            action_taken = f"ACTION: Assigning to group '{FALLBACK_GROUP_NAME}' with fallback manager '{FALLBACK_MANAGER_EMAIL}'"
                            log_api(email, "Fallback Assignment", f"Reason: {fallback_reason}. {action_taken}", None, "Fallback")

                            if FALLBACK_MANAGER_EMAIL.lower() in user_map:
                                target_manager_id = user_map[FALLBACK_MANAGER_EMAIL.lower()]['id']
                            else:
                                print(f"      ❌ CRITICAL: Fallback manager '{FALLBACK_MANAGER_EMAIL}' also NOT in workspace! No manager will be assigned.")
                            target_group_name = FALLBACK_GROUP_NAME

                        skip_group_ops = bool(target_group_name) and target_group_name.lower() in protected_group_names_lower
                        if skip_group_ops:
                            print(f"      ℹ️ Skipping protected country group '{target_group_name}'")
                        # Group creation/assignment
                        if target_group_name and not skip_group_ops:
                            if target_group_name not in group_map:
                                rate_limit_sleep()
                                res = requests.post(f"{BASE_URL}/workspaces/{workspace_id}/user-groups", headers=headers, json={"name": target_group_name})
                                log_api(email, "Create Group", f"Created '{target_group_name}'", res)
                                if res.status_code == 201:
                                    new_group = res.json()
                                    new_group['name'] = str(new_group.get('name', '')).strip()
                                    group_map[new_group['name']] = new_group
                                    group_id_map[new_group['id']] = new_group

                            if target_group_name in group_map:
                                target_group_id = group_map[target_group_name]['id']

                        if target_group_id and not skip_group_ops:
                            res = api_add_user_to_group(workspace_id, target_group_id, user_id, headers)
                            log_api(email, "Add to Group", f"Added to '{target_group_name}'", res)

                        # Manager role assignment
                        if target_manager_id:
                            # Direct assignment: Manager can manage this user specifically
                            res_d, res_d_err = api_add_role(workspace_id, target_manager_id, user_id, "TEAM_MANAGER", None, headers)
                            if res_d is not None and res_d.status_code not in [200, 201]:
                                err_text = (res_d.text or "").lower()
                                if not (res_d.status_code in [400, 409] and any(k in err_text for k in ["already", "exists", "duplicate"])):
                                    print(f"      ⚠️ Direct manager assignment failed: {res_d.status_code} - {res_d.text[:200]}")
                            log_api(email, "Assign Direct Mgr", f"Assigned {actual_manager_email_used}", res_d, res_d_err)

                            # Group assignment: Manager can manage entire group (only once per group)
                            if target_group_id and target_group_name not in groups_with_manager_assigned and not skip_group_ops:
                                # Increased delay before group assignment to ensure group state is consistent
                                time.sleep(0.3)

                                # Debug: Log the exact request being made
                                print(f"      🔍 DEBUG: Assigning manager {target_manager_id} to group {target_group_id} ('{target_group_name}')")

                                res_g, res_g_err = api_add_role(workspace_id, target_manager_id, target_group_id, "TEAM_MANAGER", "USER_GROUP", headers)

                                # Enhanced logging for debugging
                                if res_g is not None:
                                    print(f"      🔍 DEBUG: Response status={res_g.status_code}, body={res_g.text[:300]}")

                                already_assigned = res_g is not None and res_g.status_code in [400, 409] and any(
                                    k in (res_g.text or "").lower() for k in ["already", "exists", "duplicate"]
                                )

                                if res_g is not None and res_g.status_code in [200, 201]:
                                    groups_with_manager_assigned.add(target_group_name)
                                    print(f"      ✅ Manager '{actual_manager_email_used}' assigned to group '{target_group_name}'")
                                elif already_assigned:
                                    groups_with_manager_assigned.add(target_group_name)
                                    print(f"      ✅ Manager '{actual_manager_email_used}' already assigned to group '{target_group_name}'")
                                else:
                                    # Retry once with longer delay on failure
                                    print(f"      ⚠️ Group manager assignment failed, retrying after 1s delay...")
                                    time.sleep(1.0)
                                    res_g, res_g_err = api_add_role(workspace_id, target_manager_id, target_group_id, "TEAM_MANAGER", "USER_GROUP", headers)
                                    already_assigned_retry = res_g is not None and res_g.status_code in [400, 409] and any(
                                        k in (res_g.text or "").lower() for k in ["already", "exists", "duplicate"]
                                    )
                                    if res_g is not None and res_g.status_code in [200, 201]:
                                        groups_with_manager_assigned.add(target_group_name)
                                        print(f"      ✅ Manager '{actual_manager_email_used}' assigned to group '{target_group_name}' (retry succeeded)")
                                    elif already_assigned_retry:
                                        groups_with_manager_assigned.add(target_group_name)
                                        print(f"      ✅ Manager '{actual_manager_email_used}' already assigned to group '{target_group_name}' (retry)")
                                    else:
                                        print(f"      ❌ Group manager assignment FAILED: {res_g.status_code if res_g is not None else 'No response'}")
                                        print(f"         Full error: {res_g.text if res_g else 'N/A'}")
                                log_api(email, "Assign Group Mgr", f"Manager assigned to group '{target_group_name}'", res_g, res_g_err)

                    # ── POST-RUN VERIFICATION ────────────────────────────────────────────────────
                    # Query all groups with includeTeamManagers=true to verify assignments worked
                    print("\n🔍 VERIFYING TEAM MANAGER ASSIGNMENTS...")
                    time.sleep(0.5)  # Extra settle time before verification

                    # Fetch groups with team manager info
                    verified_groups = []
                    page = 1
                    while True:
                        rate_limit_sleep()
                        url = f"{BASE_URL}/workspaces/{workspace_id}/user-groups?includeTeamManagers=true&page={page}&page-size={DEFAULT_PAGE_SIZE}"
                        res = requests.get(url, headers=headers)
                        if res.status_code != 200:
                            print(f"   ❌ Failed to fetch groups for verification: {res.status_code}")
                            break
                        data = res.json()
                        if not data:
                            break
                        verified_groups.extend(data)
                        if len(data) < DEFAULT_PAGE_SIZE:
                            break
                        page += 1

                    # Check which managed groups are missing team managers
                    groups_missing_managers = []
                    for group in verified_groups:
                        group_name = group.get('name', '').strip()
                        team_managers = group.get('teamManagers', [])
                        if group_name in desired_group_names and group_name.lower() not in protected_group_names_lower and not team_managers:
                            groups_missing_managers.append(group)

                    if groups_missing_managers:
                        print(f"\n⚠️  Found {len(groups_missing_managers)} managed groups WITHOUT team managers:")
                        for g in groups_missing_managers:
                            print(f"   - {g['name']} (ID: {g['id']})")

                        # Get fallback manager ID for auto-fix
                        fallback_mgr_id = None
                        if FALLBACK_MANAGER_EMAIL.lower() in user_map:
                            fallback_mgr_id = user_map[FALLBACK_MANAGER_EMAIL.lower()]['id']

                        if fallback_mgr_id:
                            print(f"\n🔧 AUTO-FIX: Assigning fallback manager '{FALLBACK_MANAGER_EMAIL}' to groups missing managers...")
                            for g in groups_missing_managers:
                                rate_limit_sleep()
                                time.sleep(0.3)  # Extra delay for stability
                                res_fix, res_fix_err = api_add_role(workspace_id, fallback_mgr_id, g['id'], "TEAM_MANAGER", "USER_GROUP", headers)
                                if res_fix is not None and res_fix.status_code in [200, 201]:
                                    print(f"   ✅ Fixed: {g['name']}")
                                    log_api("System", "Auto-Fix Group Mgr", f"Assigned fallback manager to '{g['name']}'", res_fix, res_fix_err)
                                else:
                                    print(f"   ❌ Failed to fix: {g['name']} - {res_fix.status_code if res_fix is not None else 'No response'}")
                                    log_api("System", "Auto-Fix Group Mgr", f"FAILED to assign fallback manager to '{g['name']}'", res_fix, res_fix_err)
                        else:
                            print(f"   ⚠️  Cannot auto-fix: Fallback manager '{FALLBACK_MANAGER_EMAIL}' not found in workspace")
                    else:
                        print("✅ All managed groups have team managers assigned.")

                    # Delete non-managed groups
                    if DELETE_NON_MANAGED_GROUPS:
                        print("\n🗑️  DELETING NON-MANAGED GROUPS...")
                        for group in groups_raw:
                            group_name = group['name']
                            group_id = group['id']
                            if group_name not in desired_group_names and group_name.lower() not in protected_group_names_lower:
                                rate_limit_sleep()
                                res = requests.delete(f"{BASE_URL}/workspaces/{workspace_id}/user-groups/{group_id}", headers=headers)
                                log_api("System", "Delete Group", f"Deleted non-managed group '{group_name}'", res)
                        print("✅ Non-managed groups deleted.")

                    # Deactivation
                    print("\n⚠️  DEACTIVATION CHECKPOINT")
                    users_to_deactivate = [
                        u for e, u in user_map.items()
                        if is_user_active_in_workspace(u, workspace_id)
                        and e not in csv_safe_emails
                        and not is_workspace_owner(u)
                    ]

                    if users_to_deactivate:
                        print(f"\nFound {len(users_to_deactivate)} active users to deactivate:")
                        for u in users_to_deactivate:
                            print(f"   - {u['email']}")

                        deact_confirm = input("\nDEACTIVATE? (YES/NO): ").strip().upper()
                        if deact_confirm == "YES":
                            fallback_manager_id = user_map.get(FALLBACK_MANAGER_EMAIL.lower(), {}).get('id')
                            fallback_group_id = group_map.get(FALLBACK_GROUP_NAME, {}).get('id')

                            print("\n🔄 PHASE A: REASSIGNING MANAGED USERS...")
                            for user_obj in users_to_deactivate:
                                reassign_managed_users(workspace_id, user_obj, users_raw, fallback_manager_id, fallback_group_id, group_map, headers, protected_group_ids)

                            print("\n🔄 PHASE B: DEACTIVATING USERS...")
                            for user_obj in users_to_deactivate:
                                user_id = user_obj['id']
                                email = user_obj['email']

                                for g_name, g_data in group_map.items():
                                    if g_data['id'] in protected_group_ids:
                                        continue
                                    rate_limit_sleep()
                                    requests.delete(f"{BASE_URL}/workspaces/{workspace_id}/user-groups/{g_data['id']}/users/{user_id}", headers=headers)

                                # Revoke direct manager role
                                api_remove_role(workspace_id, user_id, user_id, "TEAM_MANAGER", None, headers)

                                roles = user_obj.get('roles', [])
                                for role in roles:
                                    if role.get('role') == 'TEAM_MANAGER':
                                        entities = role.get('entities', [])
                                        for entity in entities:
                                            target_id = entity.get('id')
                                            if not target_id: continue
                                            source = entity.get('source')
                                            if source and isinstance(source, dict) and source.get('type') == 'USER_GROUP':
                                                if target_id in protected_group_ids:
                                                    continue
                                                api_remove_role(workspace_id, user_id, target_id, "TEAM_MANAGER", "USER_GROUP", headers)
                                            else:
                                                api_remove_role(workspace_id, user_id, target_id, "TEAM_MANAGER", None, headers)

                                user_name = get_user_display_name(user_obj)
                                if user_name and user_name in group_map:
                                    group_to_delete = group_map[user_name]
                                    if group_to_delete['id'] not in protected_group_ids:
                                        rate_limit_sleep()
                                        res = requests.delete(f"{BASE_URL}/workspaces/{workspace_id}/user-groups/{group_to_delete['id']}", headers=headers)
                                        log_api(email, "Delete Manager Group", f"Deleted group '{user_name}'", res)

                                rate_limit_sleep()
                                res = requests.put(f"{BASE_URL}/workspaces/{workspace_id}/users/{user_id}", headers=headers, json={"status": "INACTIVE"})
                                log_api(email, "Deactivate", "Status set to INACTIVE", res)
                    else:
                        print("✅ No users need deactivation.")

                    # Save logs
                    print("\n💾 Saving logs...")
                    if success_log:
                        success_log_path = Path("sync_success_log.csv")
                        pd.DataFrame(success_log).to_csv(success_log_path, index=False)
                        print(f"✅ Wrote {success_log_path}")
                    if error_log:
                        error_log_path = Path("sync_error_log.csv")
                        pd.DataFrame(error_log).to_csv(error_log_path, index=False)
                        print(f"⚠️ Wrote {error_log_path}")

                    print("\n" + "="*70)
                    print("✅ SCRIPT COMPLETE!")
                    print("="*70)

                except Exception as e:
                    print(f"❌ Error: {e}")
