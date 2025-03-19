import os
import re
import time
import random
import sqlite3
import requests
import threading
from flask import Flask, request, jsonify
from datetime import datetime, timezone
import pygame

app = Flask(__name__)

# Use the new database backend.db
DB_PATH = os.getenv("DB_PATH", "backend.db")

AIRTABLE_API_KEY = 'patPuTMAeNxOLZdfF.eae8fe1269153d6dffd899b697f8e4c43bab4981c95e0ada95afae69b6ffea40'
AIRTABLE_BASE_ID = 'appTNX6MFpk1UVS4t'
AIRTABLE_TABLE_NAME = 'SuicideWatch'
AIRTABLE_ACCOUNTS_TABLE_ID = 'tbl3PTwft8fkCD6IC'
AIRTABLE_SUBREDDITS_TABLE_ID = 'tblJpHbvMOogrYNPR'
AIRTABLE_SUBREDDITS_VIEW_ID = 'viw4mpT2FmJbMZkZs'
AIRTABLE_SUICIDEWATCH_VIEW = "Grid view"  # Only non-hidden fields

rank_checker_account = None

active_upvote_threads = {}
last_upvote_sent = None
last_upvote_lock = threading.Lock()

def get_sqlite_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_db():
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    # Create "backend" table with updated fields (using emoji fields for Target and Pin)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS backend (
            Primary_Key TEXT PRIMARY KEY,
            Account_ID TEXT,
            Link TEXT,
            Subreddit TEXT,
            Status TEXT DEFAULT 'Ready',
            Username TEXT,
            Ups INTEGER DEFAULT 0,
            Ups_Total INTEGER DEFAULT 1,
            Speed INTEGER DEFAULT 1,
            Last_Upvoted TEXT,
            "🎯Target" INTEGER,
            "📌Pin" INTEGER,
            LastCreated TEXT,
            Position INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def get_post_link(primary_key):
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Link FROM backend WHERE Primary_Key = ?", (primary_key,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return row[0]
    else:
        print(f"\n❌ No link found for post {primary_key}.")
        return None

def extract_post_info(link):
    if not link:
        return None, None
    try:
        post_id_match = re.search(r"(?<=/comments/)([a-zA-Z0-9]+)", link)
        subreddit_match = re.search(r"(?<=/r/)([a-zA-Z0-9_]+)", link)
        post_id = post_id_match.group(0) if post_id_match else None
        subreddit = subreddit_match.group(0) if subreddit_match else None
        return post_id, subreddit
    except Exception as e:
        print(f"\n❌ Error extracting info from link '{link}': {e}")
        return None, None

def get_post(primary_key):
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM backend WHERE Primary_Key = ?", (primary_key,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def connect_to_proxy(proxy_username):
    print(f"\n🔌 Connecting to proxy as '{proxy_username}'...")
    proxy_host = "proxy-us.proxy-cheap.com"
    proxy_port = "5959"
    proxy_password = "PC_74H724RE50VlHlzrL"
    proxy_url = f"http://{proxy_username}:{proxy_password}@{proxy_host}:{proxy_port}"
    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        test_url = "https://www.reddit.com/"
        response = requests.get(test_url, proxies=proxies, timeout=5)
        if response.status_code == 200:
            print(f"\n✅ Successfully connected!")
            return proxies
        else:
            print(f"\n❌ Connection failed (code: {response.status_code}).")
            return None
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Proxy error: {e}")
        return None

def get_fixed_proxy():
    """
    Always returns a working proxy using fixed username "pcXQEXfQR3-res-us".
    If connection fails (e.g. due to wifi disconnect), waits 1 second and tries again.
    """
    fixed_username = "pcXQEXfQR3-res-us"
    proxies = None
    while proxies is None:
        proxies = connect_to_proxy(fixed_username)
        if proxies is None:
            print("\n❌ Proxy connection failed or wifi disconnected. Retrying in 1 second...")
            time.sleep(1)
    return proxies

def fetch_subreddits(count):
    """
    For each subreddit to fetch (count times), generate a random number between 3679 and 4984.
    Use this number to fetch exactly one record from Airtable filtering on AccountID2.
    If no record is returned or if the request fails, wait 2 seconds and try again until a record is retrieved.
    Returns a list of subreddit names.
    """
    print(f"\n🔍 Fetching random subreddits...")
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_SUBREDDITS_TABLE_ID}"
    subreddits = []
    for i in range(count):
        record_found = False
        while not record_found:
            rand_num = random.randint(3679, 4984)
            params = {
                "filterByFormula": f"{{AccountID2}} = {rand_num}",
                "maxRecords": 1
            }
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                records = data.get("records", [])
                if records:
                    record = records[0]
                    subreddit = record.get("fields", {}).get("Subreddit")
                    if subreddit:
                        subreddits.append(subreddit)
                        record_found = True
                    else:
                        print(f"\n♻️ Trying subreddit {rand_num}")
                else:
                    print(f"\n♻️ Trying subreddit {rand_num}")
            except requests.exceptions.RequestException as e:
                print(f"\n❌ Error fetching subreddit for AccountID2 = {rand_num}: {e}. Retrying in 2 seconds...")
                time.sleep(2)
    return subreddits

def get_available_account(post_id):
    """
    Fetches a random available account from the Accounts table using the following new method:
      1. Generates a random number between 2077 and 3392.
      2. Uses that number to filter on the AccountID2 field along with the existing conditions (e.g. not having upvoted the post).
      3. If no record is returned or if the request fails, waits 2 seconds and tries again until a record is retrieved.
    """
    print(f"\n🔄 Fetching account for {post_id}...")
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_ACCOUNTS_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    while True:
        rand_num = random.randint(2077, 3392)
        filter_formula = (
            f"AND(" 
            f"Clientkey != '',"
            f"NOT(FIND('{post_id}', Upvoted_Links)),"
            f"Status != 'Used',"
            f"Status != 'Banned',"
            f"{{Proxy Username}} != '',"
            f"NOT(Error),"
            f"{{AccountID2}} = {rand_num}"
            f")"
        )
        params = {"filterByFormula": filter_formula, "maxRecords": 1}
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            records = data.get("records", [])
            if records:
                selected_record = records[0]
                account = selected_record.get("fields", {})
                account["record_id"] = selected_record.get("id")
                if "User Agent" not in account or not account["User Agent"]:
                    raise ValueError("Account is missing a 'User Agent'.")
                print(f"\n✅ {account.get('Username', 'Unknown')}")
                return account
            else:
                print(f"\n♻️ Retrying with = {rand_num}")
        except requests.exceptions.RequestException as e:
            print(f"\n❌ Error fetching account for AccountID2 = {rand_num}: {e}")

def get_rank_checker_account():
    print(f"\n🔄 Fetching account 2078 for rank checking...")
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_ACCOUNTS_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    filter_formula = "AND({Account ID}='2078')"
    params = {"filterByFormula": filter_formula, "maxRecords": 1}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        records = data.get("records", [])
        if records:
            account = records[0]["fields"]
            account["record_id"] = records[0]["id"]
            if "User Agent" not in account or not account["User Agent"]:
                raise ValueError("Rank checker account missing 'User Agent'.")
            return account
        else:
            print("\n❌ No account found with Account ID 2078.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Error fetching rank checker account: {e}")
        return None

def get_reddit_token(account, proxies):
    username = account["Username"]
    password = account["Password"]
    client_id = account["Clientkey"]
    client_secret = account["Secret"]
    user_agent = account["User Agent"]
    auth_url = "https://www.reddit.com/api/v1/access_token"
    auth_data = {"grant_type": "password", "username": username, "password": password}
    auth_headers = {"User-Agent": user_agent}
    print(f"\n🔑 Authenticating as '{username}'")
    try:
        response = requests.post(auth_url, data=auth_data, auth=(client_id, client_secret),
                                 headers=auth_headers, proxies=proxies, timeout=10)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data.get("access_token")
        if access_token:
            return access_token
        else:
            return None
    except requests.exceptions.RequestException as e:
        return None

def update_airtable_record(primary_key, fields):
    """
    Update the given fields in the Airtable record for the post identified by primary_key.
    Note: For single select fields like "Status", simply passing the string is the expected format.
    """
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    try:
        response = requests.get(url, headers=headers,
                                params={"filterByFormula": f"{{Primary_Key}}='{primary_key}'",
                                        "view": AIRTABLE_SUICIDEWATCH_VIEW},
                                proxies=None)
        response.raise_for_status()
        records = response.json().get("records", [])
        if not records:
            print(f"\n❌ No record found for {primary_key} to update.")
            return
        record_id = records[0]["id"]
        update_url = f"{url}/{record_id}"
        update_payload = {"fields": fields}
        update_response = requests.patch(update_url, headers=headers, json=update_payload, proxies=None)
        update_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Record update error for {primary_key}: {e}")

def update_airtable_log(primary_key, log_message):
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    try:
        response = requests.get(url, headers=headers,
                                params={"filterByFormula": f"{{Primary_Key}}='{primary_key}'",
                                        "view": AIRTABLE_SUICIDEWATCH_VIEW},
                                proxies=None)
        response.raise_for_status()
        records = response.json().get("records", [])
        if not records:
            print(f"\n❌ No record found for {primary_key} to update log.")
            return
        record_id = records[0]["id"]
        update_url = f"{url}/{record_id}"
        update_payload = {"fields": {"Log": log_message}}
        update_response = requests.patch(update_url, headers=headers, json=update_payload, proxies=None)
        update_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Log update error for {primary_key}: {e}")

def update_airtable_upvoted_links(record_id, post_id):
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_ACCOUNTS_TABLE_ID}/{record_id}"
    try:
        response = requests.get(url, headers=headers, proxies=None)
        response.raise_for_status()
        account_data = response.json()
        existing_links = account_data["fields"].get("Upvoted_Links", "")
        updated_links = existing_links.strip()
        if updated_links:
            updated_links += f" t3_{post_id}"
        else:
            updated_links = f"t3_{post_id}"
        update_payload = {"fields": {"Upvoted_Links": updated_links}}
        update_response = requests.patch(url, headers=headers, json=update_payload, proxies=None)
        update_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Update Upvoted_Links error: {e}")

def browse_random_subreddits(token, proxies, account):
    """
    Fetches a random number (0-3) of extra subreddits and, for each,
    randomly decides to upvote, downvote, or simply skip browsing.
    For each subreddit, a search is performed and a random action is taken.
    """
    num = random.randint(0, 2)
    if num == 0:
        return
    extra_subreddits = fetch_subreddits(num)
    for subreddit in extra_subreddits:
        action = random.choice(["upvote", "downvote", "skip"])
        print(f"\n🔎 Searching r/{subreddit}...")
        headers = {"User-Agent": account["User Agent"], "Authorization": f"bearer {token}"}
        search_url = f"https://oauth.reddit.com/r/{subreddit}/new"
        limit_val = random.randint(30, 99)
        params = {"limit": limit_val}
        try:
            response = requests.get(search_url, headers=headers, proxies=proxies, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            posts = data.get("data", {}).get("children", [])
            print(f"✅ Found {len(posts)} posts in r/{subreddit}")
        except Exception as e:
            print(f"\n❌ Error searching r/{subreddit}: {e}")
            posts = []
        wait_time = random.randint(5, 10)
        print(f"\n⏳ Waiting {wait_time}s")
        time.sleep(wait_time)
        if action in ["upvote", "downvote"]:
            if posts:
                chosen_post = random.choice(posts)
                post_id = chosen_post.get("data", {}).get("id", "Unknown")
                title = chosen_post.get("data", {}).get("title", "No Title")
                if action == "upvote":
                    print(f"\n👍 Upvoting post {post_id}: {title}")
                    update_airtable_upvoted_links(account.get("record_id", ""), post_id)
                else:
                    print(f"\n👎 Downvoting post {post_id}: {title}")
                    update_airtable_upvoted_links(account.get("record_id", ""), post_id)
                action_wait = random.randint(1, 4)
                print(f"\n⏱️ Waiting {action_wait}s after action.")
                time.sleep(action_wait)
            else:
                print(f"\n🤷 Found nothing interesting in r/{subreddit}.")
        else:
            print(f"\n🤷 Found nothing interesting in r/{subreddit}.")

def quality_check():
    """
    Check posts with Status 'Qualifying' to determine if they exist on Reddit.
    If found (author extracted), update Status to 'Ready' and Username.
    Also, if the subreddit is extracted from the Link, update the Subreddit field in Airtable.
    
    IMPORTANT: This function does NOT update the Position field.
    """
    print("\n🔎 Starting Quality Check for new posts...")
    initial_delay = random.randint(1, 6)
    time.sleep(initial_delay)

    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM backend WHERE Status='Qualifying'")
    qualifying_posts = cursor.fetchall()
    conn.close()

    for post in qualifying_posts:
        primary_key = post["Primary_Key"]
        link = post["Link"]
        post_id, extracted_subreddit = extract_post_info(link)
        if not post_id or not extracted_subreddit:
            print(f"\n❌ Missing data for QC for post {primary_key}.")
            update_airtable_log(primary_key, "❌Missing data for QC")
            conn = get_sqlite_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE backend SET Status='Lost' WHERE Primary_Key=?", (primary_key,))
            conn.commit()
            conn.close()
            continue

        # Update Airtable's Subreddit field if available
        update_airtable_record(primary_key, {"Subreddit": extracted_subreddit})

        print(f"\n🔍 Quality Check for post {primary_key} in r/{extracted_subreddit}")
        search_url = f"https://www.reddit.com/r/{extracted_subreddit}/new.json"
        params = {"limit": 100}
        posts_list = []
        while True:
            try:
                proxies = get_fixed_proxy()
                response = requests.get(search_url, params=params, timeout=10,
                                        headers={"User-Agent": "QualityCheckBot/0.1"}, proxies=proxies)
                response.raise_for_status()
                data = response.json()
                posts_list = data.get("data", {}).get("children", [])
                break
            except requests.exceptions.RequestException as e:
                print(f"\n❌ Quality Check error for {primary_key}: {e}. Retrying in 1 second...")
                time.sleep(1)

        author = None
        for item in posts_list:
            if item["data"].get("id") == post_id:
                author = item["data"].get("author")
                break

        conn = get_sqlite_connection()
        cursor = conn.cursor()
        if author:
            print(f"\n🪪 Post found for {primary_key} by author '{author}'.")
            update_airtable_log(primary_key, "🪪Post found")
            # Update only Status and Username (do NOT modify the Position field)
            cursor.execute("UPDATE backend SET Status='Ready', Username=? WHERE Primary_Key=?", (author, primary_key))
            update_airtable_record(primary_key, {"Username": author, "Status": "Ready"})
        else:
            print(f"\n❌ Post not found for {primary_key}.")
            update_airtable_log(primary_key, "❌Post not found")
            cursor.execute("UPDATE backend SET Status='Lost' WHERE Primary_Key=?", (primary_key,))
        conn.commit()
        conn.close()
        wait_between = random.randint(2, 4)
        time.sleep(wait_between)
    print("\n🔎 Quality Check completed.")

def fetch_new_posts_from_airtable():
    while True:
        print(f"\n🔍 Checking SuicideWatch for incoming posts...")
        headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
        params = {"filterByFormula": "Status='Pending'", "view": AIRTABLE_SUICIDEWATCH_VIEW}
        try:
            response = requests.get(url, headers=headers, params=params, proxies=None)
            response.raise_for_status()
            data = response.json()
            conn = get_sqlite_connection()
            cursor = conn.cursor()
            fetched_count = 0
            new_posts_inserted = False
            for record in data.get('records', []):
                fields = record.get('fields', {})
                primary_key = fields.get('Primary_Key')
                if not primary_key:
                    continue

                # If 'Subreddit' is missing or '-', try extracting from 'Link'
                if not fields.get('Subreddit') or (isinstance(fields.get('Subreddit'), str) and fields.get('Subreddit').strip() == "-"):
                    link = fields.get('Link')
                    _, extracted_subreddit = extract_post_info(link)
                    if extracted_subreddit:
                        fields['Subreddit'] = extracted_subreddit

                # Map "Last Created" from Airtable to "LastCreated"
                if 'Last Created' in fields:
                    fields['LastCreated'] = fields['Last Created']
                    del fields['Last Created']
                # Ensure required fields have default values if missing.
                if '🎯Target' not in fields:
                    fields['🎯Target'] = 1
                if '📌Pin' not in fields:
                    fields['📌Pin'] = 0
                if 'LastCreated' not in fields or not fields['LastCreated']:
                    fields['LastCreated'] = datetime.now(timezone.utc).isoformat()

                # Force status to 'Qualifying'
                fields['Status'] = "Qualifying"

                # Create missing columns in the local SQLite table.
                cursor.execute("PRAGMA table_info(backend)")
                existing_columns = [row["name"] for row in cursor.fetchall()]
                for col, val in fields.items():
                    if col not in existing_columns:
                        if isinstance(val, (int, float)):
                            alter_query = f'ALTER TABLE backend ADD COLUMN "{col}" REAL'
                        else:
                            alter_query = f'ALTER TABLE backend ADD COLUMN "{col}" TEXT'
                        cursor.execute(alter_query)
                        conn.commit()

                # Build dynamic insert: convert numeric values appropriately.
                columns = ', '.join(f'"{col}"' for col in fields.keys())
                placeholders = ', '.join(['?'] * len(fields))
                values = []
                for col in fields.keys():
                    val = fields[col]
                    if isinstance(val, (int, float)):
                        values.append(val)
                    else:
                        values.append(str(val) if val is not None else None)

                # Use INSERT OR REPLACE so that the Primary_Key is enforced.
                cursor.execute(f"INSERT OR REPLACE INTO backend ({columns}) VALUES ({placeholders})", tuple(values))
                conn.commit()
                fetched_count += 1

                # Update the Airtable record to set Status to "Qualifying" and update Subreddit field if available
                update_airtable_record(primary_key, {
                    "Status": "Qualifying",
                    "Subreddit": fields.get("Subreddit", ""),
                    "🎯Target": fields.get("🎯Target"),
                    "📌Pin": fields.get("📌Pin"),
                    "LastCreated": fields.get("LastCreated")
                })
                update_airtable_log(primary_key, "📥Imported")
                new_posts_inserted = True

            conn.commit()
            conn.close()
            print(f"\n Importing {fetched_count} posts")
            if new_posts_inserted:
                qc_thread = threading.Thread(target=quality_check, daemon=True)
                qc_thread.start()
        except Exception as e:
            print(f"\n❌ Error fetching posts: {e}")
        wait_fetcher = random.randint(170, 190)
        time.sleep(wait_fetcher)

def set_post_status(primary_key, status):
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE backend SET Status=? WHERE Primary_Key=?", (status, primary_key))
    conn.commit()
    conn.close()

def cleanup_thread(primary_key):
    if primary_key in active_upvote_threads:
        del active_upvote_threads[primary_key]

def mark_post_as_done(primary_key):
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE backend SET Status='Done' WHERE Primary_Key=?", (primary_key,))
    conn.commit()
    conn.close()
    update_airtable_record(primary_key, {"Status": "Done"})
    update_airtable_log(primary_key, "Done")

def update_post_rank(primary_key, new_position, new_status):
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Position, Ups, Ups_Total FROM backend WHERE Primary_Key = ?", (primary_key,))
    row = cursor.fetchone()
    old_position = row["Position"] if row and row["Position"] is not None else new_position
    cursor.execute("UPDATE backend SET Position = ?, Status = ? WHERE Primary_Key = ?", (new_position, new_status, primary_key))
    conn.commit()
    conn.close()
    if new_position < old_position:
        update_airtable_log(primary_key, "⬆️Rank up")
    elif new_position > old_position:
        update_airtable_log(primary_key, "⬇️Rank down")
    else:
        update_airtable_log(primary_key, "🏳️Rank updated")
    if row and row["Ups_Total"]:
        progress = ((row["Ups"] / row["Ups_Total"]))
    else:
        progress = 0
    update_airtable_record(primary_key, {"🏳️Position": new_position, "Progress": progress})
    print(f"\n🪪 Post: {primary_key}, Position: {new_position}")

def rank_checker():
    global rank_checker_account
    if rank_checker_account is None:
        rank_checker_account = get_rank_checker_account()
        if rank_checker_account is None:
            print("\n❌ Cannot perform rank checking without account 2078. Exiting rank checker.")
            return

    while True:
        print("\n🏳️ Fetching ranks...")
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM backend WHERE Status IN ('Ready', 'Paused')")
        rows = cursor.fetchall()
        conn.close()

        posts = [dict(row) for row in rows]
        print(f"[DEBUG] Found {len(posts)} posts to check for rank updates.")

        for post in posts:
            primary_key = post["Primary_Key"]
            post_link = post["Link"]
            subreddit = post["Subreddit"]
            # Use the new emoji fields exclusively for target and pin
            target = post.get("🎯Target")
            pin = post.get("📌Pin")
            last_created = post.get("LastCreated")

            if not all([primary_key, subreddit, target, pin, last_created]):
                print(f"[DEBUG] Skipping {primary_key}: Missing required fields.")
                continue

            post_id, _ = extract_post_info(post_link)
            if not post_id:
                print(f"[DEBUG] Skipping {primary_key}: Could not extract post ID.")
                continue

            headers = {"User-Agent": rank_checker_account["User Agent"]}
            url = f"https://www.reddit.com/r/{subreddit}/hot.json"
            params = {"limit": 100}
            # Retry loop with proxy in case of connection issues.
            while True:
                try:
                    proxies = get_fixed_proxy()
                    response = requests.get(url, headers=headers, params=params, timeout=10, proxies=proxies)
                    response.raise_for_status()
                    data = response.json()
                    hot_posts = data.get("data", {}).get("children", [])
                    break
                except requests.exceptions.RequestException as e:
                    print(f"[DEBUG] Error fetching /hot for {primary_key} in r/{subreddit}: {e}. Retrying in 1 second...")
                    time.sleep(1)

            current_position = None
            rank = 0
            for item in hot_posts:
                if item["data"].get("stickied"):
                    continue
                rank += 1
                if item["data"].get("id") == post_id:
                    current_position = rank
                    break

            if current_position is None:
                print(f"[DEBUG] Post {primary_key} not found in /hot listing.")
                continue

            post_created_time = datetime.fromisoformat(last_created)
            age_seconds = (datetime.now(timezone.utc) - post_created_time).total_seconds()
            pin_seconds = pin * 3600
            new_status = post["Status"]
            if current_position <= target and age_seconds < pin_seconds:
                new_status = "Paused"
            elif post["Status"] == "Paused" and current_position > target:
                new_status = "Ready"

            print(f"[DEBUG] For {primary_key}: current_position = {current_position}, age = {age_seconds:.0f}s, target = {target}, pin_seconds = {pin_seconds}")
            update_post_rank(primary_key, current_position, new_status)

        time.sleep(60)

def check_and_upvote():
    while True:
        print("\n🔎 Checking posts...")
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM backend WHERE Status='Ready'")
        posts = cursor.fetchall()
        conn.close()

        output_lines = []
        # Define a tolerance of 2 seconds for time_remaining.
        tolerance = 2
        if not posts:
            output_lines.append("ℹ️ No 'Ready' posts in database.")
        else:
            for post in posts:
                primary_key = post["Primary_Key"]
                speed = post["Speed"]
                last_upvoted = post["Last_Upvoted"]
                ups = post["Ups"]
                ups_total = post["Ups_Total"] if post["Ups_Total"] else 1
                completion_percentage = round((ups / ups_total) * 100) if ups_total > 0 else 0
                interval_seconds = round((60.0 / speed) * 60)
                if last_upvoted is None:
                    time_remaining = 0
                else:
                    last_upvote_time = datetime.fromisoformat(last_upvoted)
                    elapsed_seconds = round((datetime.now(timezone.utc) - last_upvote_time).total_seconds())
                    time_remaining = max(0, interval_seconds - elapsed_seconds)
                output_lines.append(f"🪪 {primary_key}: {completion_percentage}% Complete, {time_remaining}s until next upvote")
                
                if time_remaining > tolerance:
                    continue
                # Clean up stale thread entries.
                if primary_key in active_upvote_threads:
                    thread = active_upvote_threads[primary_key]
                    if not thread.is_alive():
                        del active_upvote_threads[primary_key]
                    else:
                        continue
                update_airtable_log(primary_key, "🚀Launching upvote thread")
                update_airtable_record(primary_key, {"Status": "Upvoting"})
                set_post_status(primary_key, "Upvoting")
                upvote_thread = threading.Thread(target=execute_upvote, args=(primary_key,), daemon=True)
                active_upvote_threads[primary_key] = upvote_thread
                upvote_thread.start()
        # Group and print all status lines together.
        print("\n".join(output_lines))
        wait_scheduler = random.randint(8, 12)
        time.sleep(wait_scheduler)

def execute_upvote(primary_key):
    print(f"\n🚀 Starting thread for post {primary_key}...")
    thread_upvote_lock = random.randint(10, 20)
    print(f"\n🔒 Thread lock: {thread_upvote_lock}s")
    
    account = get_available_account(primary_key)
    if not account or "Username" not in account:
        update_airtable_log(primary_key, "❌No valid account (missing Username)")
        print(f"\n❌ No valid account for {primary_key}. Marking as Ready.")
        set_post_status(primary_key, "Ready")
        cleanup_thread(primary_key)
        return

    username = account["Username"]
    password = account["Password"]
    client_id = account["Clientkey"]
    client_secret = account["Secret"]
    proxy_username = account["Proxy Username"]
    user_agent = account["User Agent"]
    record_id = account["record_id"]

    post = get_post(primary_key)
    if not post:
        print(f"\n❌ No post found in DB for {primary_key}.")
        cleanup_thread(primary_key)
        return

    post_link = get_post_link(primary_key)
    post_id, subreddit = extract_post_info(post_link)
    if not post_id or not subreddit:
        print(f"\n❌ Failed to extract info for {primary_key}. Exiting.")
        cleanup_thread(primary_key)
        return

    print(f"\n🪪 Processing post {primary_key}")
    proxies = connect_to_proxy(proxy_username)
    while not proxies:
        update_airtable_log(primary_key, "❌Proxy error, trying new account")
        print(f"\n❌ Proxy failed for {primary_key}. Fetching another account and retrying.")
        account = get_available_account(primary_key)
        if not account or "Username" not in account:
            update_airtable_log(primary_key, "❌No valid account (missing Username)")
            print(f"\n❌ No valid account for {primary_key}. Marking as Ready.")
            set_post_status(primary_key, "Ready")
            cleanup_thread(primary_key)
            return
        username = account["Username"]
        password = account["Password"]
        client_id = account["Clientkey"]
        client_secret = account["Secret"]
        proxy_username = account["Proxy Username"]
        user_agent = account["User Agent"]
        record_id = account["record_id"]
        proxies = connect_to_proxy(proxy_username)

    # Retry obtaining a Reddit token until a valid one is returned.
    while True:
        access_token = get_reddit_token(account, proxies)
        if access_token:
            break
        update_airtable_log(primary_key, "❌Auth error, trying new account")
        print(f"\n❌ Auth error for {username}. Trying new account.")
        account = get_available_account(primary_key)
        if not account or "Username" not in account:
            update_airtable_log(primary_key, "❌No valid account (missing Username)")
            print(f"\n❌ No valid account for {primary_key}. Marking as Ready.")
            set_post_status(primary_key, "Ready")
            cleanup_thread(primary_key)
            return
        username = account["Username"]
        password = account["Password"]
        client_id = account["Clientkey"]
        client_secret = account["Secret"]
        proxy_username = account["Proxy Username"]
        user_agent = account["User Agent"]
        record_id = account["record_id"]
        proxies = connect_to_proxy(proxy_username)
        while not proxies:
            update_airtable_log(primary_key, "❌Proxy error, trying new account")
            print(f"\n❌ Proxy failed for {primary_key}. Fetching another account and retrying.")
            account = get_available_account(primary_key)
            if not account or "Username" not in account:
                update_airtable_log(primary_key, "❌No valid account (missing Username)")
                print(f"\n❌ No valid account for {primary_key}. Marking as Ready.")
                set_post_status(primary_key, "Ready")
                cleanup_thread(primary_key)
                return
            username = account["Username"]
            password = account["Password"]
            client_id = account["Clientkey"]
            client_secret = account["Secret"]
            proxy_username = account["Proxy Username"]
            user_agent = account["User Agent"]
            record_id = account["record_id"]
            proxies = connect_to_proxy(proxy_username)

    print(f"\n🔑 Authenticated successfully.")

    # Browse extra random subreddits using the valid OAuth token
    browse_random_subreddits(token=access_token, proxies=proxies, account=account)

    delay_search = random.randint(2, 5)
    time.sleep(delay_search)
    search_url = f"https://oauth.reddit.com/r/{subreddit}/new"
    search_params = {"count": 1, "limit": random.randint(60, 99)}
    headers = {"Authorization": f"bearer {access_token}", "User-Agent": user_agent}
    try:
        search_response = requests.get(search_url, headers=headers,
                                       params=search_params, proxies=proxies, timeout=10)
        if search_response.status_code == 200:
            print(f"\n✅ r/{subreddit} search successful")
        else:
            print(f"\n⚠️ r/{subreddit} search code: {search_response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"\n⚠️ Search error: {e}. Continuing.")
    delay_pre_vote = random.randint(4, 9)
    time.sleep(delay_pre_vote)
    with last_upvote_lock:
        while True:
            global last_upvote_sent
            now = time.time()
            if last_upvote_sent is None or (now - last_upvote_sent) >= thread_upvote_lock:
                break
            else:
                wait_more = random.randint(1, 3)
                time.sleep(wait_more)
        last_upvote_sent = time.time()
    upvote_url = "https://oauth.reddit.com/api/vote"
    upvote_payload = {"dir": 1, "id": f"t3_{post_id}", "rank": 2}
    
    # Retry loop for upvote attempts
    max_retries = 3
    attempt = 0
    while attempt < max_retries:
        try:
            upvote_response = requests.post(upvote_url, headers=headers, data=upvote_payload,
                                            proxies=proxies, timeout=10)
            upvote_response.raise_for_status()
            break  # Success – exit the retry loop.
        except requests.exceptions.RequestException as e:
            attempt += 1
            if attempt < max_retries:
                print(f"\n⚠️ Upvote attempt {attempt} failed for {primary_key}: {e}. Retrying...")
                time.sleep(random.randint(2, 4))
            else:
                update_airtable_log(primary_key, "❌Upvote error after retries")
                print(f"\n❌ Upvote failed for {primary_key} after {max_retries} attempts: {e}")
                set_post_status(primary_key, "Ready")
                update_airtable_record(primary_key, {"Status": "Ready"})
                cleanup_thread(primary_key)
                return

    update_airtable_log(primary_key, "👍🏼Upvoted")
    print(f"\n👍🏼 Upvoted post {primary_key}")
    pygame.mixer.init()
    pygame.mixer.music.load("C:/Users/Keagan/Desktop/camera-flash-204151.mp3")
    pygame.mixer.music.play()
    time.sleep(3)

    # Update SQLite database with new upvote count
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Ups, Ups_Total FROM backend WHERE Primary_Key=?", (primary_key,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        cleanup_thread(primary_key)
        return
    current_ups = row['Ups']
    ups_total = row['Ups_Total'] if row['Ups_Total'] else 1  
    new_ups = current_ups + 1
    now_iso = datetime.now(timezone.utc).isoformat()
    new_status = 'Done' if new_ups >= ups_total else 'Ready'
    cursor.execute("""
        UPDATE backend
        SET Ups = ?, 
            Last_Upvoted = ?, 
            Status = ?
        WHERE Primary_Key = ?
    """, (new_ups, now_iso, new_status, primary_key))
    conn.commit()
    conn.close()

    # Compute progress percentage.
    progress = new_ups / ups_total

    # 1. Update fields other than "Ups"
    update_airtable_record(primary_key, {
        "Status": new_status,
        "Progress": progress
    })
    # 2. Update only the "Ups" field separately
    update_airtable_record(primary_key, {"Ups": new_ups})

    update_airtable_upvoted_links(record_id, post_id)
    print(f"\nPost {primary_key} updated to '{new_status}' with Ups = {new_ups}.")

    cleanup_thread(primary_key)


if __name__ == '__main__':
    print("\n🚀 Initializing...")
    initialize_db()
    rank_checker_account = get_rank_checker_account()
    print("\nStarting background threads...")
    threading.Thread(target=fetch_new_posts_from_airtable, daemon=True).start()
    threading.Thread(target=check_and_upvote, daemon=True).start()
    threading.Thread(target=rank_checker, daemon=True).start()
    print("\nStarting Flask server on port 5000...")
    app.run(host='0.0.0.0', port=5000)
