from flask import Flask, request, jsonify
import sqlite3
import threading
import time
import schedule
from datetime import datetime, timezone
import os

app = Flask(__name__)

# Path to SQLite on Render persistent disk
DB_PATH = '"/tmp/reddit_posts.db"


def get_sqlite_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db():
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            Primary_Key TEXT PRIMARY KEY,
            Account_ID TEXT,
            Link TEXT,
            Subreddit TEXT,
            Status TEXT,
            Username TEXT,
            Ups INTEGER,
            Ups_Total INTEGER,
            Speed INTEGER,
            Last_Upvoted TEXT
        )
    ''')
    conn.commit()
    conn.close()


@app.route('/webhook/upvote_complete', methods=['POST'])
def upvote_complete():
    data = request.json
    primary_key = data.get('Primary_Key')
    new_ups = data.get('Ups', 0)

    if not primary_key:
        return jsonify({"error": "Missing Primary_Key"}), 400

    conn = get_sqlite_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE posts SET Last_Upvoted = ?, Ups = ? WHERE Primary_Key = ?",
        (datetime.now(timezone.utc).isoformat(), new_ups, primary_key)
    )
    conn.commit()
    conn.close()

    return jsonify({"status": "success"}), 200


def check_and_upvote():
    print(f"Checking for posts to upvote... {datetime.now()}")
    # You can paste your actual upvote_scheduler.py logic here!
    # For now just a placeholder.
    conn = get_sqlite_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT Primary_Key, Status FROM posts WHERE Status = 'Ready'")
    posts = cursor.fetchall()

    for post in posts:
        primary_key = post['Primary_Key']
        print(f"Would send upvote request for post {primary_key}")

    conn.close()


# Background scheduler thread
def run_scheduler():
    schedule.every(10).minutes.do(check_and_upvote)
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == '__main__':
    initialize_db()
    threading.Thread(target=run_scheduler, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
