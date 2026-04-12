import os
import re
import csv
import sqlite3
import asyncio
from io import StringIO
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Tuple

import httpx
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

# ============================================================================
# CONSTANTS
# ============================================================================
APP_TITLE = "Discord Quest Tracker"
DB_PATH = os.getenv("QUEST_DB_PATH", "quest_tracker.db")
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")

DISCORD_API_BASE = "https://discord.com/api/v10"

# Fixed constants
CHANNEL_ID = "1399389889677492423"
GUILD_ID = "1399389887144263690"

# WIB fixed offset
WIB = timezone(timedelta(hours=7))

# ============================================================================
# APP INITIALIZATION
# ============================================================================
app = FastAPI(title=APP_TITLE)
templates = Jinja2Templates(directory="templates")


# ============================================================================
# DATABASE HELPERS
# ============================================================================
def get_db_connection() -> sqlite3.Connection:
    """Create a new database connection with row factory enabled."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def close_db_connection(conn: Optional[sqlite3.Connection]) -> None:
    """Safely close database connection."""
    if conn:
        try:
            conn.close()
        except Exception as e:
            print(f"Warning: Failed to close DB connection: {e}")


def init_db() -> None:
    """Initialize database tables if they don't exist."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS quests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            hashtag TEXT NOT NULL,
            start_wib TEXT NOT NULL,
            end_wib TEXT NOT NULL,
            created_at_utc TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS checkins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quest_id INTEGER NOT NULL,
            user_id TEXT NOT NULL,
            username TEXT NOT NULL,
            day_index INTEGER NOT NULL,
            message_id TEXT NOT NULL,
            message_time_utc TEXT NOT NULL,
            content TEXT NOT NULL,
            FOREIGN KEY (quest_id) REFERENCES quests(id),
            UNIQUE(quest_id, user_id, day_index)
        )
        """)

        conn.commit()
    finally:
        close_db_connection(conn)


# Initialize database on startup
init_db()


# ============================================================================
# DATETIME UTILITIES
# ============================================================================
def parse_wib_datetime(value: str) -> datetime:
    """
    Parse HTML datetime-local format: YYYY-MM-DDTHH:MM
    Returns datetime with WIB timezone.
    """
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=WIB)
    return dt.astimezone(WIB)


def format_wib_display(iso_str: str) -> str:
    """Format ISO datetime string to readable WIB format."""
    try:
        dt = datetime.fromisoformat(iso_str).astimezone(WIB)
        return dt.strftime("%d %b %Y %H:%M WIB")
    except (ValueError, AttributeError) as e:
        print(f"Warning: Failed to format datetime {iso_str}: {e}")
        return "Invalid date"


def discord_ts_to_datetime_utc(ts: str) -> datetime:
    """Convert Discord timestamp to UTC datetime."""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (ValueError, AttributeError) as e:
        print(f"Warning: Failed to parse Discord timestamp {ts}: {e}")
        return datetime.now(timezone.utc)


# ============================================================================
# MESSAGE & AUTHOR UTILITIES
# ============================================================================
def extract_author(message: Dict) -> Tuple[str, str]:
    """
    Extract user_id and username from message author data.
    Returns: (user_id, username)
    """
    author = message.get("author") or {}
    user_id = str(author.get("id", ""))
    username = author.get("username", "unknown")
    return user_id, username


# ============================================================================
# QUEST DAY CALCULATIONS
# ============================================================================
def get_quest_days(start_wib: datetime, end_wib: datetime) -> int:
    """Calculate number of days in quest period."""
    if end_wib <= start_wib:
        return 0
    delta = end_wib - start_wib
    return int(delta.total_seconds() // 86400)


def day_window(start_wib: datetime, day_index: int) -> Tuple[datetime, datetime]:
    """Get the start and end time for a specific day in quest."""
    day_start = start_wib + timedelta(days=day_index - 1)
    day_end = start_wib + timedelta(days=day_index)
    return day_start, day_end


def get_day_index_for_message(
    start_wib: datetime, 
    end_wib: datetime, 
    msg_time_utc: datetime
) -> Optional[int]:
    """
    Determine which day of the quest a message falls into.
    Returns day index (1-indexed) or None if outside quest period.
    """
    msg_wib = msg_time_utc.astimezone(WIB)
    if not (start_wib <= msg_wib < end_wib):
        return None
    delta = msg_wib - start_wib
    return int(delta.total_seconds() // 86400) + 1


# ============================================================================
# STREAK CALCULATIONS
# ============================================================================
def compute_streaks(total_days: int, day_map: Dict[int, bool]) -> Tuple[int, int]:
    """
    Calculate current streak and max streak from day map.
    Returns: (current_streak, max_streak)
    """
    streak = 0
    max_streak = 0

    # Calculate max streak
    for i in range(1, total_days + 1):
        if day_map.get(i, False):
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0

    # Calculate current streak (from end)
    current_streak = 0
    for i in range(total_days, 0, -1):
        if day_map.get(i, False):
            current_streak += 1
        else:
            break

    return current_streak, max_streak


# ============================================================================
# DISCORD API HELPERS
# ============================================================================
def validate_discord_token() -> None:
    """Validate that Discord token is configured."""
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN environment variable is not set")


async def discord_get(url: str, params: Optional[Dict[str, str]] = None) -> Dict:
    """Make GET request to Discord API."""
    validate_discord_token()

    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "User-Agent": "QuestTracker/1.0"
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()


async def discord_put(url: str) -> httpx.Response:
    """Make PUT request to Discord API."""
    validate_discord_token()

    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "User-Agent": "QuestTracker/1.0"
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.put(url, headers=headers)
        return r


# ============================================================================
# MESSAGE FETCHING
# ============================================================================
async def fetch_messages_in_window(
    channel_id: str,
    start_utc: datetime,
    end_utc: datetime
) -> List[Dict]:
    """
    Fetch channel messages in [start_utc, end_utc) window.
    Handles pagination and rate limiting.
    """
    validate_discord_token()

    headers = {
        "Authorization": f"Bot {DISCORD_BOT_TOKEN}",
        "User-Agent": "QuestTracker/1.0"
    }

    all_msgs: List[Dict] = []
    before: Optional[str] = None

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            params: Dict[str, str] = {"limit": "100"}
            if before:
                params["before"] = before

            url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
            r = await client.get(url, headers=headers, params=params)

            # Handle rate limiting
            if r.status_code == 429:
                data = r.json()
                retry_after = float(data.get("retry_after", 1.0))
                await asyncio.sleep(retry_after)
                continue

            r.raise_for_status()
            batch = r.json()

            if not batch:
                break

            all_msgs.extend(batch)

            # Check if we've reached the start of the window
            oldest = batch[-1]
            oldest_dt = discord_ts_to_datetime_utc(oldest["timestamp"])

            if oldest_dt < start_utc:
                break

            before = oldest["id"]

    # Filter messages to exact time window
    return [
        m for m in all_msgs
        if start_utc <= discord_ts_to_datetime_utc(m["timestamp"]) < end_utc
    ]


# ============================================================================
# LEADERBOARD BUILDING
# ============================================================================
def build_leaderboard(quest_id: int) -> Tuple[Optional[Dict], Optional[int], Optional[List[Dict]]]:
    """
    Build complete leaderboard for a quest.
    Returns: (quest_data, total_days, leaderboard)
    """
    conn = get_db_connection()
    
    try:
        quest = conn.execute(
            "SELECT * FROM quests WHERE id=?", 
            (quest_id,)
        ).fetchone()
        
        if not quest:
            return None, None, None

        start_wib = datetime.fromisoformat(quest["start_wib"]).astimezone(WIB)
        end_wib = datetime.fromisoformat(quest["end_wib"]).astimezone(WIB)
        total_days = get_quest_days(start_wib, end_wib)

        # Fetch all checkins for this quest
        rows = conn.execute("""
            SELECT user_id, username, day_index
            FROM checkins
            WHERE quest_id=?
            ORDER BY username
        """, (quest_id,)).fetchall()

        # Build per-user data
        per_user: Dict[str, Dict] = {}
        for r in rows:
            uid = r["user_id"]
            if uid not in per_user:
                per_user[uid] = {
                    "user_id": uid,
                    "username": r["username"],
                    "day_map": {}
                }
            per_user[uid]["day_map"][r["day_index"]] = True
            per_user[uid]["username"] = r["username"]

        # Build leaderboard entries - FIX: ensure all values are hashable
        leaderboard: List[Dict] = []
        for u in per_user.values():
            day_map = u["day_map"]
            points = sum(1 for i in range(1, total_days + 1) if day_map.get(i, False))
            current_streak, max_streak = compute_streaks(total_days, day_map)
            
            # Create grid safely - ensure all values are strings
            grid = [
                "✅" if day_map.get(i, False) else "" 
                for i in range(1, total_days + 1)
            ]
            
            username_str = str(u["username"]).lower()
            
            leaderboard.append({
                "user_id": str(u["user_id"]),
                "username": str(u["username"]),
                "points": int(points),
                "current_streak": int(current_streak),
                "max_streak": int(max_streak),
                "grid": grid
            })

        # Sort by points, max_streak, then username (all are hashable types)
        leaderboard.sort(
            key=lambda x: (
                -x["points"],  # negative for reverse sort
                -x["max_streak"],
                x["username_lower"] if "username_lower" in x else x["username"].lower()
            )
        )
        
        # Cleaner sort approach - create comparison key safely
        leaderboard.sort(
            key=lambda x: (-x["points"], -x["max_streak"], x["username"].lower())
        )

        return dict(quest), total_days, leaderboard
        
    finally:
        close_db_connection(conn)


# ============================================================================
# DISCORD ROLES
# ============================================================================
async def fetch_assignable_roles() -> List[Dict]:
    """Fetch list of assignable roles from Discord guild."""
    try:
        roles = await discord_get(f"{DISCORD_API_BASE}/guilds/{GUILD_ID}/roles")
        
        filtered: List[Dict] = []
        for role in roles:
            name = role.get("name", "")
            if name == "@everyone":
                continue
            if role.get("managed", False):
                continue
            
            filtered.append({
                "id": str(role["id"]),
                "name": str(name),
                "position": int(role.get("position", 0))
            })

        # Sort by position (descending)
        filtered.sort(key=lambda x: x["position"], reverse=True)
        return filtered
        
    except Exception as e:
        print(f"Error fetching roles: {e}")
        raise


# ============================================================================
# ROUTES: INDEX
# ============================================================================
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    """Display list of all quests."""
    conn = get_db_connection()
    
    try:
        quests = conn.execute(
            "SELECT * FROM quests ORDER BY id DESC"
        ).fetchall()
        
        quest_rows = []
        for q in quests:
            quest_rows.append({
                "id": int(q["id"]),
                "name": str(q["name"]),
                "hashtag": str(q["hashtag"]),
                "start_display": format_wib_display(q["start_wib"]),
                "end_display": format_wib_display(q["end_wib"]),
            })

        return templates.TemplateResponse("index.html", {
            "request": request,
            "title": APP_TITLE,
            "quests": quest_rows
        })
        
    finally:
        close_db_connection(conn)


# ============================================================================
# ROUTES: CREATE QUEST
# ============================================================================
@app.post("/quest/create")
def create_quest(
    name: str = Form(...),
    hashtag: str = Form(...),
    start_wib: str = Form(...),
    end_wib: str = Form(...),
):
    """Create a new quest."""
    s = parse_wib_datetime(start_wib)
    e = parse_wib_datetime(end_wib)

    # Auto-extend if end is before start
    if e <= s:
        e = s + timedelta(days=7)

    conn = get_db_connection()
    
    try:
        conn.execute("""
            INSERT INTO quests(name, hashtag, start_wib, end_wib, created_at_utc)
            VALUES(?,?,?,?,?)
        """, (
            name.strip(),
            hashtag.strip(),
            s.isoformat(),
            e.isoformat(),
            datetime.now(timezone.utc).isoformat()
        ))
        conn.commit()
        
        qid = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        
        return RedirectResponse(url=f"/quest/{qid}", status_code=303)
        
    finally:
        close_db_connection(conn)


# ============================================================================
# ROUTES: VIEW QUEST
# ============================================================================
@app.get("/quest/{quest_id}", response_class=HTMLResponse)
async def view_quest(request: Request, quest_id: int):
    """Display quest details and leaderboard."""
    quest, total_days, leaderboard = build_leaderboard(quest_id)
    if not quest:
        return HTMLResponse("Quest not found", status_code=404)

    roles: List[Dict] = []
    role_error: Optional[str] = None
    
    try:
        roles = await fetch_assignable_roles()
    except Exception as e:
        role_error = str(e)

    return templates.TemplateResponse("quest.html", {
        "request": request,
        "title": f"{APP_TITLE} - {quest['name']}",
        "quest": {
            "id": quest["id"],
            "name": quest["name"],
            "hashtag": quest["hashtag"],
            "start_display": format_wib_display(quest["start_wib"]),
            "end_display": format_wib_display(quest["end_wib"]),
        },
        "total_days": total_days,
        "leaderboard": leaderboard,
        "roles": roles,
        "role_error": role_error,
        "assign_summary": None
    })


# ============================================================================
# ROUTES: SYNC QUEST
# ============================================================================
@app.post("/quest/{quest_id}/sync")
async def sync_quest(quest_id: int):
    """Sync quest data from Discord messages."""
    conn = get_db_connection()
    
    try:
        quest = conn.execute(
            "SELECT * FROM quests WHERE id=?", 
            (quest_id,)
        ).fetchone()
        
        if not quest:
            return HTMLResponse("Quest not found", status_code=404)

        start_wib = datetime.fromisoformat(quest["start_wib"]).astimezone(WIB)
        end_wib = datetime.fromisoformat(quest["end_wib"]).astimezone(WIB)
        hashtag = str(quest["hashtag"])
        
    finally:
        close_db_connection(conn)

    # Fetch messages from Discord
    start_utc = start_wib.astimezone(timezone.utc)
    end_utc = end_wib.astimezone(timezone.utc)

    msgs = await fetch_messages_in_window(CHANNEL_ID, start_utc, end_utc)
    msgs.sort(key=lambda m: m["timestamp"])

    # Process messages and insert into database
    conn = get_db_connection()
    
    try:
        for m in msgs:
            # Filter message type
            if m.get("type") not in (0, 19):
                continue
            
            # Skip bot messages
            if m.get("author", {}).get("bot", False):
                continue

            # Check for hashtag
            content = (m.get("content") or "").strip()
            if hashtag.lower() not in content.lower():
                continue

            # Extract author
            user_id, username = extract_author(m)
            if not user_id:
                continue

            # Determine day index
            msg_time_utc = discord_ts_to_datetime_utc(m["timestamp"])
            day_index = get_day_index_for_message(start_wib, end_wib, msg_time_utc)
            if day_index is None:
                continue

            # Insert checkin (ignore duplicates)
            try:
                conn.execute("""
                    INSERT INTO checkins(quest_id, user_id, username, day_index, message_id, message_time_utc, content)
                    VALUES(?,?,?,?,?,?,?)
                """, (
                    quest_id,
                    user_id,
                    username,
                    day_index,
                    str(m["id"]),
                    msg_time_utc.isoformat(),
                    content
                ))
            except sqlite3.IntegrityError:
                # Duplicate entry, skip
                pass

        conn.commit()
        
    finally:
        close_db_connection(conn)

    return RedirectResponse(url=f"/quest/{quest_id}", status_code=303)


# ============================================================================
# ROUTES: EXPORT CSV
# ============================================================================
@app.get("/quest/{quest_id}/export.csv")
def export_csv(quest_id: int):
    """Export quest leaderboard as CSV."""
    quest, total_days, leaderboard = build_leaderboard(quest_id)
    if not quest:
        return HTMLResponse("Quest not found", status_code=404)

    out = StringIO()
    writer = csv.writer(out)

    # Write header
    day_headers = [f"Day {i}" for i in range(1, total_days + 1)]
    writer.writerow(["User ID", "Username"] + day_headers + ["Total Points", "Current Streak", "Max Streak"])

    # Write data rows
    for row in leaderboard:
        writer.writerow([
            row["user_id"],
            row["username"],
            *row["grid"],
            row["points"],
            row["current_streak"],
            row["max_streak"]
        ])

    out.seek(0)
    filename = f"quest_{quest_id}_export.csv"
    return StreamingResponse(
        iter([out.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# ============================================================================
# ROUTES: CLEAR SYNC
# ============================================================================
@app.post("/quest/{quest_id}/clear-sync")
def clear_sync_data(quest_id: int):
    """Clear all sync data for a quest."""
    conn = get_db_connection()
    
    try:
        quest = conn.execute(
            "SELECT id FROM quests WHERE id=?", 
            (quest_id,)
        ).fetchone()
        
        if not quest:
            return HTMLResponse("Quest not found", status_code=404)

        conn.execute("DELETE FROM checkins WHERE quest_id=?", (quest_id,))
        conn.commit()
        
    finally:
        close_db_connection(conn)

    return RedirectResponse(url=f"/quest/{quest_id}", status_code=303)


# ============================================================================
# ROUTES: DELETE QUEST
# ============================================================================
@app.post("/quest/{quest_id}/delete")
def delete_quest(quest_id: int):
    """Delete a quest and all associated data."""
    conn = get_db_connection()
    
    try:
        quest = conn.execute(
            "SELECT id FROM quests WHERE id=?", 
            (quest_id,)
        ).fetchone()
        
        if not quest:
            return HTMLResponse("Quest not found", status_code=404)

        conn.execute("DELETE FROM checkins WHERE quest_id=?", (quest_id,))
        conn.execute("DELETE FROM quests WHERE id=?", (quest_id,))
        conn.commit()
        
    finally:
        close_db_connection(conn)

    return RedirectResponse(url="/", status_code=303)


# ============================================================================
# ROUTES: ASSIGN ROLE
# ============================================================================
@app.post("/quest/{quest_id}/assign-role", response_class=HTMLResponse)
async def assign_role(
    request: Request,
    quest_id: int,
    role_id: str = Form(...),
    min_points: int = Form(1)
):
    """Assign Discord role to eligible quest participants."""
    quest, total_days, leaderboard = build_leaderboard(quest_id)
    if not quest:
        return HTMLResponse("Quest not found", status_code=404)

    roles: List[Dict] = []
    role_error: Optional[str] = None
    
    try:
        roles = await fetch_assignable_roles()
    except Exception as e:
        role_error = str(e)

    # Filter eligible users
    eligible = [u for u in leaderboard if u["points"] >= min_points]

    success_count = 0
    failed: List[Dict] = []

    # Assign role to each eligible user
    for user in eligible:
        url = f"{DISCORD_API_BASE}/guilds/{GUILD_ID}/members/{user['user_id']}/roles/{role_id}"
        try:
            r = await discord_put(url)
            if r.status_code in (204, 201):
                success_count += 1
            else:
                failed.append({
                    "username": user["username"],
                    "user_id": user["user_id"],
                    "reason": f"HTTP {r.status_code}"
                })
        except Exception as e:
            failed.append({
                "username": user["username"],
                "user_id": user["user_id"],
                "reason": str(e)
            })

    # Build summary
    assign_summary = {
        "role_id": role_id,
        "min_points": min_points,
        "eligible_count": len(eligible),
        "success_count": success_count,
        "failed_count": len(failed),
        "failed": failed[:20]
    }

    return templates.TemplateResponse("quest.html", {
        "request": request,
        "title": f"{APP_TITLE} - {quest['name']}",
        "quest": {
            "id": quest["id"],
            "name": quest["name"],
            "hashtag": quest["hashtag"],
            "start_display": format_wib_display(quest["start_wib"]),
            "end_display": format_wib_display(quest["end_wib"]),
        },
        "total_days": total_days,
        "leaderboard": leaderboard,
        "roles": roles,
        "role_error": role_error,
        "assign_summary": assign_summary
    })
