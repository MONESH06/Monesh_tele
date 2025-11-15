#!/usr/bin/env python3
"""
Driver Log Bot for Telegram using python-telegram-bot.

Features:
- /start: welcome & check registration
- /register: multi-step capture of driver name
- /log_drive: multi-step capture of vehicle, date, odo start, odo end
- Business logic implements 50 km HA cycle rule and sets HA expiry as 3 months after a successful clearing drive.
- Stores persistent "database" as JSON on disk (simulated DB).
- Appends every drive as a new record to an Excel file.

Dependencies:
- python-telegram-bot >= 20.x
- pandas
- openpyxl
- python-dateutil (optional; fallback to 90 days if not installed)

Install:
pip install python-telegram-bot pandas openpyxl python-dateutil
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from telegram import Update, ForceReply
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# Try to import relativedelta for more accurate "3 months" calculation
try:
    from dateutil.relativedelta import relativedelta

    def add_three_months(d: datetime):
        return d + relativedelta(months=3)
except Exception:
    # Fallback: 90 days if python-dateutil is not installed
    def add_three_months(d: datetime):
        return d + timedelta(days=90)


# ----------------------------
# Configuration & file paths
# ----------------------------
TOKEN = "8184965672:AAFEWf6AeK_op4k3cvpE2zz2Cic9-KYGXUw"  # <-- set your bot token here or via env var

DATA_FILE = Path("driver_db.json")
EXCEL_FILE = Path("driver_drives.xlsx")

# ----------------------------
# Conversation states
# ----------------------------
(
    REGISTER_NAME,
    LOG_VEHICLE,
    LOG_DATE,
    LOG_ODO_START,
    LOG_ODO_END,
) = range(5)

# ----------------------------
# In-memory "database" structure
# ----------------------------
# format:
# {
#   "telegram_user_id_str": {
#       "driver_name": "...",
#       "current_deficit": 0.0,
#       "ha_expiry": "HA status pending." or "YYYY-MM-DD",
#       "drives": [ { 'date': 'YYYY-MM-DD', 'vehicle': '...', 'odo_start': X, 'odo_end': Y, 'distance': Z, 'timestamp': 'ISO' }, ... ]
#   }
# }
db = {}


# ----------------------------
# Persistence helpers
# ----------------------------
def load_data():
    global db
    if DATA_FILE.exists():
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                db = json.load(f)
        except Exception as e:
            print(f"[load_data] failed loading {DATA_FILE}: {e}")
            db = {}
    else:
        db = {}


def save_data():
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(db, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[save_data] failed to save {DATA_FILE}: {e}")


def append_drive_to_excel(user_id: str, driver_name: str, drive_record: dict, cumulative_mileage: float, current_deficit: float, ha_expiry: str):
    """
    Append the drive as a new row to EXCEL_FILE.
    Columns: timestamp, telegram_user_id, driver_name, vehicle, date_of_drive, odo_start, odo_end, distance, cumulative_mileage, current_deficit, ha_expiry
    """
    row = {
        "timestamp": drive_record.get("timestamp"),
        "telegram_user_id": user_id,
        "driver_name": driver_name,
        "vehicle": drive_record.get("vehicle"),
        "date_of_drive": drive_record.get("date"),
        "odo_start": drive_record.get("odo_start"),
        "odo_end": drive_record.get("odo_end"),
        "distance": drive_record.get("distance"),
        "cumulative_mileage": cumulative_mileage,
        "current_deficit": current_deficit,
        "ha_expiry": ha_expiry,
    }

    df_row = pd.DataFrame([row])
    # If file exists, append; otherwise create with header
    if EXCEL_FILE.exists():
        try:
            with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
                # read existing sheet names - we'll append to the first sheet (default)
                # to avoid complications, write to a sheet named "drives"
                df_row.to_excel(writer, index=False, header=False, startrow=writer.sheets.get("drives").max_row if "drives" in writer.sheets else 0, sheet_name="drives")
        except Exception:
            # fallback: read whole file and append then write entire file (slower but robust)
            existing = pd.read_excel(EXCEL_FILE, sheet_name="drives") if "drives" in pd.ExcelFile(EXCEL_FILE).sheet_names else pd.DataFrame()
            combined = pd.concat([existing, df_row], ignore_index=True)
            combined.to_excel(EXCEL_FILE, sheet_name="drives", index=False)
    else:
        # New file: create sheet "drives"
        df_row.to_excel(EXCEL_FILE, sheet_name="drives", index=False)


# ----------------------------
# Utility helpers
# ----------------------------
def get_user_record(user_id: int):
    return db.get(str(user_id))


def register_user(user_id: int, driver_name: str):
    uid = str(user_id)
    if uid not in db:
        db[uid] = {
            "driver_name": driver_name,
            "current_deficit": 0.0,
            "ha_expiry": "HA status pending.",
            "drives": [],
        }
    else:
        db[uid]["driver_name"] = driver_name  # update name
    save_data()


def add_drive_record(user_id: int, drive_record: dict):
    uid = str(user_id)
    if uid not in db:
        # Should not happen if registration is enforced, but create fallback
        db[uid] = {
            "driver_name": "Unknown",
            "current_deficit": 0.0,
            "ha_expiry": "HA status pending.",
            "drives": [],
        }
    db[uid]["drives"].append(drive_record)
    save_data()


def compute_cumulative_mileage(uid: str) -> float:
    """Sum distances of all stored drives for the user."""
    rec = db.get(uid, {})
    drives = rec.get("drives", [])
    total = 0.0
    for d in drives:
        try:
            total += float(d.get("distance", 0.0))
        except Exception:
            pass
    return total


# ----------------------------
# Bot command handlers
# ----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start - greet user and show commands if registered."""
    user = update.effective_user
    uid = user.id
    rec = get_user_record(uid)
    if rec:
        name = rec.get("driver_name", user.first_name or "Driver")
        text = (
            f"Hello, {name}! 👋\n\n"
            "You are registered. Available commands:\n"
            "/log_drive - Log a new drive\n"
            "/register - Update your name\n"
            "/status - Show your HA status\n"
            "/cancel - Cancel any ongoing operation\n"
        )
    else:
        text = (
            f"Hello, {user.first_name or 'Driver'}! 👋\n\n"
            "You are not registered yet. Please register first with /register"
        )
    await update.message.reply_text(text, reply_markup=ForceReply(selective=True))


# ----------------------------
# Registration conversation
# ----------------------------
async def register_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Please send your full Driver Name (reply with text):")
    return REGISTER_NAME


async def register_name_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = user.id
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("I didn't catch a name. Please send your full Driver Name:")
        return REGISTER_NAME

    register_user(uid, name)
    await update.message.reply_text(
        f"Thank you, {name}! ✅\nYou are now registered. Use /log_drive to log a new drive."
    )
    return ConversationHandler.END


# ----------------------------
# Log drive conversation
# ----------------------------
async def log_drive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    rec = get_user_record(user.id)
    if not rec:
        await update.message.reply_text("You are not registered yet. Please register first with /register")
        return ConversationHandler.END

    await update.message.reply_text("Enter Vehicle Number (e.g., ABC1234):")
    return LOG_VEHICLE


async def log_vehicle_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    vehicle = update.message.text.strip()
    if not vehicle:
        await update.message.reply_text("Vehicle Number cannot be empty. Please enter the Vehicle Number:")
        return LOG_VEHICLE
    context.user_data["vehicle"] = vehicle
    await update.message.reply_text("Enter Date of Drive (YYYY-MM-DD):")
    return LOG_DATE


async def log_date_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    # validate date
    try:
        date_obj = datetime.strptime(txt, "%Y-%m-%d").date()
    except ValueError:
        await update.message.reply_text("Invalid date format. Please provide date as YYYY-MM-DD (e.g., 2025-11-15):")
        return LOG_DATE

    context.user_data["date_of_drive"] = date_obj.isoformat()
    await update.message.reply_text("Enter Odometer Start (km). Numeric value only (e.g., 12345):")
    return LOG_ODO_START


async def log_odo_start_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip().replace(",", "")
    try:
        odo_start = float(txt)
        if odo_start < 0:
            raise ValueError("Negative")
    except Exception:
        await update.message.reply_text("Invalid odometer start. Please enter a positive number (e.g., 12345):")
        return LOG_ODO_START

    context.user_data["odo_start"] = odo_start
    await update.message.reply_text("Enter Odometer End (km). Numeric value only (e.g., 12380):")
    return LOG_ODO_END


async def log_odo_end_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip().replace(",", "")
    try:
        odo_end = float(txt)
        if odo_end < 0:
            raise ValueError("Negative")
    except Exception:
        await update.message.reply_text("Invalid odometer end. Please enter a positive number (e.g., 12380):")
        return LOG_ODO_END

    odo_start = context.user_data.get("odo_start")
    if odo_start is None:
        await update.message.reply_text("Unexpected error: missing start value. Please /log_drive again.")
        return ConversationHandler.END

    if odo_end < odo_start:
        await update.message.reply_text("Odometer End cannot be less than Odometer Start. Please enter Odometer End again:")
        return LOG_ODO_END

    # All data collected - compose drive record and apply business logic
    user = update.effective_user
    uid = str(user.id)
    vehicle = context.user_data.get("vehicle")
    date_of_drive_str = context.user_data.get("date_of_drive")
    odo_start = float(odo_start)
    odo_end = float(odo_end)
    distance = odo_end - odo_start
    timestamp_iso = datetime.now(timezone.utc).isoformat()

    # ensure user exists in db
    if uid not in db:
        # fallback register minimal record to avoid crashes
        db[uid] = {
            "driver_name": user.first_name or "Driver",
            "current_deficit": 0.0,
            "ha_expiry": "HA status pending.",
            "drives": [],
        }

    user_rec = db[uid]

    # Business logic variables
    current_deficit = float(user_rec.get("current_deficit", 0.0))
    # required_to_hit_50 = 50 - current_deficit (as per spec)
    required_to_hit_50 = 50.0 - current_deficit
    if required_to_hit_50 < 0:
        required_to_hit_50 = 0.0  # defensive

    # Apply rule
    if distance >= required_to_hit_50:
        # Requirement met: reset deficit, set expiry to 3 months after date_of_drive
        new_current_deficit = 0.0
        # parse date_of_drive for expiry calc
        try:
            dod_dt = datetime.strptime(date_of_drive_str, "%Y-%m-%d")
        except Exception:
            dod_dt = datetime.now()
        expiry_dt = add_three_months(dod_dt)
        ha_expiry_str = expiry_dt.strftime("%Y-%m-%d")
        message = (
            f"Logged successfully.\n"
            f"Distance for this drive: {distance:.1f} km\n"
            f"Total requirement met (needed {required_to_hit_50:.1f} km). HA cycle is reset.\n"
            f"Driver HA will expire on {ha_expiry_str}. Requirement met!"
        )
    else:
        # Not met: reduce deficit
        new_current_deficit = round(required_to_hit_50 - distance, 1)
        ha_expiry_str = "HA status pending."
        message = (
            f"Logged conduct.\n"
            f"Distance for this drive: {distance:.1f} km\n"
            f"You still need {new_current_deficit:.1f} km to reset your 3-month HA cycle. HA status pending."
        )

    # Update user record: append drive, update deficit and expiry
    drive_record = {
        "date": date_of_drive_str,
        "vehicle": vehicle,
        "odo_start": odo_start,
        "odo_end": odo_end,
        "distance": round(distance, 1),
        "timestamp": timestamp_iso,
    }
    add_drive_record(int(uid), drive_record)  # this also saves db to disk

    # update deficit & expiry in our stored record (after adding drive)
    db[uid]["current_deficit"] = new_current_deficit
    db[uid]["ha_expiry"] = ha_expiry_str
    save_data()

    # compute cumulative mileage after adding this drive
    cumulative_mileage = compute_cumulative_mileage(uid)

    # Append to Excel file (each drive is a new record)
    try:
        append_drive_to_excel(uid, user_rec.get("driver_name", ""), drive_record, cumulative_mileage, new_current_deficit, ha_expiry_str)
    except Exception as e:
        # do not fail the whole flow; just log locally and notify user minimal info
        print(f"[append_drive_to_excel] failed: {e}")

    # final reply (single message requirement) - include total mileage and HA status
    final_reply = (
        f"{message}\n\n"
        f"Total Mileage (cumulative): {cumulative_mileage:.1f} km\n"
        f"Current Deficit: {new_current_deficit:.1f} km\n"
        f"HA Expiry: {ha_expiry_str}"
    )

    await update.message.reply_text(final_reply)
    # clear user_data used during conversation
    context.user_data.clear()
    return ConversationHandler.END


# ----------------------------
# Status & cancel handlers
# ----------------------------
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    rec = get_user_record(user.id)
    if not rec:
        await update.message.reply_text("You are not registered. Use /register to register.")
        return
    uid = str(user.id)
    cumulative = compute_cumulative_mileage(uid)
    await update.message.reply_text(
        f"Driver: {rec.get('driver_name')}\n"
        f"Total cumulative mileage: {cumulative:.1f} km\n"
        f"Current deficit: {float(rec.get('current_deficit', 0.0)):.1f} km\n"
        f"HA Expiry: {rec.get('ha_expiry')}"
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END


# ----------------------------
# Error handler
# ----------------------------
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    # Log error and inform user politely
    print(f"Exception while handling an update: {context.error}")
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text("An unexpected error occurred. Please try again later.")
    except Exception:
        pass


# ----------------------------
# Main: build and run the bot
# ----------------------------
def main():
    # Load data from disk
    load_data()

    # Allow token override via env var for convenience
    global TOKEN
    TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", TOKEN)
    if not TOKEN or len(TOKEN) < 40:
        print("ERROR: Invalid or missing Telegram bot token.")
        return

    # Build application
    app = ApplicationBuilder().token(TOKEN).build()

    # Registration Conversation
    register_conv = ConversationHandler(
        entry_points=[CommandHandler("register", register_command)],
        states={
            REGISTER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_name_received)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_user=True,
        per_chat=True,
    )

    # Log Drive Conversation
    log_drive_conv = ConversationHandler(
        entry_points=[CommandHandler("log_drive", log_drive_command)],
        states={
            LOG_VEHICLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, log_vehicle_received)],
            LOG_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, log_date_received)],
            LOG_ODO_START: [MessageHandler(filters.TEXT & ~filters.COMMAND, log_odo_start_received)],
            LOG_ODO_END: [MessageHandler(filters.TEXT & ~filters.COMMAND, log_odo_end_received)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_user=True,
        per_chat=True,
    )

    # Add handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(register_conv)
    app.add_handler(log_drive_conv)
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("cancel", cancel))

    # global error handler
    app.add_error_handler(error_handler)

    # Start polling
    print("Bot started. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=None)


if __name__ == "__main__":
    main()
