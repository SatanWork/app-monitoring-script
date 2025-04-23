import gspread
import json
import os
import time
from oauth2client.service_account import ServiceAccountCredentials
from google_play_scraper import app
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

print("🔄 Подключаемся к Google Sheets...")

creds_json = os.getenv("GOOGLE_CREDENTIALS")
if not creds_json:
    raise ValueError("❌ GOOGLE_CREDENTIALS не найдены!")

creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
])
client = gspread.authorize(creds)

spreadsheet_id = "1DpbYJ5f6zdhIl1zDtn6Z3aCHZRDFTaqhsCrkzNM9Iqo"
sheet = client.open_by_key(spreadsheet_id).sheet1
log_sheet = client.open_by_key(spreadsheet_id).worksheet("Changes Log")

log_buffer = []

def remove_old_ban_log(package_name):
    try:
        all_logs = log_sheet.get_all_values()
        updated_logs = []
        removed = False

        for row in all_logs:
            if len(row) >= 4 and row[1] == "Бан приложения" and row[3] == package_name:
                removed = True
            else:
                updated_logs.append(row)

        if removed:
            log_sheet.clear()
            log_sheet.append_rows(updated_logs)
            print(f"🗑️ Удалена старая запись 'Бан приложения' для {package_name}")
    except Exception as e:
        print(f"❌ Ошибка при очистке старого лога: {e}")

def log_change(change_type, app_number, package_name):
    print(f"📌 Логируем: {change_type} - {package_name}")
    if change_type == "Приложение вернулось в стор":
        remove_old_ban_log(package_name)
    log_buffer.append([datetime.today().strftime("%Y-%m-%d"), change_type, app_number, package_name])

def flush_log():
    global log_buffer
    if log_buffer:
        try:
            log_sheet.append_rows(log_buffer)
            print(f"✅ В лог записано {len(log_buffer)} изменений.")
            log_buffer = []
        except Exception as e:
            print(f"❌ Ошибка записи в 'Changes Log': {e}")

def convert_timestamp(value):
    if isinstance(value, int) and value > 1000000000:
        return datetime.utcfromtimestamp(value).strftime("%Y-%m-%d")
    return value

def fetch_google_play_data(package_name):
    try:
        time.sleep(0.5)
        data = app(package_name)
        developer = data.get("developer", "")
        release = convert_timestamp(data.get("released"))
        updated = convert_timestamp(data.get("updated"))
        final_date = release or updated or "Не найдено"
        return {
            "package": package_name,
            "status": "ready",
            "release": final_date,
            "not_found_date": "",
            "developer": developer,
        }
    except Exception:
        return {
            "package": package_name,
            "status": "ban",
            "release": "",
            "not_found_date": datetime.today().strftime("%Y-%m-%d"),
            "developer": ""
        }

def retry_fetch(apps, retries=3, delay=30):
    for attempt in range(retries):
        print(f"🔁 Попытка {attempt + 1} из {retries}")
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(lambda pkg: fetch_google_play_data(pkg[1]), apps))

        not_found = [app for app, result in zip(apps, results) if result["status"] == "ban" and result["developer"] == ""]
        found = [result for result in results if result["status"] == "ready" or result["developer"]]

        if not not_found or attempt == retries - 1:
            return found + [fetch_google_play_data(pkg[1]) for pkg in not_found]
        time.sleep(delay)

def update_google_sheets(data):
    print("🔄 Загружаем актуальные данные из таблицы...")
    current_data = sheet.get_all_values()
    apps_google_play = current_data[1:]

    updates = []
    ready_count = 0
    color_updates = []

    for i, row in enumerate(apps_google_play, start=2):
        if len(row) < 8 or not row[7]:
            continue
        package = row[7]
        match = next((d for d in data if d["package"] == package), None)
        if not match:
            continue

        old_status = row[3]
        new_status = match["status"]

        if old_status != new_status:
            if old_status == "ban" and new_status == "ready":
                log_change("Приложение вернулось в стор", row[0], package)
            elif old_status not in ["ban", "", None] and new_status == "ban":
                log_change("Бан приложения", row[0], package)
            elif old_status in ["", None] and new_status == "ready":
                log_change("Загружено новое приложение", row[0], package)

        updates.extend([
            {"range": f"D{i}", "values": [[new_status]]},
            {"range": f"F{i}", "values": [[match['release']]]},
            {"range": f"G{i}", "values": [[match['not_found_date']]]},
            {"range": f"E{i}", "values": [[match['developer']]]},
        ])

        color = {"red": 0.8, "green": 1, "blue": 0.8} if new_status == "ready" else {"red": 1, "green": 0.8, "blue": 0.8}
        color_updates.append({"range": f"A{i}", "format": {"backgroundColor": color}})

        if new_status == "ready":
            ready_count += 1

    if updates:
        try:
            sheet.batch_update(updates)
            print("✅ Таблица обновлена.")
        except Exception as e:
            print(f"❌ Ошибка при обновлении: {e}")

    if color_updates:
        try:
            sheet.batch_format(color_updates)
        except Exception as e:
            print(f"❌ Ошибка форматирования: {e}")

    try:
        sheet.update(range_name="J2", values=[[ready_count]])
    except Exception as e:
        print(f"❌ Ошибка обновления счётчика: {e}")

def job():
    print("🚀 Старт задачи...")
    apps_list = [row for row in apps_google_play if len(row) >= 8 and row[7]]
    data = retry_fetch(apps_list)
    update_google_sheets(data)
    flush_log()
    print("✅ Задача завершена.")

job()
print("⏳ Скрипт завершил работу. Он запустится снова через 10 минут.")
