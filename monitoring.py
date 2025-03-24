import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials
from google_play_scraper import app
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time

# 🔄 Подключение к Google Sheets
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
all_values = sheet.get_all_values()
apps_google_play = all_values[1:]

# 🧾 Подключение к Changes Log
try:
    log_sheet = client.open_by_key(spreadsheet_id).worksheet("Changes Log")
except gspread.exceptions.WorksheetNotFound:
    log_sheet = client.open_by_key(spreadsheet_id).add_worksheet(title="Changes Log", rows="1000", cols="4")
    log_sheet.append_row(["Дата изменения", "Тип изменения", "Номер приложения", "Package"])

# 🧹 Удаление дублей по типу, номеру и package
def remove_duplicates_from_log():
    try:
        all_logs = log_sheet.get_all_values()
        if not all_logs:
            return

        headers = all_logs[0]
        entries = all_logs[1:]
        seen = {}

        for row in entries:
            if len(row) >= 4:
                key = (row[1], row[2], row[3])
                seen[key] = row

        cleaned_logs = [headers] + list(seen.values())
        log_sheet.clear()
        log_sheet.append_rows(cleaned_logs)
        print(f"🧹 Удалено {len(entries) - len(seen)} дублей.")
    except Exception as e:
        print(f"❌ Ошибка при удалении дублей: {e}")

# 🔎 Есть ли запись "Бан приложения"
def check_ban_log_exists(package_name):
    try:
        for row in log_sheet.get_all_values():
            if len(row) >= 4 and row[1] == "Бан приложения" and row[3] == package_name:
                return True
        return False
    except Exception as e:
        print(f"❌ Ошибка проверки лога: {e}")
        return False

# 🗑️ Удаление "Бан приложения"
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
        print(f"❌ Ошибка удаления записи: {e}")

# 📝 Логирование изменений
log_buffer = []

def log_change(change_type, app_number, package_name):
    print(f"📌 Лог: {change_type} – {package_name}")
    if change_type == "Приложение вернулось в стор":
        remove_old_ban_log(package_name)
    log_buffer.append([datetime.today().strftime("%Y-%m-%d"), change_type, app_number, package_name])

def flush_log():
    global log_buffer
    if log_buffer:
        try:
            log_sheet.append_rows(log_buffer)
            print(f"✅ В лог записано: {len(log_buffer)} строк.")
            log_buffer = []
        except Exception as e:
            print(f"❌ Ошибка записи в лог: {e}")

# 📲 Проверка одного приложения
def fetch_google_play_data(package_name, app_number, existing_status, existing_release_date, existing_not_found_date):
    try:
        print(f"🔍 Проверяем {package_name}...")
        time.sleep(0.5)
        data = app(package_name)
        status = "ready"

        def convert_timestamp(value):
            if isinstance(value, int) and value > 1000000000:
                return datetime.utcfromtimestamp(value).strftime("%Y-%m-%d")
            return value

        release_date = convert_timestamp(data.get("released"))
        last_updated = convert_timestamp(data.get("updated"))
        final_date = release_date or last_updated or "Не найдено"
        not_found_date = ""

        print(f"📅 Дата: {final_date}")
        print(f"🔄 Статус: {existing_status} → {status}")

        if existing_status in ["", None]:
            log_change("Загружено новое приложение", app_number, package_name)
        elif existing_status == "ban" and status == "ready":
            if check_ban_log_exists(package_name):
                log_change("Приложение вернулось в стор", app_number, package_name)
            else:
                log_change("Приложение появилось в сторе", app_number, package_name)

        return [package_name, status, final_date, not_found_date]

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        status = "ban"
        not_found_date = existing_not_found_date or datetime.today().strftime("%Y-%m-%d")

        if existing_status in ["", None]:
            log_change("Загружено новое приложение", app_number, package_name)
        elif existing_status not in ["ban", None, ""]:
            log_change("Бан приложения", app_number, package_name)

        return [package_name, status, existing_release_date, not_found_date]

# 🚀 Проверка всех приложений
def fetch_all_data():
    print("🚀 Старт проверки всех приложений...")
    apps_list = [
        (row[0], row[7], row[3], row[5], row[6])
        for row in apps_google_play if len(row) >= 8 and row[7]
    ]

    with ThreadPoolExecutor(max_workers=5) as executor:
        return list(executor.map(lambda x: fetch_google_play_data(*x), apps_list))

# 🧾 Обновление таблицы
def update_google_sheets(sheet, data):
    print("📋 Обновляем таблицу...")
    all_values = sheet.get_all_values()
    apps_google_play = all_values[1:]

    updates = []
    color_updates = []
    ready_count = 0

    for i, row in enumerate(apps_google_play, start=2):
        package_name = row[7]
        for app_data in data:
            if app_data[0] == package_name:
                updates += [
                    {"range": f"D{i}", "values": [[app_data[1]]]},
                    {"range": f"F{i}", "values": [[app_data[2]]]},
                    {"range": f"G{i}", "values": [[app_data[3]]]}
                ]
                if app_data[1] == "ready":
                    ready_count += 1

                color = {"red": 0.8, "green": 1, "blue": 0.8} if app_data[1] == "ready" else {"red": 1, "green": 0.8, "blue": 0.8}
                color_updates.append({"range": f"A{i}", "format": {"backgroundColor": color}})
                break

    if updates:
        try:
            sheet.batch_update(updates)
            print(f"✅ Обновлено {len(updates)//3} строк.")
        except Exception as e:
            print(f"❌ Ошибка обновления данных: {e}")

    if color_updates:
        try:
            sheet.batch_format(color_updates)
        except Exception as e:
            print(f"❌ Ошибка цвета ячеек: {e}")

    try:
        sheet.update("J2", [[ready_count]])
    except Exception as e:
        print(f"❌ Ошибка счётчика ready: {e}")

# 🔁 Главная функция
def job():
    print("🔁 Начинаем обновление...")
    data = fetch_all_data()
    update_google_sheets(sheet, data)
    flush_log()
    remove_duplicates_from_log()
    print("✅ Обновление завершено!")

job()
print("🕒 Скрипт завершён. Следующий запуск через 5 минут.")
