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
    raise ValueError("❌ Ошибка: GOOGLE_CREDENTIALS не найдены!")

creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
])
client = gspread.authorize(creds)

spreadsheet_id = "1DpbYJ5f6zdhIl1zDtn6Z3aCHZRDFTaqhsCrkzNM9Iqo"
sheet = client.open_by_key(spreadsheet_id).sheet1
log_sheet = client.open_by_key(spreadsheet_id).worksheet("Changes Log")

all_values = sheet.get_all_values()
apps_google_play = all_values[1:]

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

def fetch_google_play_data(package_name, app_number, existing_status, existing_release_date, existing_not_found_date):
    try:
        print(f"🔍 Проверяем {package_name}...")
        time.sleep(0.5)
        data = app(package_name)

        status = "ready"
        developer_name = data.get("developer", "")
        release_date = data.get("released")
        last_updated = data.get("updated")

        def convert_timestamp(value):
            if isinstance(value, int) and value > 1000000000:
                return datetime.utcfromtimestamp(value).strftime("%Y-%m-%d")
            return value

        release_date = convert_timestamp(release_date)
        last_updated = convert_timestamp(last_updated)

        final_date = release_date if release_date else last_updated or "Не найдено"
        not_found_date = ""

        # 🛠️ Новый правильный блок логирования
        if existing_status in ["", None]:
            log_change("Загружено новое приложение", app_number, package_name)
        elif existing_status == "ban" and status == "ready":
            logs = log_sheet.get_all_values()
            found_ban = any(row[1] == "Бан приложения" and row[3] == package_name for row in logs)

            if found_ban:
                # Важно: если в старой таблице релиза не было, а сейчас появился - значит это новое появление
                if existing_release_date in ["", "Не найдено", None] and release_date not in ["", "Не найдено", None]:
                    log_change("Приложение появилось в сторе", app_number, package_name)
                else:
                    log_change("Приложение вернулось в стор", app_number, package_name)

        return [package_name, status, final_date, not_found_date, developer_name]

    except Exception:
        return None

def fetch_all_data():
    print("🚀 Запуск проверки всех приложений...")
    apps_list = []
    for row in apps_google_play:
        if len(row) >= 8 and row[7]:
            apps_list.append((row[0], row[7], row[3], row[5], row[6]))

    print(f"✅ Найдено {len(apps_list)} приложений для проверки.")

    remaining = apps_list
    results = []
    max_attempts = 5

    for attempt in range(max_attempts):
        print(f"🔁 Попытка {attempt + 1} из {max_attempts}")
        with ThreadPoolExecutor(max_workers=5) as executor:
            partial_results = list(executor.map(
                lambda x: fetch_google_play_data(x[1], x[0], x[2], x[3], x[4]), remaining
            ))

        next_remaining = []
        for i, result in enumerate(partial_results):
            if result is None:
                next_remaining.append(remaining[i])
            else:
                results.append(result)

        if not next_remaining:
            break
        if attempt < max_attempts - 1:
            print("⏳ Ожидаем 5 секунд перед следующей попыткой...")
            time.sleep(5)
        remaining = next_remaining

    for row in remaining:
        app_number, package_name, status, release, not_found = row
        not_found_date = not_found or datetime.today().strftime("%Y-%m-%d")
        results.append([package_name, "ban", release, not_found_date, ""])

    return results

def update_google_sheets(sheet, data):
    print("🔄 Перезагружаем данные из Google Sheets...")
    all_values = sheet.get_all_values()
    apps_google_play = all_values[1:]

    updates = []
    ready_count = 0
    color_updates = []

    for i, row in enumerate(apps_google_play, start=2):
        if len(row) < 8:
            continue

        app_number = row[0]
        package_name = row[7]
        old_status = row[3]
        old_release = row[5]
        old_not_found = row[6]
        old_developer = row[4]

        for app_data in data:
            if app_data[0] == package_name:
                new_status = app_data[1]
                new_release = app_data[2]
                new_not_found = app_data[3]
                new_developer = app_data[4]

                # Только если что-то изменилось
                if (old_status != new_status or old_release != new_release or
                        old_not_found != new_not_found or old_developer != new_developer):

                    updates.append({"range": f"D{i}", "values": [[new_status]]})
                    updates.append({"range": f"F{i}", "values": [[new_release]]})
                    updates.append({"range": f"G{i}", "values": [[new_not_found]]})
                    updates.append({"range": f"E{i}", "values": [[new_developer]]})

                    # Логика логирования
                    if old_status != new_status:
                        if old_status in ["", None] and new_status == "ready":
                            log_change("Загружено новое приложение", app_number, package_name)
                        elif old_status == "ban" and new_status == "ready":
                            log_change("Приложение вернулось в стор", app_number, package_name)
                        elif old_status == "ready" and new_status == "ban":
                            log_change("Бан приложения", app_number, package_name)

                if new_status == "ready":
                    ready_count += 1

                color = {"red": 0.8, "green": 1, "blue": 0.8} if new_status == "ready" else {"red": 1, "green": 0.8, "blue": 0.8}
                color_updates.append({"range": f"A{i}", "format": {"backgroundColor": color}})
                break

    if updates:
        try:
            sheet.batch_update(updates)
            print(f"✅ Данные обновлены. Доступных приложений: {ready_count}")
        except Exception as e:
            print(f"❌ Ошибка обновления данных: {e}")
    if color_updates:
        try:
            sheet.batch_format(color_updates)
        except Exception as e:
            print(f"❌ Ошибка изменения цвета ячеек: {e}")
    try:
        sheet.update(range_name="J2", values=[[ready_count]])
    except Exception as e:
        print(f"❌ Ошибка обновления счётчика доступных приложений: {e}")

def job():
    print("🔄 Начинаем обновление данных...")
    data = fetch_all_data()
    update_google_sheets(sheet, data)
    flush_log()
    print("✅ Обновление завершено!")

job()
print("✅ Скрипт завершил работу. Он запустится снова через 10 минут.")
