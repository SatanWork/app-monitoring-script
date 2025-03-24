import gspread
import json
import os
import time
import logging
from oauth2client.service_account import ServiceAccountCredentials
from google_play_scraper import app
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(message)s')

# ------------------- Подключение к Google Sheets -------------------
logging.info("🔄 Подключаемся к Google Sheets...")
creds_json = os.getenv("GOOGLE_CREDENTIALS")
if not creds_json:
    raise ValueError("❌ GOOGLE_CREDENTIALS не найдены!")
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(
    creds_dict,
    ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
)
client = gspread.authorize(creds)

spreadsheet_id = "1DpbYJ5f6zdhIl1zDtn6Z3aCHZRDFTaqhsCrkzNM9Iqo"
sheet = client.open_by_key(spreadsheet_id).sheet1
all_values = sheet.get_all_values()
apps_google_play = all_values[1:]  # Пропускаем заголовок основной таблицы

# ------------------- Подключение к листу "Changes Log" -------------------
try:
    log_sheet = client.open_by_key(spreadsheet_id).worksheet("Changes Log")
except gspread.exceptions.WorksheetNotFound:
    log_sheet = client.open_by_key(spreadsheet_id).add_worksheet(title="Changes Log", rows="1000", cols="4")
    # Заголовки: Дата изменения, Тип изменения, Номер приложения, Package
    log_sheet.append_row(["Дата изменения", "Тип изменения", "Номер приложения", "Package"])

# ------------------- Кэш логов -------------------
# Ключ – (Тип изменения, Номер приложения, Package)
existing_log_keys = set()

def init_existing_log_keys():
    global existing_log_keys
    try:
        logs = log_sheet.get_all_values()[1:]
        for row in logs:
            if len(row) >= 4:
                key = (row[1], row[2], row[3])
                existing_log_keys.add(key)
    except Exception as e:
        logging.error(f"❌ Ошибка при чтении логов: {e}")

init_existing_log_keys()

# ------------------- Удаление дублей в листе логов -------------------
def remove_duplicates_from_log():
    try:
        all_logs = log_sheet.get_all_values()
        if not all_logs:
            return
        headers = all_logs[0]
        entries = all_logs[1:]
        seen = set()
        cleaned = [headers]
        for row in entries:
            if len(row) >= 4:
                key = (row[1], row[2], row[3])
                if key not in seen:
                    seen.add(key)
                    cleaned.append(row)
        if len(cleaned) != len(all_logs):
            log_sheet.clear()
            log_sheet.append_rows(cleaned)
            logging.info(f"🧹 Удалено {len(all_logs) - len(cleaned)} дублей.")
            # Обновляем кэш
            init_existing_log_keys()
    except Exception as e:
        logging.error(f"❌ Ошибка при удалении дублей: {e}")

# ------------------- Проверка и удаление записи "Бан приложения" -------------------
def check_ban_log_exists(package_name, app_number):
    key = ("Бан приложения", app_number, package_name)
    return key in existing_log_keys

def remove_old_ban_log(package_name, app_number):
    try:
        all_logs = log_sheet.get_all_values()
        updated_logs = []
        removed = False
        for row in all_logs:
            if len(row) >= 4 and row[1] == "Бан приложения" and row[2] == app_number and row[3] == package_name:
                removed = True
            else:
                updated_logs.append(row)
        if removed:
            log_sheet.clear()
            log_sheet.append_rows(updated_logs)
            logging.info(f"🗑️ Удалена запись 'Бан приложения' для {package_name} (№ {app_number})")
            # Обновляем кэш
            init_existing_log_keys()
    except Exception as e:
        logging.error(f"❌ Ошибка удаления записи: {e}")

# ------------------- Логирование изменений (с буфером) -------------------
log_buffer = []

def log_change(change_type, app_number, package_name):
    date_str = datetime.today().strftime("%Y-%m-%d")
    key = (change_type, app_number, package_name)
    # Если запись уже есть – пропускаем
    if key in existing_log_keys:
        logging.info(f"⚠️ Запись уже существует: {change_type} – {app_number}")
        return
    for entry in log_buffer:
        if (entry[1], entry[2], entry[3]) == key:
            logging.info(f"⚠️ Запись уже в буфере: {change_type} – {app_number}")
            return
    # Если приложение вернулось в стор, удаляем предыдущий бан
    if change_type == "Приложение вернулось в стор":
        remove_old_ban_log(package_name, app_number)
    log_buffer.append([date_str, change_type, app_number, package_name])
    existing_log_keys.add(key)
    logging.info(f"📌 Лог: {change_type} – {app_number} ({package_name})")

def flush_log():
    global log_buffer
    if log_buffer:
        try:
            log_sheet.append_rows(log_buffer)
            logging.info(f"✅ В лог записано: {len(log_buffer)} строк.")
            log_buffer = []
        except Exception as e:
            logging.error(f"❌ Ошибка записи в лог: {e}")

# ------------------- Проверка одного приложения -------------------
def fetch_google_play_data(package_name, app_number, existing_status, existing_release_date, existing_not_found_date):
    try:
        logging.info(f"🔍 Проверяем {package_name} (№ {app_number})...")
        time.sleep(0.5)
        details = app(package_name)
        status = "ready"
        def convert_timestamp(val):
            if isinstance(val, int) and val > 1000000000:
                return datetime.utcfromtimestamp(val).strftime("%Y-%m-%d")
            return val
        release_date = convert_timestamp(details.get("released"))
        last_updated = convert_timestamp(details.get("updated"))
        final_date = release_date or last_updated or "Не найдено"
        not_found_date = ""
        logging.info(f"📅 Дата: {final_date}")
        logging.info(f"🔄 Статус: {existing_status} → {status}")
        if existing_status in ["", None]:
            log_change("Загружено новое приложение", app_number, package_name)
        elif existing_status == "ban" and status == "ready":
            if check_ban_log_exists(package_name, app_number):
                log_change("Приложение вернулось в стор", app_number, package_name)
            else:
                log_change("Приложение появилось в сторе", app_number, package_name)
        return [package_name, status, final_date, not_found_date]
    except Exception as e:
        logging.error(f"❌ Ошибка: {e}")
        status = "ban"
        not_found_date = existing_not_found_date or datetime.today().strftime("%Y-%m-%d")
        if existing_status in ["", None]:
            log_change("Загружено новое приложение", app_number, package_name)
        elif existing_status not in ["ban", None, ""]:
            if not check_ban_log_exists(package_name, app_number):
                log_change("Бан приложения", app_number, package_name)
            else:
                logging.info(f"⚠️ Повтор записи в логе – пропускаем: {package_name}")
        return [package_name, status, existing_release_date, not_found_date]

# ------------------- Проверка всех приложений -------------------
def fetch_all_data():
    logging.info("🚀 Старт проверки всех приложений...")
    apps_list = [
        (row[0], row[7], row[3], row[5], row[6])
        for row in apps_google_play if len(row) >= 8 and row[7]
    ]
    with ThreadPoolExecutor(max_workers=5) as executor:
        return list(executor.map(lambda x: fetch_google_play_data(*x), apps_list))

# ------------------- Обновление основной таблицы -------------------
def update_google_sheets(sheet, data):
    logging.info("📋 Обновляем основную таблицу...")
    all_values = sheet.get_all_values()
    main_rows = all_values[1:]
    updates = []
    color_updates = []
    ready_count = 0
    for i, row in enumerate(main_rows, start=2):
        package = row[7]
        for app_data in data:
            if app_data[0] == package:
                updates.extend([
                    {"range": f"D{i}", "values": [[app_data[1]]]},
                    {"range": f"F{i}", "values": [[app_data[2]]]},
                    {"range": f"G{i}", "values": [[app_data[3]]]}
                ])
                if app_data[1] == "ready":
                    ready_count += 1
                # Обновление цвета ячейки: зеленый для ready, красный для ban
                color = {"red": 0.8, "green": 1, "blue": 0.8} if app_data[1] == "ready" else {"red": 1, "green": 0.8, "blue": 0.8}
                color_updates.append({"range": f"A{i}", "format": {"backgroundColor": color}})
                break
    if updates:
        try:
            sheet.batch_update(updates)
            logging.info(f"✅ Обновлено {len(updates)//3} строк.")
        except Exception as e:
            logging.error(f"❌ Ошибка обновления данных: {e}")
    if color_updates:
        try:
            sheet.batch_format(color_updates)
        except Exception as e:
            logging.error(f"❌ Ошибка изменения цвета ячеек: {e}")
    try:
        # Обновляем счётчик (новый порядок аргументов: values, range_name)
        sheet.update(values=[[ready_count]], range_name="J2")
    except Exception as e:
        logging.error(f"❌ Ошибка обновления счётчика ready: {e}")

# ------------------- Главная функция -------------------
def job():
    logging.info("🔁 Начинаем обновление...")
    data = fetch_all_data()
    update_google_sheets(sheet, data)
    flush_log()  # Записываем логи батчем
    remove_duplicates_from_log()  # Удаляем дубли в логах
    logging.info("✅ Обновление завершено!")

if __name__ == "__main__":
    job()
    logging.info("🕒 Скрипт завершён. Следующий запуск через 5 минут.")
