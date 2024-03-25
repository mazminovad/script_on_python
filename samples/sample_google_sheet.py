import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import os
import psycopg2
from datetime import datetime

# Подключение к БД
conn = psycopg2.connect(
    host="localhost",
    port=****,  # Поправьте порт, если он у вас отличается
    database="p_prod",
    user="******",
    password="******"
)

CREDENTIALS_FILE = 'project-******.json'  # имя файла с закрытым ключом
CREDENTIALS_PATH = os.path.abspath(CREDENTIALS_FILE)

credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
driveService = apiclient.discovery.build('drive', 'v3', http = httpAuth)
spreadsheetService = apiclient.discovery.build('sheets', 'v4', http=httpAuth)

# Создание новой таблицы
spreadsheet = {
    'properties': {
        'title': 'Test'
    }
}
spreadsheet = spreadsheetService.spreadsheets().create(body=spreadsheet,
                                                        fields='spreadsheetId').execute()
spreadsheet_id = spreadsheet['spreadsheetId']

shareRes = driveService.permissions().create(
    fileId = spreadsheet_id,
    body = {'type': 'anyone', 'role': 'writer'},  # доступ на чтение кому угодно
    fields = 'id'
).execute()

# Запрос к БД
cur = conn.cursor()
cur.execute("""select "Id", "CreatedAt", "Email" ,
"Phone", "Name", "Surname"
from accounts where "Deleted" = 'false' and "CreatedAt">= now() AT TIME ZONE 'UTC' - INTERVAL '8d'""")
rows = cur.fetchall()

# Преобразование результатов запроса в формат, пригодный для записи в Google-таблицу
values = [[str(value) if not isinstance(value, datetime) else value.strftime('%Y-%m-%d %H:%M:%S') for value in row] for row in rows]

# Создание тела запроса для записи в Google-таблицу
body = {
    'values': values
}

# Указываем диапазон для записи данных (например, A1)
range_name = 'Sheet1!A1'

# Выполняем запрос на запись данных в таблицу
result = spreadsheetService.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    range=range_name,
    valueInputOption='RAW',
    body=body
).execute()

print("Таблица успешно создана.")
print('Данные успешно добавлены в Google-таблицу.')
print("Создана новая таблица: ", spreadsheet_id)
print("Таблица создана с использованием файла ключа:", CREDENTIALS_PATH)
