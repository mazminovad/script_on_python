import requests
import json

# Настройки
old_sentry_url = "https://sentry.phrm.tech/api/0/projects/expero/pharmix_catalog_server/"
new_sentry_url = "http://87.242.88.133/api/0/teams/sentry/sentry/projects/"
api_token_old = "68499a658cf74d71b929df16c33a80807858494f1fa94d818560180b8326357b"
api_token_new = "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"

headers_old = {
    "Authorization": f"Bearer {api_token_old}",
    "Content-Type": "application/json"
}

headers_new = {
    "Authorization": f"Bearer {api_token_new}",
    "Content-Type": "application/json"
}

# Получение данных о проектах из старой инстанции
response_old = requests.get(old_sentry_url, headers=headers_old)

# Проверка статуса ответа
if response_old.status_code == 200:
    try:
        project = response_old.json()
        print(f"Получены данные о проекте: {json.dumps(project, indent=4)}")
    except ValueError as e:
        print("Ошибка при разборе JSON:", e)
        project = None
else:
    print(f"Не удалось получить данные о проекте. Статус код: {response_old.status_code}")
    project = None

# Словарь для хранения данных о проекте и его DSN ключах
project_data = {}

if project:
    project_slug = project.get("slug")
    project_name = project.get("name")
    
    # Проверка на наличие слуга и имени проекта
    if project_slug and project_name:
        # Получение DSN ключей для проекта
        dsn_url = f"https://sentry.phrm.tech/api/0/projects/expero/{project_slug}/keys/"
        response_dsn = requests.get(dsn_url, headers=headers_old)
        
        if response_dsn.status_code == 200:
            try:
                dsn_keys = response_dsn.json()
                print(f"Получены DSN ключи для проекта {project_name}: {json.dumps(dsn_keys, indent=4)}")
            except ValueError as e:
                print("Ошибка при разборе JSON ключей DSN:", e)
                dsn_keys = []
        else:
            print(f"Не удалось получить DSN ключи для проекта {project_slug}. Статус код: {response_dsn.status_code}")
            dsn_keys = []
        
        project_data[project_slug] = {
            "name": project_name,
            "dsn_keys": dsn_keys,
            "other_project_data": project  # Сохраняем все данные о проекте
        }
        print(f"Получены данные для проекта: {project_name} ({project_slug})")
    else:
        print("Проект не содержит необходимую информацию (slug или name).")
else:
    print("Нет данных о проекте для обработки.")

# Вывод собранных данных в консоль для проверки
print(json.dumps(project_data, indent=4))

# Сохранение данных в файл
file_path = "projects_data.json"
with open(file_path, "w") as file:
    json.dump(project_data, file, indent=4)

print(f"Данные сохранены в {file_path}")

# Создание нового проекта на новой инстанции Sentry
for slug, data in project_data.items():
    project_payload = {
        "name": data["name"],
        "slug": slug,
        # Добавьте другие необходимые параметры проекта, если нужно
    }
    
    response_new = requests.post(new_sentry_url, headers=headers_new, json=project_payload)
    
    if response_new.status_code == 201:
        print(f"Проект {data['name']} ({slug}) успешно создан в новой инстанции.")
        # Получение DSN ключа нового проекта
        new_project_slug = response_new.json()["slug"]
        new_dsn_url = f"http://87.242.88.133/api/0/projects/sentry/{new_project_slug}/keys/"
        
        # Копирование DSN ключей в новый проект
        for key in data["dsn_keys"]:
            dsn_payload = {
                "name": key["name"],
                "public": key["public"],
                "secret": key["secret"]
            }
            response_new_dsn = requests.post(new_dsn_url, headers=headers_new, json=dsn_payload)
            if response_new_dsn.status_code == 201:
                print(f"DSN ключ {key['name']} успешно создан для проекта {data['name']} ({new_project_slug}) в новой инстанции.")
            else:
                print(f"Не удалось создать DSN ключ {key['name']} для проекта {data['name']} ({new_project_slug}). Статус код: {response_new_dsn.status_code}")
    else:
        print(f"Не удалось создать проект {data['name']} ({slug}) в новой инстанции. Статус код: {response_new.status_code}")
