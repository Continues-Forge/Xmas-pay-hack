## Предварительные требования

Перед началом работы убедитесь, что у вас установлены:

- **Docker** (https://docs.docker.com/get-docker/)  
- **Docker Compose** (https://docs.docker.com/compose/install/)

---

## Установка и запуск

### 1. Клонируйте репозиторий

Склонируйте репозиторий с проектом на ваше устройство.

---

### 2. Настройте структуру папок

Создайте папку `data` в корне проекта и добавьте файлы данных:

- Файл с платежами (`payments`)
- Файл с провайдерами (`providers`)
- Файл с курсами валют (`ex_rates`)

---

### 3. Сборка Docker-образа

Для сборки Docker-образа выполните следующую команду:

```bash
docker-compose build
```

---

### 4. Запуск контейнера

Для инициализации и запуска сервиса выполните:

```bash
docker-compose up --build
```

Сервис начнёт обработку данных и сохранит результат в файл `res.csv` в корневой директории.

---

## Параметры запуска

Вы можете настроить пути к данным, изменив параметры в `docker-compose.yml`:

```yaml
command: ["python", "main.py", "--data_dir", "/app/data"]
```

### Основные параметры:

- `--data_dir` — путь до папки с данными (по умолчанию: `/app/data`).

**Обязательные файлы в папке с данными:**

- `payments` — файл с данными о платежах.
- `providers` — файл с данными о провайдерах.
- `ex_rates` — файл с курсами валют.

---

## Ожидаемый результат

После успешного выполнения скрипта в корне проекта будет создан файл `res.csv` с обработанными данными.
