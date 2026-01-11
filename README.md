# Uppi 🐝

**Uppi** — це інструмент для автоматизованої взаємодії з порталом **Agenzia delle Entrate (AE)** та сервісом **SISTER**. 
Його основна мета — отримання кадастрових виписок (Visure Catastali) для списку клієнтів (орендодавців) та генерація атестацій (Attestazioni) для договорів оренди.

Проєкт включає:
- Веб-скрейпінг та автоматизацію браузера (Playwright + Scrapy).
- Розв'язання Captcha (2Captcha).
- Збереження даних (PostgreSQL) та файлів (S3/MinIO).
- Генерацію документів.

---

## 🛠 Вимоги до середовища

Для запуску проєкту локально вам знадобляться:

1.  **Python 3.10+**
2.  **PostgreSQL** (локально або в Docker).
3.  **S3-сумісне сховище** (MinIO локально або хмарне, наприклад R2).
4.  **Google Chrome** або інший браузер для Playwright.
5.  **2Captcha API Key** (для автоматичного проходження капчі на сайті AE).

### Встановлення залежностей

1.  Клонуйте репозиторій.
2.  Створіть віртуальне середовище:
    ```bash
    python -m venv venv
    source venv/bin/activate  # Для macOS/Linux
    # venv\Scripts\activate   # Для Windows
    ```
3.  Встановіть Python-бібліотеки:
    ```bash
    pip install -r requirements.txt
    ```
    > **Важливо**: Переконайтеся, що бібліотека `psycopg` (версії 3) встановлена, оскільки вона використовується в коді для роботи з БД (`import psycopg`), навіть якщо в `requirements.txt` вона закоментована.
    ```bash
    pip install psycopg
    ```

4.  Встановіть браузери для Playwright:
    ```bash
    playwright install
    ```

---

## ⚙️ Налаштування конфігурації

Проєкт використовує файл `.env` для збереження конфіденційних даних. Створіть файл `.env` у корені проєкту та заповніть його за зразком:

```ini
# АВТОРИЗАЦІЯ В AGENZIA ENTRATE (SISTER)
AE_USERNAME=ваш_код_користувача     # Codice Fiscale або ім'я користувача
AE_PASSWORD=ваш_пароль
AE_PIN=ваш_пін_код

# СЕРВІС РОЗПІЗНАВАННЯ CAPTCHA
TWO_CAPTCHA_API_KEY=ваш_api_key_2captcha

# БАЗА ДАНИХ (PostgreSQL)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=uppi_db
DB_USER=uppi_user
DB_PASSWORD=uppi_password
DB_SSL_MODE=prefer

# S3 СХОВИЩЕ (MinIO / R3)
S3_ENDPOINT=localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_SECURE=False                 # True для HTTPS (наприклад, Cloudflare R2)
VISURE_BUCKET=uppi-bucket
ATTESTAZIONI_BUCKET=attestazioni

#################AE AND SISTER URLS AND KEYS###################
# AE URLs 
AE_LOGIN_URL=https://iampe.agenziaentrate.gov.it/sam/UI/Login?realm=/agenziaentrate
AE_URL_SERVIZI=https://portale.agenziaentrate.gov.it/PortaleWeb/servizi

# SISTER URLs
SISTER_SERVIZI_URL=https://sister.agenziaentrate.gov.it/Servizi/
SISTER_RICERCA_PERSONA_FISICA_URL=https://sister.agenziaentrate.gov.it/Visure/DataRichiesta.do
SISTER_VISURE_CATASTALI_URL=https://sister.agenziaentrate.gov.it/Visure/Informativa.do?tipo=/T/TM/VCVC_
SISTER_LOGOUT_URL=https://sister.agenziaentrate.gov.it/Servizi/CloseSessionsSis
################END AE AND SISTER URLS AND KEYS###############

```

---

## 🏗 Архітектура та Основний Флоу

Проєкт побудований на базі **Scrapy** з використанням **Playwright** для випадків, коли потрібна повноцінна взаємодія з браузером (login, JS, captcha).

### Основні етапи роботи (`scrapy crawl uppi`):

1.  **Ініціалізація (`start`)**:
    - Павук (`UppiSpider`) читає список клієнтів з файлу `clients/clients.yml`.
    - Для кожного клієнта перевіряється:
        - Чи є вже завантажена візура в БД?
        - Чи існує сам файл візури в S3?
    - Якщо даних немає або вони старі — клієнт додається в чергу на завантаження (`self.clients_to_fetch`).

2.  **Логін (Playwright)**:
    - Якщо є клієнти для обробки, запускається браузер.
    - Виконується вхід в особистий кабінет Agenzia Entrate (вкладка Fisconline).
    - Зберігається сесія.

3.  **Навігація та Парсинг (SISTER)**:
    - Перехід на портал SISTER "Visura per soggetto".
    - Заповнення форми пошуку (Codice Fiscale, Comune).
    - **Обробка Captcha**:
        - Якщо є картинка капчі — робиться скріншот.
        - Відправляється на 2Captcha.
        - Отриманий код вводиться в форму.
    - Натискання кнопки "Inoltra/Download".

4.  **Збереження**:
    - PDF-файл завантажується і зберігається в **S3** (`visure/<CF>.pdf`).
    - Метадані про візуру записуються в **PostgreSQL** (`visure` table).
    - Парсер (`VisuraProcessor`) розбирає PDF і зберігає дані про нерухомість (`immobili`) в БД.

5.  **Генерація (Post-processing)**:
    - На основі даних з БД та налаштувань контракту генерується документ **Word (docx)** — Attestazione.

### Взаємодія модулів

*   `uppi/spiders/uppi_spider.py` — головний оркестратор.
*   `uppi/ae/auth.py` — логіка входу в систему.
*   `uppi/ae/captcha.py` — взаємодія з 2Captcha.
*   `uppi/services/visura_processor.py` — парсинг PDF (використовує Camelot/PDFPlumber).
*   `uppi/services/db_repo.py` — усі запити до бази даних.
*   `uppi/services/storage_minio.py` — робота з S3.

---

## 📂 Структура проєкту

*   `uppi/` — основний код пакету.
    *   `ae/` — модулі для взаємодії з Agenzia Entrate (Playwright scripts).
    *   `cli/` — інструменти командного рядка (наприклад, перегляд стану клієнтів).
    *   `config/` — класи конфігурації.
    *   `domain/` — бізнес-сутність та моделі даних.
    *   `parsers/` — логіка розбору PDF-файлів.
    *   `services/` — бізнес-логіка (DB, S3, генерація довідок).
    *   `spiders/` — Scrapy павуки (`uppi`).
    *   `utils/` — допоміжні утиліти (DB init, логи).
*   `clients/` — вхідні дані (файл `clients.yml`).
*   `requirements.txt` — залежності.

---

## 🗄 База Даних

База даних зберігає інформацію про людей (власників), адреси, об'єкти нерухомості (з прив'язкою до кадастрових даних) та договори.

### Ініціалізація БД

Перед першим запуском потрібно створити таблиці. Для цього запустіть скрипт:

```bash
python uppi/utils/db_utils/init_db.py
```

Цей скрипт виконає SQL-команди з файлу `uppi/utils/db_utils/uppi_schema.sql`.

**Основні таблиці:**
*   `persons` — фізичні особи (орендодавці).
*   `visure` — метадані завантажених виписок.
*   `immobili` — об'єкти нерухомості (квартири, будинки), розпаршені з візури.
*   `contracts` — параметри договорів оренди.
*   `attestazioni` — згенеровані документи (лог).

---

## 🚀 Як працює зберігання

*   **Файли (PDF, Docx)**: Зберігаються в S3-сховищі (локальний MinIO або хмара).
    *   Візури: `bucket/visure/<CF>.pdf`
    *   Атестації: `bucket/attestazioni/<CF>/<ContractID>.docx`
*   **Кеш візури**:
    *   Система перевіряє дату `fetched_at` у таблиці `visure`.

**Логи**:
*   Виводяться в консоль (stdout/stderr).
*   Scrapy може вести свої логи, налаштування в `settings.py`.

---

## ▶️ Приклад запуску (повний флоу)

1.  **Підготуйте вхідні дані**:
    Відредагуйте `clients/clients.yml`, додавши клієнта:
    ```yaml
    - LOCATORE_CF: "RSSMRA80A01H501U"
      COMUNE: "ROMA"
      # та інші параметри...
    ```

2.  **Запустіть MinIO та PostgreSQL** (якщо ще не запущені).

3.  **Запустіть Scrapy-павука**:
    ```bash
    scrapy crawl uppi
    ```
    Павук автоматично:
    *   Залогіниться.
    *   Перевірить базу.
    *   Скачає відсутні візури (з вирішенням капчі).
    *   Збереже все в БД та S3.

4.  **Перевірте результати**:
    Ви можете використати CLI-утиліт для перегляду зібраних даних по клієнту:
    ```bash
    python uppi/cli/inspect_clients.py --cf RSSMRA80A01H501U
    ```
    Вона виведе в консоль структуру: Person -> Visura -> Immobili -> Contracts.
