# Uppi — автоматизація завантаження Visure та генерації Attestazione

## Опис проєкту
Uppi — це Python-додаток, який автоматизує вхід до особистого кабінету **Agenzia delle Entrate**, отримання кадастрових виписок (**visura**) з порталу **SISTER**, їх обробку та генерацію документів **Attestazione** для договорів оренди.

Проєкт використовує **Scrapy** разом із **Playwright** для керування браузером, автоматичного логіну та розв’язання CAPTCHA через сервіс **2Captcha**.

---

## Локальне розгортання

### Вимоги
- Python 3.10+
- PostgreSQL
- MinIO або AWS S3 / Cloudflare R2
- Playwright
- Обліковий запис 2Captcha

### Встановлення залежностей
```bash
pip install -r requirements.txt
playwright install
```

---

## Налаштування `.env`
Створіть файл `.env` у корені проєкту:

```env
################AE AND SISTER CONFIGURATION###################
AE_USERNAME=your_login
AE_PASSWORD=your_password
AE_PIN=your_pin
################END AE AND SISTER CONFIGURATION###############

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

TWO_CAPTCHA_API_KEY=your_2captcha_key

#################POSTGRES CONFIGURATION####################
DB_HOST=localhost
DB_PORT=5432
DB_NAME=uppi_db
DB_USER=uppi_user
DB_PASSWORD=uppi_password
DB_SSL_MODE=prefer
#################END POSTGRES CONFIGURATION###################

#################S3 CONFIGURATION#############################
S3_ENDPOINT=localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_SECURE=False
VISURE_BUCKET=uppi-bucket
ATTESTAZIONI_BUCKET=attestazioni
#################END S3 CONFIGURATION##########################
UPPI_CLIENTS_YAML=clients/clients.yml

# Delete local visura file after upload to MinIO/Aiven
DELETE_LOCAL_VISURA_AFTER_UPLOAD=True
# Prune old immobili without contracts from DB
PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS=True
```

---

## Ініціалізація бази даних
```bash
python uppi/utils/db_utils/init_db.py
```

Створюються таблиці:
- addresses
- attestazioni
- canone_calcoli
- contracts
- immobile_elements
- immobili
- persons
- visure

---

## Основний флоу роботи

1. Зчитування клієнтів з `clients/clients.yml`
2. Перевірка наявності актуальної visura
3. Логін в Agenzia Entrate через Playwright
4. Перехід на SISTER та пошук за Codice Fiscale
5. Розв’язання CAPTCHA через 2Captcha
6. Завантаження PDF visura
7. Збереження файлів у S3 та метаданих у PostgreSQL
8. Парсинг PDF та збереження об’єктів нерухомості
9. Генерація Attestazione у форматі DOCX

---

## Запуск

```bash
scrapy crawl uppi
```

---

## Структура проєкту

```
uppi/
├── ae/          # авторизація, CAPTCHA
├── spiders/     # Scrapy spiders
├── services/    # БД, S3, генерація документів
├── parsers/     # PDF-парсинг
├── domain/      # бізнес-моделі
├── cli/         # CLI-утиліти
├── utils/       # ініціалізація, логування
clients/
requirements.txt
```

---

## Зберігання файлів

- Visure: `visure/<CF>.pdf`
- Attestazioni: `attestazioni/<CF>/<ContractID>.docx`


## Логування
Логи виводяться в stdout. Для Scrapy використовуйте налаштування в `settings.py`.

---

## Безпека
- Не комітьте `.env`
- Міняйте ключі перед продакшеном
- Обмежуйте доступ до S3

---

## Ліцензія
MIT
