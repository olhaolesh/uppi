# UPPI

UPPI працює у **трьох окремих режимах**. Важливо розуміти їхню роль:

1. **Підготовка одного клієнта (`prepare-by-CF`)**
   Цей режим перевіряє, чи вже є достатньо даних у базі для конкретного клієнта.
   Якщо даних не вистачає, система спочатку оновлює їх через імпорт з SISTER, а потім створює файл `immobili.yml` для цього клієнта.

2. **Генерація документів (`scrapy crawl uppi`)**
   Цей режим читає вже підготовлений в кроці `prepare-by-CF` файл `immobili.yml` і запускає генерацію документа Atestazione.
   Він **не** ходить у SISTER і **не** підтягує дані самостійно.
   Якщо потрібного об’єкта немає в базі, команда зупиниться з підказкою спочатку запустити підготовку описану в кроці `prepare-by-CF`.

3. **Масове оновлення (наповнення) бази з CSV (`bulk-import-by-clients-csv`)**
   Цей режим проходить по списку клієнтів із переданого CSV-файлу імпортує дані з SISTER та оновлює/додає в базу.
   Він **не** створює `immobili.yml` і **не** запускає генерацію документів Atestazione.



Основні правила роботи системи зафіксовані в документі
[docs/immobili_rollout_source_of_truth.md](docs/immobili_rollout_source_of_truth.md)

Загальна карта документації зібрана тут:
[docs/README.md](docs/README.md)

## Що змінилося

* `immobili.yml` тепер є **основним файлом для генерації**
* один файл `immobili.yml` описує **одного клієнта**
* підготовка даних клієнта винесена в окремий режим `prepare-by-CF`
* масове оновлення/наповнення клієнтів в базі даних через `clients.csv` працює окремо від генерації
* команда `scrapy crawl uppi` тепер відповідає **лише за генерацію** Atestazione
* якщо потрібного об’єкта немає в базі, генерація не намагається автоматично сходити в SISTER, а просить спочатку виконати підготовку режим `prepare-by-CF`
* правила перевірки YAML і поведінка `"-"` для очищення полів тепер описані окремо й працюють однаково по всій системі

## Основні команди

### 1. Підготувати дані для одного клієнта

```bash
python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z
```

Що робить ця команда:

* перевіряє, чи є в базі потрібні дані по клієнту
* якщо даних не вистачає, виконує імпорт з SISTER
* після цього створює файл `immobili.yml` з даними по всіх об'єктах нерухомості клієнта

### 2. Примусово оновити visura і заново згенерувати YAML

```bash
python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z --force-update-visura
```

Це потрібно тоді, коли дані в базі вже є, але ти хочеш примусово оновити їх (наприклад якщо VISURA клієнта змінилася) перед створенням нового `immobili.yml`.

### 3. Масово оновити/наповнити базу клієнтами вказаними в CSV

```bash
python -m uppi.cli.bulk_import_clients_csv --csv clients/clients.csv
```

Що робить ця команда:

* читає список клієнтів із CSV
* для кожного клієнта запускає лише оновлення/наповнення даних у базі інформацією з SISTER
* **не** створює `immobili.yml`
* **не** запускає генерацію документів Attestazione

### 4. Запустити генерацію з уже підготовленого YAML

```bash
scrapy crawl uppi
```

Що робить ця команда:

* читає підготовлений файл `immobili.yml` з даними про нерухомість одного клієнта
* бере тільки активні записи там де enable: True
* запускає розрахунок і генерацію документів Attestazione для кожного активного об'єкту нерухомості

Що вона **не** робить:

* не оновлює дані з SISTER
* не виконує імпорт
* не приймає рішення, чи достатньо даних у базі

## Як правильно працювати з системою

### Варіант 1. Потрібно згенерувати документи для одного клієнта

1. Запусти підготовку:

   ```bash
   python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z
   ```

2. Відкрий згенерований файл `immobili.yml`.

3. Перевір дані та, за потреби, відредагуй їх.

4. Якщо якийсь об’єкт не треба обробляти, постав:

   ```yaml
   enabled: false
   ```

5. Якщо треба очистити значення, використовуй `"-"` **тільки для тих полів, де це дозволено**.

6. Запусти генерацію:

   ```bash
   scrapy crawl uppi
   ```

### Варіант 2. Потрібно спочатку масово оновити/наповнити базу з SISTER

1. Запусти масове оновлення:

   ```bash
   python -m uppi.cli.bulk_import_clients_csv --csv clients/clients.csv
   ```

2. Для кожного клієнта, для якого потрібна генерація, окремо виконай:

   ```bash
   python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z
   ```

3. Перевір і відредагуй `immobili.yml`.

4. Запусти:

   ```bash
   scrapy crawl uppi
   ```

Детальний покроковий опис для оператора:
[docs/operator_workflow.md](docs/operator_workflow.md)

## Які документи читати

### Основні документи

* [docs/README.md](docs/README.md)
* [docs/immobili_rollout_source_of_truth.md](docs/immobili_rollout_source_of_truth.md)
* [docs/operator_workflow.md](docs/operator_workflow.md)
* [docs/validation_clear_policy_matrix.md](docs/validation_clear_policy_matrix.md)

### Як улаштована система

* [docs/runtime_flow.md](docs/runtime_flow.md)
* [docs/current_architecture.md](docs/current_architecture.md)
* [docs/web_migration_baseline.md](docs/web_migration_baseline.md)
* [docs/architecture_decisions/web_service_foundation.md](docs/architecture_decisions/web_service_foundation.md)
* [docs/web_backend_shell.md](docs/web_backend_shell.md)
* [docs/local_development_and_testing.md](docs/local_development_and_testing.md)

### Тести, перевірки, запуск перед релізом

* [docs/regression_test_map.md](docs/regression_test_map.md)
* [docs/rollout_ready_checklist.md](docs/rollout_ready_checklist.md)
* [docs/live_smoke_strategy_ae_sister.md](docs/live_smoke_strategy_ae_sister.md)

### Довідкові технічні документи

* [docs/refactor_protected_invariants.md](docs/refactor_protected_invariants.md)
* [docs/state_json_lifecycle_contract.md](docs/state_json_lifecycle_contract.md)
* [docs/document_generation.md](docs/document_generation.md)
* [docs/failure_registry_contract.md](docs/failure_registry_contract.md)

## Налаштування через змінні середовища

### Основний YAML для генерації

* `UPPI_IMMOBILI_YAML`

Ця змінна вказує, який саме файл `immobili.yml` брати для генерації.

### CSV для масового оновлення

* `UPPI_CLIENTS_CSV`

Ця змінна вказує шлях до CSV-файлу для масового імпорту.

### Стара сумісність

* `UPPI_CLIENTS_YAML`

Це старий механізм сумісності.
Він більше **не є основним способом** запуску генерації і залишений лише для внутрішніх або перехідних частин захищеного шляху імпорту.

## Тестування

Карта тестів і рекомендації щодо запуску:
[docs/regression_test_map.md](docs/regression_test_map.md)

Повний запуск тестів:

```bash
python -m pytest -q
```

Перевірки перед використанням у роботі:
[docs/rollout_ready_checklist.md](docs/rollout_ready_checklist.md)

## Жива перевірка AE/SISTER

Живу перевірку AE/SISTER потрібно робити лише тоді, коли зміни зачіпають:

* захищений браузерний шлях імпорту
* повторне використання browser/import логіки
* життєвий цикл `state.json`

Актуальний чекліст для такої перевірки:
