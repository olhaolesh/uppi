# UPPI: AWS provisioning checklist для MVP

## 1. Призначення документа

Цей документ описує повний практичний список того, що потрібно створити і налаштувати в AWS, щоб запустити UPPI як веб-сервіс у першій версії MVP.

Документ орієнтований на послідовне створення ресурсів і не змішує це з runtime refactor або frontend/backend implementation tasks.

---

## 2. Цільова картина MVP

У результаті має бути розгорнуто:

* frontend як статичний сайт;
* backend API на FastAPI;
* runtime для UPPI/Playwright/generation/import;
* PostgreSQL база даних;
* S3 для артефактів;
* Parameter Store для конфігів і MVP credentials;
* CloudWatch Logs для логів;
* домен і HTTPS;
* test environment, через який можна пройти повний операторський сценарій.

---

## 3. Які AWS сервіси потрібні

### Обов'язкові для MVP

* IAM
* VPC
* EC2 Security Groups
* ECR
* ECS
* Fargate
* Application Load Balancer
* RDS PostgreSQL
* S3
* CloudFront
* ACM
* Route 53
* Systems Manager Parameter Store
* CloudWatch Logs

### Опційні не на першому кроці

* Secrets Manager
* WAF
* SQS
* CloudWatch Alarms
* AWS Backup

---

## 4. Послідовність створення ресурсів

# Крок 1. Підготувати AWS account і базову безпеку

1. Увійти в AWS account.
2. Вибрати основний region для MVP.
3. Увімкнути MFA для root account.
4. Не використовувати root account для щоденної роботи.
5. Створити IAM admin user або роль для роботи з інфраструктурою.
6. Налаштувати billing alerts.
7. Зафіксувати convention для іменування ресурсів, наприклад:

   * `uppi-dev-*`
   * `uppi-test-*`
   * `uppi-prod-*`

### Результат

Є безпечний AWS account і окремий робочий admin доступ.

---

# Крок 2. Вибрати environment strategy

1. Для MVP визначити мінімум 1 середовище: `test`.
2. Якщо є ресурс — одразу закласти 2 середовища:

   * `test`
   * `prod`
3. На першому етапі не обов'язково мати окремий `dev` в AWS, якщо локальна розробка достатня.

### Результат

Зафіксовано, які environments будуть реально існувати в AWS.

---

# Крок 3. Створити VPC і мережеву схему

1. Створити або використати окрему VPC для UPPI.
2. Створити щонайменше:

   * 2 public subnets;
   * 2 private subnets.
3. Рознести subnet-и хоча б на 2 Availability Zones.
4. В public subnets розміщувати:

   * ALB.
5. В private subnets розміщувати:

   * ECS tasks;
   * RDS PostgreSQL.
6. Налаштувати Internet Gateway.
7. Налаштувати route tables.
8. Якщо ECS tasks у private subnets мають виходити в інтернет для runtime needs — передбачити NAT strategy.

### Результат

Є мережевий каркас для ALB, ECS і RDS.

---

# Крок 4. Створити security groups

Потрібно окремо створити security groups:

### 4.1. Для ALB

* inbound: `80` і `443` з інтернету;
* outbound: до ECS tasks.

### 4.2. Для ECS tasks

* inbound: тільки від ALB на backend port;
* outbound: до RDS, S3/API endpoints, Parameter Store, CloudWatch та інших потрібних сервісів.

### 4.3. Для RDS

* inbound: тільки від ECS task security group на PostgreSQL port;
* без відкриття БД у публічний інтернет.

### Результат

Доступ між компонентами обмежений за ролями.

---

# Крок 5. Створити S3 buckets

Потрібно створити мінімум 2 buckets або 1 bucket з чіткими prefixes:

### Варіант А — окремі buckets

* `uppi-test-frontend`
* `uppi-test-artifacts`

### Варіант Б — один bucket із prefixes

* `frontend/`
* `visure/`
* `attestazioni/`
* `failed-imports/`

### Що налаштувати

1. Block Public Access залишити увімкненим для artifact bucket.
2. Для frontend hosting не робити bucket публічним напряму, якщо використовується CloudFront.
3. Увімкнути versioning хоча б для artifacts bucket.
4. За потреби додати lifecycle rules.

### Результат

Є сховище для frontend і артефактів системи.

---

# Крок 6. Створити Parameter Store parameters

Створити параметри в AWS Systems Manager Parameter Store.

## 6.1. SecureString parameters для MVP credentials

Наприклад:

* `/uppi/test/auth/username`
* `/uppi/test/auth/password`
* `/uppi/test/auth/pin`

## 6.2. String / SecureString для іншого config

Наприклад:

* `/uppi/test/app/env`
* `/uppi/test/db/host`
* `/uppi/test/db/name`
* `/uppi/test/db/user`
* `/uppi/test/db/password`
* `/uppi/test/s3/bucket_artifacts`
* `/uppi/test/aws/region`

## 6.3. Організаційні правила

1. Використовувати ієрархічний path naming.
2. Розділити параметри по environment.
3. Не зберігати ці значення в коді або frontend build.

### Результат

Усі MVP конфіги і credentials винесені в Parameter Store.

---

# Крок 7. Створити RDS PostgreSQL

1. Створити PostgreSQL DB instance.
2. Обрати Single-AZ для MVP.
3. Розмістити DB instance в private subnets.
4. Створити DB subnet group.
5. Прив'язати правильний security group.
6. Зберегти DB credentials у Parameter Store.
7. Увімкнути automated backups на мінімально прийнятному рівні.
8. Зафіксувати параметри підключення для backend.

### Додатково

* створити окрему DB для `test`;
* підготувати SQLAlchemy/Alembic connection string format;
* перевірити доступність БД із ECS runtime.

### Результат

Є робоча PostgreSQL база для веб-сервісу.

---

# Крок 8. Створити ECR repository

1. Створити ECR repository для backend image.
2. Визначити naming convention, наприклад:

   * `uppi-backend-test`
3. Налаштувати image push permissions.
4. Підготувати CI або локальну процедуру push image в ECR.

### Результат

Є registry для контейнерного образу backend/runtime.

---

# Крок 9. Підготувати IAM roles для ECS

Потрібно мінімум 2 ролі:

## 9.1. ECS task execution role

Права для:

* читання image з ECR;
* запису логів у CloudWatch;
* базового старту task.

## 9.2. ECS task role

Права для самого застосунку:

* читання Parameter Store;
* доступу до S3 buckets;
* доступу до інших потрібних AWS API.

### Результат

ECS tasks мають окремі ролі для запуску і для runtime access.

---

# Крок 10. Створити CloudWatch log groups

Створити log groups для:

* backend API logs;
* runtime/import/generation logs;
* за потреби окремих job categories.

Рекомендовано:

* задати retention policy;
* відразу домовитися про log naming convention.

### Результат

Логи контейнерів збираються централізовано.

---

# Крок 11. Створити ECS cluster

1. Створити ECS cluster.
2. Обрати Fargate launch type.
3. Підготувати task definition для backend container.
4. В task definition описати:

   * image з ECR;
   * CPU / memory;
   * port mapping;
   * env variables;
   * Parameter Store secrets injection;
   * log configuration;
   * ephemeral storage, якщо потрібно більше дефолтного значення.

### Результат

Є готовий ECS cluster і task definition для backend/runtime.

---

# Крок 12. Створити Application Load Balancer

1. Створити ALB у public subnets.
2. Прив'язати ALB security group.
3. Створити target group для ECS service.
4. Налаштувати health check endpoint, наприклад `/health/live` або `/health/ready`.
5. Пізніше додати HTTPS listener з ACM certificate.

### Результат

Є публічний entrypoint для backend API.

---

# Крок 13. Створити ECS service

1. Створити ECS service на основі task definition.
2. Прив'язати його до target group ALB.
3. Розмістити service у private subnets.
4. Прив'язати ECS task security group.
5. Встановити desired count для MVP.
6. Перевірити, що health checks проходять.

### Результат

Backend API працює в ECS/Fargate і доступний через ALB.

---

# Крок 14. Підготувати frontend build hosting

Є 2 робочі варіанти, але для цього MVP рекомендований один.

## Рекомендований варіант

### S3 + CloudFront

1. Створити frontend bucket.
2. Завантажити frontend build у bucket.
3. Створити CloudFront distribution поверх bucket.
4. Налаштувати default root object.
5. Налаштувати SPA fallback behavior, якщо frontend SPA.
6. Обмежити прямий доступ до bucket і віддавати frontend через CloudFront.

### Результат

Frontend доступний як статичний сайт через CloudFront.

---

# Крок 15. Створити ACM certificate

1. Якщо буде custom domain — створити certificate в ACM.
2. Для CloudFront сертифікат має бути в region, який використовується CloudFront для cert handling.
3. Підтвердити домен через DNS.

### Результат

Є TLS certificate для frontend і/або backend domain.

---

# Крок 16. Налаштувати Route 53

1. Якщо домен уже є в Route 53 — використати існуючу hosted zone.
2. Якщо домену ще немає — створити hosted zone або зареєструвати домен.
3. Створити DNS записи:

   * frontend domain → CloudFront;
   * API domain → ALB.
4. Використати alias records там, де це підтримується.

### Результат

Є нормальні доменні імена для frontend і backend.

---

# Крок 17. Підключити backend до Parameter Store і S3

1. Переконатися, що ECS task role має доступ до потрібних SSM paths.
2. Переконатися, що ECS task role має доступ до потрібних S3 buckets/prefixes.
3. Перевірити читання:

   * auth username/password/pin;
   * DB credentials;
   * artifact bucket config.
4. Перевірити запис у S3:

   * visure;
   * attestazioni;
   * failed CSV.

### Результат

Backend читає конфіг і секрети, а також працює з артефактами.

---

# Крок 18. Налаштувати CI/CD мінімального рівня

Мінімум потрібно мати окремі процеси для:

## Backend

* build Docker image;
* push у ECR;
* оновлення ECS service.

## Frontend

* build frontend;
* upload у S3;
* invalidation CloudFront cache.

### Результат

Є repeatable deployment process без ручного хаосу.

---

# Крок 19. Smoke check у test environment

Після підняття всіх ресурсів пройти базову перевірку:

1. Відкривається frontend URL.
2. Працює HTTPS.
3. Працює вхід у сервіс.
4. Frontend може звертатися до backend API.
5. Backend читає credentials із Parameter Store.
6. Працює підключення до RDS.
7. Працює запис у S3.
8. Працює пошук клієнта.
9. Працює generation flow.
10. Працює bulk import flow.
11. Працює скачування failed CSV.
12. Логи видно в CloudWatch.

### Результат

Є працездатний test MVP у AWS.

---

# Крок 20. Мінімальний post-deploy hardening

Після першого успішного запуску зробити мінімум:

1. Налаштувати retention для CloudWatch Logs.
2. Перевірити, що secrets не потрапляють у логи.
3. Перевірити S3 bucket policies.
4. Увімкнути versioning там, де потрібно.
5. Перевірити backup settings для RDS.
6. Перевірити security groups ще раз.
7. Зафіксувати runbook для:

   * redeploy;
   * rollback;
   * smoke check;
   * ротації credentials вручну через Parameter Store.

### Результат

MVP не просто запущений, а приведений до мінімально керованого стану.

---

## 5. Що можна відкласти

Для першого етапу можна не робити одразу:

* Secrets Manager;
* WAF;
* Auto Scaling policies складного рівня;
* Multi-AZ RDS;
* окремий worker service;
* SQS;
* повний IaC для всього, якщо спочатку швидше підняти test manually;
* production HA hardening.

---

## 6. Мінімальний практичний порядок “без зайвого”

Якщо робити найкоротшим шляхом, порядок такий:

1. AWS account + IAM + MFA
2. Region selection
3. VPC + subnets + security groups
4. S3 buckets
5. Parameter Store parameters
6. RDS PostgreSQL
7. ECR repository
8. IAM roles для ECS
9. CloudWatch log groups
10. ECS cluster + task definition
11. ALB + target group
12. ECS service
13. Frontend bucket + CloudFront
14. ACM certificate
15. Route 53 records
16. CI/CD мінімального рівня
17. Smoke check
18. Post-deploy hardening

---

## 7. Підсумок

Щоб запустити UPPI як веб-сервіс у AWS, не потрібно одразу будувати складну enterprise-схему. Для MVP достатньо правильно послідовно створити:

* мережу;
* БД;
* сховище артефактів;
* конфіг і credentials у Parameter Store;
* ECS/Fargate runtime;
* ALB;
* frontend hosting через S3/CloudFront;
* DNS і HTTPS.

Після цього вже можна переходити до окремих technical prompts для Codex по кожному етапу AWS provisioning або по кожному deployment slice окремо.
