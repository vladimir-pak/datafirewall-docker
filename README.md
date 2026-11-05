# DataFirewall — локальный отказоустойчивый кластер в Docker Compose

**Назначение:** развёртывание и испытания DataFirewall на одном Docker-хосте: инфраструктура, четыре Java-микросервиса, Apache Flink 1.20.3 в конфигурации High Availability, транзакционная обработка сообщений Artemis/IBM MQ, Kafka-аудит и хранение состояния в S3-совместимом хранилище MinIO.

> **Область применения:** локальное, интеграционное и нагрузочное тестирование. Это **не** отказоустойчивость на уровне физических серверов: все контейнеры выполняются на одном Docker-хосте. PostgreSQL, Kafka, Artemis, Ignite и MinIO в данной сборке имеют по одному экземпляру и остаются точками отказа.

## Содержание

1. [Архитектура](#1-архитектура)
2. [Сервисы и порты](#2-сервисы-и-порты)
3. [Структура проекта и подготовка](#3-структура-проекта-и-подготовка)
4. [Первый запуск](#4-первый-запуск)
5. [Конфигурация Flink и ресурсы](#5-конфигурация-flink-и-ресурсы)
6. [Запуск и управление Flink Job](#6-запуск-и-управление-flink-job)
7. [Настройки DataFirewall Job](#7-настройки-datafirewall-job)
8. [HA, checkpoint и savepoint](#8-ha-checkpoint-и-savepoint)
9. [Балансировка, Adaptive Scheduler и возвращение TaskManager](#9-балансировка-adaptive-scheduler-и-возвращение-taskmanager)
10. [Сценарии тестирования отказоустойчивости](#10-сценарии-тестирования-отказоустойчивости)
11. [Логирование и диагностика](#11-логирование-и-диагностика)
12. [Пересборка образов и обновление конфигурации](#12-пересборка-образов-и-обновление-конфигурации)
13. [Типовые проблемы](#13-типовые-проблемы)
14. [Ограничения и рекомендации](#14-ограничения-и-рекомендации)

## 1. Архитектура

В `compose.yaml` описаны **19 сервисов**:

- 2 JobManager: `flink-jm1`, `flink-jm2`, `flink-jm3`;
- 2 TaskManager: `flink-tm1`, `flink-tm2`, `flink-tm3`;
- 3 узла ZooKeeper: `zk1`, `zk2`, `zk3`;
- PostgreSQL, Kafka и `kafka-init`, Artemis, Ignite, MinIO и `minio-init`;
- 3 отдельных Java-сервиса: `spring-api`, `datafirewall-kafka`, `jms-publisher` (четвёртый Java-сервис — `ignite`).

Схема взаимодействия (упрощённо):

```text
                     ┌────────────────────────────────────┐
                     │   Docker bridge: datafirewall      │
                     └────────────────────────────────────┘

  jms-publisher ──> Artemis:61616 ──> DataFirewall Flink Job
                                              │
                              ┌───────────────┼──────────────────┐
                              ▼               ▼                  ▼
                      spring-api:9200      Kafka:9092      Artemis:61616
                              │           (3 топика)       (ответы)
                              ▼               │
                        Ignite:10800           ▼
                                       datafirewall-kafka
                                               │
                                               ▼
                                          PostgreSQL

       Flink HA:
       flink-jm1  ──────┐
       flink-jm2  ──────┼── ZooKeeper zk1 + zk2 + zk3
       flink-jm3  ──────┤
       flink-tm1  ──────┤
       flink-tm2  ──────┤
       flink-tm3  ──────┘
               │
               └── MinIO/S3: HA metadata + checkpoints + savepoints
```

В каждый момент времени один JobManager выступает лидером (активным координатором), второй может принять управление после отказа лидера. TaskManager — исполнители задач; это не самостоятельные Job. Три JobManager и три TaskManager используют одну сеть Docker и общий bucket `flink` в MinIO.

При запуске `docker compose` сначала должен инициализироваться PostgreSQL, затем инфраструктура и сервисы. Для зависимостей используются `depends_on` и healthcheck, но **`depends_on` не обеспечивает восстановление потерянных соединений после запуска** — переподключение должно быть реализовано на уровне клиентов.

## 2. Сервисы и порты

| Сервис | Адрес **из другого контейнера** | Адрес с Docker-хоста | Примечание |
|---|---|---|---|
| PostgreSQL | `postgres:5432` | `localhost:5432` | БД `datafirewall` по умолчанию |
| Spring REST API | `spring-api:9200` | `localhost:9900` | Внутренний порт **9200**, не 9900 |
| Ignite REST | `ignite:9900` | `localhost:9901` | Отдельно от Spring API |
| Ignite Thin Client | `ignite:10800` | `localhost:10800` | Используется клиентами Ignite |
| Kafka | `kafka:9092` | `localhost:9094` | Внутренний SASL-порт и внешний listener |
| Artemis JMS | `artemis:61616` | `localhost:61616` | Очереди запросов/ответов |
| Artemis Console | `artemis:8161` | `localhost:8161` | Веб-консоль брокера |
| MinIO S3 API | `minio:9000` | `localhost:8333` | Bucket `flink` |
| MinIO Console | `minio:9001` | `localhost:9001` | Веб-консоль |
| Flink JobManager 1 | `flink-jm1:8081` | `localhost:8081` | REST/UI первого JM |
| Flink JobManager 2 | `flink-jm2:8081` | `localhost:8082` | REST/UI второго JM |
| datafirewall-kafka | `datafirewall-kafka:9800` | `localhost:9801` | Kafka consumer / аудит |
| jms-publisher | `jms-publisher:9800` | `localhost:9800` | Отправка сообщений в Artemis |
| ZooKeeper | `zk1:2181`, `zk2:2181`, `zk3:2181` | Не опубликованы | Кворум 2 из 3 |
| TaskManager | `flink-tm1`, `flink-tm2` | Нет опубликованного порта | Управляются через Flink REST/UI |

Внутренние имена Docker работают только в сети Compose. **Нельзя** использовать внешний проброшенный порт для обращения между контейнерами. Например, из Flink правильный URL Spring API — `http://spring-api:9200`, а не `http://spring-api:9900`.

Часть портов снаружи привязана только к `127.0.0.1`, но порты Java-микросервисов в текущем Compose могут слушать все интерфейсы хоста. Это допустимо только для контролируемой отладочной среды.

## 3. Структура проекта и подготовка

```text
datafirewall-docker/
├── compose.yaml
├── .env.example
├── .env                       # создать локально;
├── README.md
├── docker/
│   ├── flink/Dockerfile
│   ├── ignite/Dockerfile
│   ├── spring/Dockerfile
│   ├── kafka/kafka-start.sh
│   ├── kafka/client.properties
│   ├── kafka-reader/Dockerfile
│   └── jms-publisher/Dockerfile
├── dist/
│   ├── datafirewall-spring.jar
│   ├── ignite-server.jar
│   ├── datafirewall-kafka.jar
│   └── jms-publisher.jar
├── jobs/
│   ├── datafirewall-flink.jar
│   └── job.properties
├── postgres/init/
│   ├── 01-ddl.sql
│   └── 02-dml.sql
├── config/
│   └── logback-spring.xml
└── reference/
    ├── application_spring.yaml
    ├── application_ignite.yaml
    └── application_kafka.yaml
```

**Перед первым стартом:**

1. Установить Docker Engine / Docker Desktop и Docker Compose v2.
2. Положить **четыре исполняемых Java JAR** в `dist/`; архив с Compose не содержит собранные бинарники.
3. Положить Flink JAR в `jobs/datafirewall-flink.jar`; его entrypoint — `com.gpb.datafirewall.Main`.
4. Проверить `jobs/job.properties` (очереди, topic, адрес Spring REST, пароли и JWT).
5. **Заменить шаблоны** `postgres/init/01-ddl.sql` и `02-dml.sql` настоящими DDL/DML приложения. Шаблоны в дистрибутиве создают только базовые объекты и маркер готовности, не всю прикладную схему.
6. Создать `.env` из `.env.example` и проверить параметры и доступную оперативную память.

```bash
cp .env.example .env
mkdir -p dist jobs
ls -lh dist/ jobs/
docker compose config -q
```

**Особенность PostgreSQL:** SQL-скрипты `/docker-entrypoint-initdb.d/` выполняются только при **первой инициализации пустого volume**. Правка SQL после создания БД сама по себе не применится. Healthcheck проверяет `public.dfw_bootstrap` и маркер `stage='dml'`; этот маркер должен выставляться **после успешного DML**.

## 4. Первый запуск

При **первой инициализации** DataFirewall сервисы необходимо поднимать последовательно: сначала PostgreSQL, затем Ignite, после этого Spring REST API. До запуска Flink Job нужно создать JWT для сервиса `datafirewall-flink` и указать его в `jobs/job.properties`.

Все команды выполняются из каталога, содержащего `compose.yaml`.

### Шаг 1. Инициализация PostgreSQL

```bash
# Проверка итоговой конфигурации с подстановкой переменных
docker compose config -q

# По желанию: сначала поднять PostgreSQL и проверить инициализацию
docker compose up -d postgres
docker compose logs -f postgres
```

Дождитесь завершения выполнения `01-ddl.sql` и `02-dml.sql` и перехода PostgreSQL в состояние `healthy`. Для выхода из просмотра логов используйте `Ctrl+C` — контейнер продолжит работать.

### Шаг 2. Отдельный запуск Ignite

```bash
docker compose up -d --build ignite
docker compose logs -f ignite
```

Убедитесь, что Ignite успешно запустился, кластер активирован и REST/Thin Client доступны. Затем выйдите из просмотра логов с помощью `Ctrl+C`.

### Шаг 3. Отдельный запуск Spring REST API

```bash
docker compose up -d --build spring-api
docker compose logs -f spring-api
```

**Особенность зависимостей:** в текущем `compose.yaml` сервис `spring-api` зависит от `postgres`, `ignite` и `kafka-init`. Поэтому команда `docker compose up -d spring-api` **автоматически поднимет Kafka и выполнит `kafka-init`**, если они ещё не запущены. Это ожидаемое поведение; остальные компоненты, включая Flink, на данном этапе отдельно не запускаем.

Убедитесь, что Spring REST API запущен и принимает HTTP-запросы на `http://localhost:9900` (внутри сети Docker — `spring-api:9200`).

### Шаг 4. Создание JWT для Flink

До запуска остальных контейнеров создайте JWT через REST API `spring-api`:

```bash
curl --request POST 'http://localhost:9900/api/token/create' \
  --header 'Content-Type: application/json' \
  --data '{
    "secret": "ЗНАЧЕНИЕ_JWT_SECRET",
    "service": "datafirewall-flink"
  }'
```

Замените `ЗНАЧЕНИЕ_JWT_SECRET` на значение переменной `JWT_SECRET`, используемое **запущенным** сервисом `spring-api` (обычно задаётся в `.env` и подставляется через `compose.yaml`). Не путайте секрет подписи с самим JWT: в запросе передаётся **секрет**, а в ответе API возвращается **токен**.

Из успешного ответа скопируйте полученный JWT и укажите его в `jobs/job.properties`:

```properties
vault.enabled=false
secrets.jwt=ПОЛУЧЕННЫЙ_JWT
```

Замените `ПОЛУЧЕННЫЙ_JWT` на полную строку токена без кавычек. Не публикуйте секрет и токен в Git, логах или скриншотах. Если `vault.enabled=true`, способ получения JWT определяется настройками Vault и логикой приложения, а не только значением `secrets.jwt` в файле.

Проверка значения в конфигурации (без вывода самого токена):

```bash
grep -q '^secrets.jwt=.' ./jobs/job.properties \
  && echo 'JWT указан в job.properties'
```

### Шаг 5. Запуск остальных сервисов

После сохранения JWT запустите оставшиеся контейнеры:

```bash
# Сборка локальных образов и запуск остальных сервисов
docker compose up -d --build

# Состояние всех контейнеров
docker compose ps -a
```

Ранее запущенные `postgres`, `ignite`, `spring-api` и их зависимости Docker Compose переиспользует, если их конфигурация и образы не изменились. **Flink Job не отправляется автоматически** при запуске контейнеров: после готовности кластера отправьте его командой из [раздела 6](#6-запуск-и-управление-flink-job). На момент отправки Job актуальное значение `secrets.jwt` уже должно находиться в `jobs/job.properties`.

Проверка инфраструктуры:

```bash
# Маркер завершения DML
docker compose exec postgres psql -U postgres -d datafirewall \
  -c "SELECT * FROM public.dfw_bootstrap;"

# Список Kafka topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --command-config /opt/datafirewall/client.properties --list

# Логи инициализации S3 bucket и топиков
docker compose logs --tail=100 minio minio-init kafka kafka-init

# Flink UI с хоста
curl -i http://localhost:8081/overview
curl -i http://localhost:8082/overview
curl -i http://localhost:8083/overview
```

Текущая конфигурация создаёт:

| Объект | Наименование | Параметры |
|---|---|---|
| Artemis — вход | `datafirewall.in` | JMS/anycast очередь |
| Artemis — выход | `datafirewall.out` | JMS/anycast очередь |
| Kafka | `rules-update` | 1 partition (уведомления об изменении правил) |
| Kafka | `processing-audit` | 3 partitions (аудит обработки) |
| Kafka | `cef-audit` | 3 partitions (CEF-аудит) |
| MinIO | bucket `flink` | Префиксы `ha/metadata`, `ha/checkpoints`, `ha/savepoints` |

`kafka-init` и `minio-init` — одноразовые сервисы и после успешного выполнения могут находиться в состоянии `Exited (0)`: это не ошибка.

**Не выполняйте `docker compose down -v`, если нужно сохранить состояние.** Этот ключ удаляет именованные тома PostgreSQL, Kafka, Artemis, Ignite, ZooKeeper, MinIO и локального состояния Flink.

## 5. Конфигурация Flink и ресурсы

Кластерные свойства задаются в блоке `x-flink-properties` (`FLINK_PROPERTIES`) файла `compose.yaml`. Переменные `.env` подставляются Docker Compose перед созданием контейнеров.

Пример **текущей тестовой настройки** из последних изменений окружения:

```dotenv
FLINK_JM_MEMORY=1024m
FLINK_TM_MEMORY=2g
FLINK_TM_SLOTS=8
FLINK_PARALLELISM=8
```

- `FLINK_JM_MEMORY` — общий объём памяти процесса каждого JobManager.
- `FLINK_TM_MEMORY` — общий объём памяти процесса каждого TaskManager.
- `FLINK_TM_SLOTS` — **количество слотов на каждом TaskManager**, не во всём кластере: `3 × 8 = 24` объявленных слотов.
- `FLINK_PARALLELISM` — параллелизм операторов **по умолчанию**, не количество Job и не обязательное число используемых слотов.

При параллелизме 4 приложение может занимать примерно четыре слота за счёт **slot sharing**, но фактическая потребность определяется топологией и отдельными slot-sharing groups. Память `2g` выделяется **на весь TaskManager**, а не на каждый слот; восемь слотов при 2 GiB не обеспечивают восемь независимых процессов с 2 GiB памяти.

Разделение ответственности:

| Где задаётся | Примеры параметров | Когда применяется |
|---|---|---|
| `.env` → `compose.yaml` → `FLINK_PROPERTIES` | `parallelism.default`, `taskmanager.numberOfTaskSlots`, память JM/TM, `jobmanager.scheduler`, checkpoint/S3/HA | После пересоздания соответствующих Flink-контейнеров; действующий Job не всегда автоматически принимает изменения |
| `jobs/job.properties` | `messaging.backend`, `parallelism.process`, `ignite.apiUrl`, Artemis/Kafka, Vault, JWT | При **новой отправке Job** (если код специально не реализует горячую перезагрузку) |

## 6. Запуск и управление Flink Job

Каталог `./jobs` подключается как **bind mount**:

```yaml
volumes:
  - ./jobs:/opt/flink/usrlib:ro
```

Следовательно, файл `jobs/job.properties` в контейнерах меняется **сразу после изменения на хосте**. Но уже запущенный Flink Job продолжает использовать прочитанную ранее конфигурацию. **Пересборка образа для изменения JAR или `job.properties` не требуется**, а вот повторная отправка Job обычно нужна.

Проверьте файлы:

```bash
docker compose exec flink-jm1 ls -lh /opt/flink/usrlib
```

Отправка Job (делать **один раз** на весь HA-кластер):

```bash
docker compose exec flink-jm1 flink run -d \
  -c com.gpb.datafirewall.Main \
  /opt/flink/usrlib/datafirewall-flink.jar \
  --config.file /opt/flink/usrlib/job.properties
```

Проверка Job и TaskManager:

```bash
docker compose exec flink-jm1 flink list
curl -s http://localhost:8081/jobs/overview
curl -s http://localhost:8081/taskmanagers
```

Для интерактивного просмотра откройте `http://localhost:8081` или `http://localhost:8082` (активный JobManager может меняться). В `Running Jobs` должен отображаться **один JobID** для одного развернутого DataFirewall. Два JobID означают два отдельных запуска приложения, **а не** по одному Job на TaskManager.

### Плановое обновление Job с сохранением состояния

1. Убедиться, что успешен последний checkpoint и доступны все необходимые системы.
2. Получить актуальный `JOB_ID` через `flink list`.
3. Остановить Job с финальным savepoint (потребуется поддержка корректной остановки со стороны кастомных source/sink):

```bash
docker compose exec flink-jm1 flink stop \
  --savepointPath s3://flink/ha/savepoints \
  <JOB_ID>
```

4. **Записать полный путь**, выведенный Flink после успешного savepoint.
5. Обновить `jobs/datafirewall-flink.jar` и/или `jobs/job.properties`.
6. Повторно отправить Job с восстановлением:

```bash
docker compose exec flink-jm1 flink run -d \
  -s <ПОЛНЫЙ_SAVEPOINT_PATH> \
  -c com.gpb.datafirewall.Main \
  /opt/flink/usrlib/datafirewall-flink.jar \
  --config.file /opt/flink/usrlib/job.properties
```

`<JOB_ID>` и `<ПОЛНЫЙ_SAVEPOINT_PATH>` — заполнители; подставляйте реальные значения **без угловых скобок**. Сохранение state не равно гарантии отсутствия повторных действий во внешнем JMS/Kafka: это проверяется интеграционными тестами бизнес-транзакций.

Для простой остановки **без нового savepoint**:

```bash
docker compose exec flink-jm1 flink cancel <JOB_ID>
```

При обновлении кластерных параметров планировщика/JM/TM действующий Job может автоматически восстановиться из HA со старыми параметрами. Для контролируемого применения новых параметров используйте управляемую остановку и новый запуск.

## 7. Настройки DataFirewall Job

В последнем предоставленном `job.properties` заданы:

```properties
# Выбран Artemis, а не IBM MQ
messaging.backend=artemis
artemis.broker.url=tcp://artemis:61616
artemis.in.queue=datafirewall.in
artemis.out.queue=datafirewall.out

kafka.bootstrap=kafka:9092
kafka.topic=rules-update
kafka.group=dfw-rules-group
kafka.security.protocol=SASL_PLAINTEXT
kafka.sasl.enabled=true
kafka.sasl.mechanism=SCRAM-SHA-512

audit.kafka.enabled=true
audit.kafka.bootstrap=kafka:9092
audit.kafka.topic=processing-audit
cef.audit.enabled=true
cef.audit.kafka.enabled=true
cef.audit.kafka.topic=cef-audit

ignite.apiUrl=http://spring-api:9200

vault.enabled=false
```

Параллелизм в последнем файле **закомментирован**:

```properties
# parallelism=4
# parallelism.process=8
# parallelism.audit=4
# audit.slot-sharing-group=audit
```

Если код не переопределяет значения, общий parallelism берётся из Flink execution environment. Для фиксированных четырёх транзакционных обработчиков можно явно задать `parallelism=4` и `parallelism.process=4`, затем **переотправить** Job. Отдельная `audit.slot-sharing-group` требует дополнительных слотов.

Для выбранного Artemis-контроллера каждый worker использует отдельную JMS-сессию/подключение (согласно комментариям к переданному `job.properties`).

Аудит `processing-audit` в текущем файле описан как **best-effort, не часть JMS-транзакции**. CEF-аудит настраивается отдельно; для локального `cef.log` по умолчанию не гарантирована сохранность после пересоздания контейнера без дополнительного volume. Результаты аудита не следует использовать как единственный признак «ровно один раз» без сквозной сверки JMS и прикладных результатов.

### Vault и секреты

В тестовой конфигурации установлено `vault.enabled=false`, значения `secrets.*` берутся из `job.properties`. При `vault.enabled=true` должны быть доступны Vault, AppRole и KV/секреты в соответствии с поддерживаемым кодом приложения. Для production используйте secrets management, сетевую защиту и TLS; текущий `SASL_PLAINTEXT` шифрования трафика не даёт.

## 8. HA, checkpoint и savepoint

| Компонент | Роль в отказоустойчивости | Что происходит при отказе |
|---|---|---|
| `flink-jm1`, `flink-jm2`, `flink-jm3` | Выбор одного лидера и резервирование координатора | Другой JM может стать лидером при доступных ZooKeeper и S3 metadata |
| `flink-tm1`, `flink-tm2`, `flink-tm3` | Выполнение операторов | Flink пытается восстановить subtask на доступных слотах |
| ZooKeeper x3 | Кворум, координация leader election | При отказе одного узла кворум 2/3 остаётся; потеря большинства нарушает HA |
| MinIO/S3 | HA metadata, checkpoints, savepoints | Недоступность S3 может помешать checkpoint/восстановлению |
| RocksDB (локально на TM) | Локальное состояние потоковой обработки | Ускоряет обработку/локальное восстановление; не заменяет удалённые checkpoints |

Фактические пути из `compose.yaml`:

```yaml
high-availability.type: zookeeper
high-availability.storageDir: s3://flink/ha/metadata
high-availability.zookeeper.quorum: zk1:2181,zk2:2181,zk3:2181

state.backend.type: rocksdb
execution.checkpointing.storage: filesystem
execution.checkpointing.dir: s3://flink/ha/checkpoints
execution.checkpointing.savepoint-dir: s3://flink/ha/savepoints
execution.checkpointing.interval: 60s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
```

**Checkpoint** — периодическое восстановимое состояние Job; **savepoint** — управляемая точка сохранения для планового обновления/перезапуска. Для восстановления нужны совместимые operator IDs, сериализаторы, код и доступные файлы состояния. 

Перед тестами отказоустойчивости убедитесь, что на вкладке **Checkpoints** есть успешный `COMPLETED` и отсутствуют постоянные ошибки обращения к S3.

## 9. Балансировка, Adaptive Scheduler и возвращение TaskManager

### Разница между слотами, parallelism и количеством контейнеров

При двух TaskManager, 8 слотах на каждый и `parallelism.default=4` кластер объявляет **16 слотов**, но Job не обязан занимать все 16. Один TaskManager способен разместить все четыре параллельных worker, если позволяет память и число необходимых slot-sharing groups.

Пример сценария, подтверждённого тестом:

```text
Исходно:               TM1: 2 subtask       TM2: 2 subtask
Отказ TM1:             TM1: недоступен      TM2: 4 subtask
Возврат TM1:           TM1: 0 subtask       TM2: 4 subtask
```

Последняя строка — **нормальное поведение**: возвращение свободных слотов не заставляет Flink переместить уже работающие задачи.

### Распределение при новом планировании

Для более равномерного распределения вновь выделяемых слотов добавлен в `x-flink-properties`:

```yaml
taskmanager.load-balance.mode: SLOTS
```

Этот режим **не** выполняет постоянную перебалансировку ранее назначенных subtasks и не даёт строгой гарантии 2+2. Он влияет на выбор слотов при планировании/восстановлении.

### Опциональный Adaptive Scheduler

Чтобы экспериментировать с изменением параллелизма при изменении ресурсов, можно использовать следующие **кластерные** параметры:

```yaml
jobmanager.scheduler: Adaptive
jobmanager.adaptive-scheduler.prefer-minimal-taskmanagers: false
jobmanager.adaptive-scheduler.scaling-interval.min: 30s
jobmanager.adaptive-scheduler.scaling-interval.max: 60s
taskmanager.load-balance.mode: SLOTS
```

`Adaptive` — документированное значение `jobmanager.scheduler` во Flink 1.20.3 (`Default`, `Adaptive`, `AdaptiveBatch`). Настройки приведены **как пример**, они не включены автоматически в исходный Compose. Добавляйте их в существующий блок `FLINK_PROPERTIES`, не создавая второй блок с тем же именем.

| Параметр | Что делает | Чего **не** гарантирует |
|---|---|---|
| `jobmanager.scheduler: Adaptive` | Может уменьшать/увеличивать параллелизм streaming Job в пределах конфигурации при изменении ресурсов | Рестарт при каждом изменении числа TM |
| `prefer-minimal-taskmanagers: false` | Отключает предпочтение минимального числа TM | Жёсткое распределение 2+2 |
| `scaling-interval.min: 30s` | Минимальный интервал между операциями rescale | Рестарт строго через 30 секунд |
| `scaling-interval.max: 60s` | Позволяет выполнить rescale при наличии изменений ресурсов, даже если обычный порог увеличения не достигнут | Рестарт каждые 60 секунд или при появлении незадействованного TM |
| `taskmanager.load-balance.mode: SLOTS` | Стремится распределять **выделяемые слоты** между TM | Динамическую миграцию работающих subtasks |

При `parallelism=4` и четырёх работающих subtask на TM2, возврат TM1 **необязательно приводит к rescale**, потому что желаемый parallelism уже достигнут. Adaptive Scheduler при rescale обычно перезапускает **весь Job**. Для session cluster с несколькими Job документация Flink предупреждает об ограничениях Adaptive Scheduler при разделении ресурсов.

**Если требуется обязательное перераспределение 4 → 2+2 после возврата TM1:** используйте внешнее управление: отслеживание `GET /taskmanagers`, ожидание устойчивой регистрации двух TM, `stop-with-savepoint`, повторный `flink run -s ...`, проверка размещения. Альтернатива — ручное выполнение такого цикла во время тестирования. Это управляемый рестарт с временной паузой обработки, а не миграция без прерывания.

**Ограничение памяти:** даже при 8 слотах на TM оставшийся TaskManager располагает только `2g` памяти процесса. Суммарная нагрузка четырёх JMS-сессий, RocksDB, сетевых буферов и Kafka producers может потребовать больше памяти. Успех восстановления слотов сам по себе не означает успешное выполнение под нагрузкой.

## 10. Тестирование (отправка сообщений) через jms-publisher

Для отправки сообщений необходимо использовать jms-publisher:

```bash
  postman request POST 'http://localhost:9800/api/load/request-reply' \
  --header 'Content-Type: application/json' \
  --body '{
    "count": 5000,
    "concurrency": 10,
    "includeResponses": false,
    "payload": {}
}'
```

Где:
`count` - количество запросов
`concurrency` - количество параллельных потоков
`includeResponses` - вывод в stdout ответов
`payload` - тело запроса json

Для расширенного испытания можно использовать отдельный Excel-план `datafirewall_ha_test_cases.xlsx`.

## 11. Логирование и диагностика

### Контейнерные логи

```bash
docker compose logs -f --tail=150 flink-jm1 flink-jm2 flink-jm3 flink-tm1 flink-tm2 flink-tm3
docker compose logs --tail=200 kafka kafka-init artemis ignite spring-api
```

### Логи TaskManager в Flink UI

Обычные файлы располагаются в `/opt/flink/log`. После перезапуска процесса имя может получить другой индекс:

```text
flink--taskexecutor-0-flink-tm1.log  # предыдущий процесс
flink--taskexecutor-1-flink-tm1.log  # новый процесс
```

Проверьте файлы и JVM-параметр текущего процесса:

```bash
docker compose exec flink-tm1 ls -lah /opt/flink/log
docker compose exec flink-tm1 sh -c 'ps -ef | grep "[l]og.file"'
```

Flink Web UI читает лог через API кластера, а не напрямую из вывода `docker compose logs`. Если файлы есть, но в UI пусто, сначала получите **актуальный** ID TaskManager и список доступных логов:

```bash
curl -s http://localhost:8081/taskmanagers
# Далее подставить реальный идентификатор TM, без угловых скобок:
curl -i 'http://localhost:8081/taskmanagers/<TM_ID>/logs'
```

Проверьте настройки логирования и ошибки JobManager/TaskManager. При обычном `docker compose restart` содержимое записываемого слоя контейнера остаётся, но при `--force-recreate` старые файлы **без отдельного log volume могут исчезнуть**. Текущий общий Compose **не монтирует `/opt/flink/log` отдельным volume для TM**; если требуется сохранение файлов между пересозданиями, настройте разные volumes для `flink-tm1` и `flink-tm2` с корректными правами пользователя `flink`.

### Состояние Flink

```bash
# Список Job
docker compose exec flink-jm1 flink list

# Список TaskManager и свободных слотов (REST)
curl -s http://localhost:8081/taskmanagers

# Текущая конфигурация REST JobManager
curl -s http://localhost:8081/jobmanager/config

# Список Job по REST
curl -s http://localhost:8081/jobs/overview
```

Если `localhost:8081` относится к неактивному/недоступному JobManager, используйте доступный REST endpoint `localhost:8082` или выполните CLI-запрос через работающий JobManager.

## 12. Пересборка образов и обновление конфигурации

### Полная пересборка локальных Docker-образов

```bash
docker compose build --no-cache --pull
docker compose up -d --force-recreate --remove-orphans
```

Эти команды **не удаляют именованные volumes**. Флаг `--no-cache` применим к `build`, не к повторному скачиванию чужих образов.

### Пересборка Flink

В исходном Compose у четырёх Flink-сервисов общий Dockerfile, но нет обязательного общего `image:`. Без `image:` Compose вправе присваивать разные служебные имена образам. Старые образы после обновления тега могут отображаться как `<none>:<none>` — это не означает неисправность.

**Опционально** укажите один и тот же образ в общем блоке:

```yaml
x-flink-common: &flink-common
  image: datafirewall/flink:1.20.3-custom
  build:
    context: .
    dockerfile: docker/flink/Dockerfile
  # ... остальные настройки общего блока оставить как были
```

После этого можно собрать один образ и пересоздать все четыре сервиса:

```bash
docker compose build --no-cache flink-jm1
docker compose up -d --no-build --force-recreate \
  flink-jm1 flink-jm2 flink-jm3 flink-tm1 flink-tm2 flink-tm3
```

`image:` имеет смысл только при **одинаковом образе и Dockerfile** для всех четырёх контейнеров; роли JM/TM задаются полем `command`. Учитывайте влияние пересоздания на выполняемый Job и checkpointing.

### Обновление `job.properties` без пересборки

```bash
# На хосте (из каталога Compose)
sha256sum ./jobs/job.properties

# В JobManager
docker compose exec flink-jm1 sha256sum /opt/flink/usrlib/job.properties
```

Хеши должны совпадать. Если файл обновился, а Job по-прежнему использует старый URL (например, `http://ignite:9900`), причина обычно в **ранее запущенном Job**, а не Docker mount. Применяйте изменённый `job.properties` посредством новой отправки Job; для stateful Job предпочтителен savepoint.

### Изменения `.env` и конфигурации Flink

После изменения переменных `FLINK_TM_SLOTS`, `FLINK_PARALLELISM`, `FLINK_TM_MEMORY`, `jobmanager.scheduler` и прочих кластерных настроек проверьте результирующую конфигурацию и пересоздайте необходимые контейнеры:

```bash
docker compose config -q
docker compose up -d --no-deps --force-recreate \
  flink-jm1 flink-jm2 flink-jm3 flink-tm1 flink-tm2 flink-tm3
```

**На stateful Job это влияет:** перед запланированным обновлением рекомендуется управляемый stop/savepoint. При `--force-recreate` без savepoint восстановление зависит от актуального checkpoint и корректности реализации внешних источников/приёмников.

Удаление неиспользуемых dangling images (не удаляет используемые контейнерами):

```bash
docker image prune -f
```

## 13. Типовые проблемы

### Все subtasks работают на одном TM

Это допустимо при наличии свободных слотов и достаточных ресурсов. `taskmanager.load-balance.mode: SLOTS` помогает при **новом планировании**, но после возврата TM автоматическая миграция **не гарантирована**. Для обязательного возврата к 2+2 используйте управляемую переотправку Job с savepoint и проверкой размещения.

### Недостаточно слотов после отказа TM

Проверьте `taskmanager.numberOfTaskSlots`, число slot-sharing groups и реальную потребность Job. При двух TM по 8 слотов отказ одного оставляет 8 объявленных слотов. Но 2 GiB общего объёма памяти TM могут не выдержать всех параллельных worker под нагрузкой. Увеличение slots **не увеличивает RAM**.

### PostgreSQL не проходит healthcheck

Маркер `public.dfw_bootstrap` может отсутствовать, если скрипты не выполнены или прерваны. Healthcheck использует предварительную проверку наличия таблицы; посмотрите `docker compose logs postgres` и выполните `SELECT * FROM public.dfw_bootstrap`. При повторном старте с уже созданным volume init SQL **не запускается заново**.

### MinIO не запускается или bucket отсутствует

Проверьте `minio`, `minio-init`, их healthcheck, доступность `minio:9000`, создание bucket `flink` и креденшелы из `.env`. Образ от Chainguard может требовать доступа к соответствующему реестру. Не удаляйте MinIO volume для устранения ошибки pull или сети.

### Kafka/Artemis не принимают сообщения

Проверьте Kafka broker health, темы `rules-update`, `processing-audit`, `cef-audit`, параметры SASL/SCRAM и правильный listener (`kafka:9092` внутри Docker). В Artemis проверьте фактическое существование очередей: `EXTRA_ARGS --queues ...` применяется при первой инициализации брокера, а существующий volume сохраняет прежнее состояние.

### `docker compose config` не отражает изменения

Проверьте текущий рабочий каталог, имя используемого файла (`compose.yaml` / пользовательский `compose(2).yaml`), `.env` и переменные окружения оболочки. При нестандартном имени используйте `docker compose -f <путь> ...`. Команды в этом README предполагают, что файл проекта называется `compose.yaml`.

## 14. Ограничения и рекомендации

1. **Только один Docker-хост.** Две JM, две TM и ZooKeeper 3/3 защищают от некоторых отказов контейнеров, но не от потери хоста, диска, Docker daemon или общей сети.
2. **Единичные брокеры и хранилища.** Kafka, Artemis, Ignite, PostgreSQL и MinIO не образуют в этой сборке HA-кластеров. Отказ этих сервисов проверяет retry/recovery, а не безостановочное переключение.
3. **Производительность.** Число слотов — не объём ресурсов. Для четырёх транзакционных worker и RocksDB протестируйте 2 GiB TM под реальной нагрузкой, JVM/RocksDB native memory и рост очередей.
4. **Семантика внешних транзакций.** Настройка checkpoint `EXACTLY_ONCE` не заменяет проверку атомарности JMS consume/produce, ack/commit, redelivery и корректности best-effort Kafka audit.
5. **Безопасность.** Тестовые пароли, JWT, открытые HTTP и SASL_PLAINTEXT не подходят для промышленной среды. Действующие секреты, попавшие в файлы/логи, следует заменить и не публиковать.
6. **Изменение parallelism и state.** Перед изменением схемы операторов/JAR/properties сохраняйте совместимый savepoint и проверяйте восстановление на стенде.
7. **Достоверность.** Этот README построен по переданным Compose/JAR-конвенциям и параметрам Job; работоспособность всех пользовательских Java endpoint, транзакций и HA-сценариев необходимо подтверждать интеграционными тестами. Сборка и запуск Docker здесь не выполнялись.

### Полезные ссылки на документацию

- [Apache Flink 1.20 — Configuration](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/config/)
- [Apache Flink 1.20 — Command-Line Interface](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/cli/)
- [Apache Flink 1.20 — REST API](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/rest_api/)
- [Apache Flink — Elastic Scaling (Adaptive Scheduler)](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/elastic_scaling/)

---

