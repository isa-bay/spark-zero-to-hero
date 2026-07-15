## Содержание

<details>
<summary>

### 1. Фундамент
</summary>

- **Введение в Spark**
  - Проблемы традиционной обработки данных (MapReduce)
  - Что такое Apache Spark?
  - История создания и философия
  - Области применения и версии Spark
  - Плюсы и минусы Spark, сравнение с альтернативами (Flink, Trino)
- **Установка и настройка**
  - Локальная установка (Windows, macOS, Linux)
  - Настройка Spark в Docker-контейнере
  - Подключение к облачным сервисам (EMR, Dataproc, HDInsight)
  - Сборка из исходников и подключение Spark к IDE (IntelliJ, VSCode)
  - Типовые конфигурационные файлы (spark-defaults.conf, log4j.properties)
- **Архитектура кластера**
  - Компоненты: Driver, Executor, Cluster Manager
  - Жизненный цикл Driver и Executor
  - Задачи (Tasks), стадии (Stages), этапы (Jobs)
  - Режимы развёртывания (client vs cluster)
  - Динамическое выделение ресурсов (Dynamic Allocation) — обзор
- **Архитектура приложения**
  - SparkContext, SparkSession, SparkConf
  - Spark UI и History Server
  - Логирование и конфигурирование для разных окружений (dev/stage/prod)
- **Основные абстракции данных**
  - **RDD**
    - Понятие, неизменяемость, отказоустойчивость (lineage)
    - Создание (parallelize, из внешних источников)
    - Трансформации (map, filter, flatMap, mapPartitions и др.) и действия (reduce, collect, count и др.)
    - Узкие и широкие зависимости, влияние на shuffle
  - **DataFrame**
    - Отличие от RDD: Catalyst, Tungsten, бестиповый API
    - Схема (StructType) и её задание
    - Реляционные операции
  - **Dataset**
    - Типобезопасность, энкодеры (Encoders)
    - Сочетание преимуществ RDD и DataFrame
  - **Сравнение RDD, DataFrame, Dataset** — когда что выбирать
- **Физическое исполнение**
  - Жизненный цикл запроса: парсинг → анализ → оптимизация → физический план → генерация кода → выполнение
  - Catalyst Optimizer: логические и физические правила
  - Tungsten: управление памятью вне кучи, кодогенерация целых стадий (Whole-Stage CodeGen)
  - Predicate pushdown и column pruning
- **Менеджеры кластеров**
  - Standalone (простой, встроенный)
  - Hadoop YARN (интеграция с Hadoop)
  - Mesos (историческая справка)
  - Kubernetes (современный стандарт, Spark Operator, поды)
  - Сравнение: выбор менеджера под задачу
- **Системы хранения**
  - Подключение к HDFS, S3 (s3a), ADLS, GCS, локальной FS
  - Важность локальности данных (Data Locality) и настройки `spark.locality.wait`
  - Поддержка облачных объектных хранилищ: консистентность, коммиттеры (S3A Committer, Magic Committer)

</details>

<details>
<summary>

### 2. API и работа с данными
</summary>

- **Spark SQL**
  - Создание таблиц и представлений (временные, глобальные, перманентные)
  - Интеграция с Hive Metastore
  - Выполнение SQL-запросов (`spark.sql`)
  - Взаимодействие DataFrame и SQL
  - Встроенные функции (строковые, математические, агрегатные)
  - `cube`, `rollup`, `grouping sets` для многомерных отчётов
- **DataFrame API**
  - Базовые операции: select, withColumn, drop, filter/where, distinct
  - Агрегации: groupBy, agg, pivot
  - Типы join: inner, outer, left, right, semi, anti; cross join
  - Union, intersect, except
  - Работа со сложными типами:
    - StructType / StructField
    - ArrayType (explode, posexplode, array_contains, filter, transform)
    - MapType (keys, values, explode)
    - JSON: from_json, to_json, get_json_object, schema_of_json
  - Обработка null: fill, drop, replace; функции coalesce, ifnull, isnull, nanvl, when
  - Оконные функции:
    - Определение окна (partitionBy, orderBy, rowsBetween/rangeBetween)
    - Ранжирующие: row_number, rank, dense_rank, percent_rank, ntile
    - Аналитические: lead, lag, first_value, last_value
    - Агрегатные функции с окнами
- **RDD API (для legacy и низкоуровневых операций)**
  - Трансформации: map, flatMap, filter, mapPartitions, mapPartitionsWithIndex, glom
  - PairRDD: groupByKey, reduceByKey, aggregateByKey, combineByKey, sortByKey
  - Действия: reduce, fold, aggregate, collect, count, take, top, foreach, foreachPartition
  - Накопители (Accumulators) и широковещательные переменные (Broadcast Variables)
  - Сохранение RDD в файлы (saveAsTextFile, saveAsObjectFile)
- **Работа с датой и временем**
  - Парсинг: to_date, to_timestamp, unix_timestamp
  - Извлечение компонентов: year, month, day, hour, minute, second
  - Арифметика: date_add, date_sub, datediff, months_between, add_months
  - Работа с временными зонами: from_utc_timestamp, to_utc_timestamp
- **Пользовательские функции (UDF)**
  - UDF: регистрация, ограничения (чёрный ящик для Catalyst)
  - UDAF (агрегатные функции) – через Aggregator API
  - Pandas UDF (векторизованные): Series → Series, Iterator of Series → Iterator of Series
  - Pandas Function APIs: groupby().applyInPandas(), mapInPandas()
  - PySpark UDF vs Pandas UDF: когда что применять
  - UDF в Scala/Java и рекомендации по производительности
- **Дополнительные API**
  - Чтение/запись через JDBC: параллелизация, настройка fetchsize, batchsize
  - Работа с бинарными данными и схемами Avro
  - Статистические функции: summary, approxQuantile, corr, crosstab, sampleBy

</details>

<details>
<summary>

### 3. Оптимизация и тонкая настройка
</summary>

- **Понимание планов запросов**
  - Чтение и интерпретация `explain()` (включая формат `formatted`, `cost`, `codegen`)
  - Разница между логическим и физическим планом
  - Анализ узких мест: Exchange, Sort, BroadcastExchange
  - Использование DAG визуализации в Spark UI
- **Партиционирование**
  - Понятие партиции, факторы, влияющие на их количество
  - `repartition()` (полный shuffle) vs `coalesce()` (сужение без shuffle)
  - `spark.sql.shuffle.partitions`, `spark.default.parallelism`
  - Партиционирование при записи (`partitionBy`), последствия для small files
  - Бакетирование (Bucketing): создание, использование при join, ограничения
  - Диапазонное и хэш-партиционирование
- **Управление памятью**
  - Архитектура памяти Executor: On-Heap и Off-Heap (Tungsten)
  - Области памяти: Execution, Storage, Reserved, User Memory
  - `spark.memory.fraction`, `spark.memory.storageFraction`
  - Настройка GC: G1GC, Parallel GC, мониторинг пауз через Spark UI / jstat
  - Spill to disk: причины, влияние, минимизация
  - OutOfMemoryError: диагностика по логам и UI, типовые решения
  - Настройка числа ядер и памяти исполнителей (executor-cores, executor-memory)
- **Кэширование и сохранение**
  - `cache()` vs `persist()`, уровни хранения (MEMORY_ONLY, MEMORY_AND_DISK, etc.)
  - Сериализация в кэше (Kryo)
  - Checkpointing: разрыв lineage, запись на надёжное хранилище, отличие от persist
  - Когда кэшировать, а когда нет (анализ lineage, многократное использование)
- **Оптимизация Shuffle**
  - Почему shuffle дорог: сетевой обмен, диск, сериализация
  - Map-side combine (reduceByKey vs groupByKey)
  - Настройка буферов: `spark.reducer.maxSizeInFlight`, `spark.shuffle.file.buffer`
  - Сжатие shuffle: `spark.shuffle.compress`, `spark.io.compression.codec`
  - Data Skew (перекос данных):
    - Обнаружение перекоса через Spark UI (размер задач, время выполнения)
    - Техники борьбы: salting ключей, разделение ключей на горячие/холодные, AQE Skew Join Optimization
  - Speculative Execution: борьба с медленными задачами
- **Join-оптимизации**
  - Стратегии: Broadcast Hash Join, Sort Merge Join, Shuffled Hash Join, Cartesian Join
  - Как Spark выбирает стратегию (cost-based оптимизатор CBO, сбор статистики)
  - `spark.sql.autoBroadcastJoinThreshold`, хинт `/*+ BROADCAST(t) */`
  - Оптимизация при перекосе данных в join: salt, AQE Dynamic Optimize Skew Join
  - Bucket Join: преимущества и требования
  - Оптимизация сбора статистики таблиц (`ANALYZE TABLE`)
- **Настройка сериализации**
  - Java serialization (медленно, громоздко)
  - Kryo serialization: включение, регистрация классов, `spark.kryo.registrationRequired`
- **Адаптивное выполнение (AQE)**
  - Основные фичи: динамическое слияние shuffle-партиций, динамическая смена стратегии join, оптимизация перекоса
  - Настройка и мониторинг AQE
- **Динамическое выделение ресурсов**
  - Принцип работы: освобождение idle executor-ов, запрос новых
  - Настройка для YARN, Kubernetes
  - Взаимодействие с shuffle-файлами и внешним shuffle-сервисом
- **Работа с маленькими файлами**
  - Проблема “small files” на чтении и записи
  - Компактирование файлов (Spark job, Delta Optimize, Hudi Clustering)
  - Настройка `maxRecordsPerFile` при записи
  - Коалесценция перед записью

</details>

<details>
<summary>

### 4. Экосистема и интеграции
</summary>

- **Spark Streaming**
  - **DStream** (устаревший, ознакомительно): создание, операции с окнами
  - **Structured Streaming**
    - Модель бесконечной таблицы и программирование как над статическим DataFrame
    - Режимы вывода: Append, Update, Complete
    - Триггеры: Processing time, Event time, Once, Continuous
    - Водяные знаки (Watermarking) и обработка поздних событий
    - Exactly-once семантика: чекпоинтинг, WAL, идемпотентные приёмники
    - Stateful-обработка: mapGroupsWithState, flatMapGroupsWithState
    - Соединения: stream-static и stream-stream join
    - Дедупликация событий (dropDuplicates с watermark)
    - Интеграция с Kafka: источники, смещения (offsets), стратегии сброса, SASL/SSL
    - Продвинутые приёмники: foreachBatch, Cassandra, Delta, Iceberg
    - Режим continuous processing (особенности и ограничения)
- **Spark MLlib**
  - Pipeline API: Transformer, Estimator, Evaluator, PipelineModel
  - Feature Engineering:
    - Векторизация: VectorAssembler, CountVectorizer, TF-IDF
    - Нормализация: StandardScaler, MinMaxScaler
    - Кодирование категориальных признаков: StringIndexer, OneHotEncoder, Bucketizer
    - Обработка пропусков: Imputer
  - Алгоритмы: логистическая регрессия, случайный лес, градиентный бустинг, K-means
  - Кросс-валидация и подбор гиперпараметров
  - Сохранение и загрузка моделей (MLeap, PMML)
  - Feature Store: концепция и интеграция с Spark (Feast, Databricks Feature Store)
- **GraphX / GraphFrames**
  - Абстракции: Graph, VertexRDD, EdgeRDD, GraphFrame
  - Алгоритмы: PageRank, Connected Components, Label Propagation
  - Операторы: aggregateMessages, motif finding
  - GraphFrames: преимущества перед GraphX (DataFrame-based API)
- **Глубокое обучение**
  - TensorFlow on Spark (Horovod, TFoS)
  - PyTorch и Spark (TorchDistributor, SparkTorch)
  - Ускорение инференса с Pandas UDF
- **Языки программирования**
  - Scala: нативный язык, максимальная производительность, implicit encoders
  - PySpark: Python API, Pandas UDF, Arrow-оптимизации
  - SparkR, Java – обзор
  - SQL как универсальный интерфейс
  - Выбор языка под задачу и команду
- **Форматы данных**
  - CSV, JSON: плюсы, минусы, настройки парсинга (опция `mode` для bad records)
  - Parquet: колоночное хранение, сжатие, предикатный пушдаун, схема эволюции
  - Avro: row-based, интеграция со Schema Registry
  - ORC: оптимизирован для Hive, преимущества
  - Сравнение Parquet vs Avro vs ORC, выбор для слоёв данных (raw vs processed)
- **Облачные интеграции**
  - Amazon EMR, Google Dataproc, Azure HDInsight: особенности настройки, работа с объектными хранилищами
  - Databricks: управляемый Spark, Delta Lake, Workflows, SQL Analytics
  - Serverless Spark (AWS Glue, Google Dataproc Serverless)

</details>

<details>
<summary>

### 5. Продвинутые темы и инструменты Data Engineer
</summary>

- **Delta Lake (глубокое погружение)**
  - ACID-транзакции, оптимистическая конкурентность
  - Time Travel, audit logs, восстановление состояния таблицы
  - Schema enforcement и evolution
  - Upsert / Merge: синтаксис, оптимизация, работа с несколькими условиями
  - Vacuum: очистка старых файлов снапшотов
  - Delta Log: структура, checkpoints
  - Оптимизации: Compaction (OPTIMIZE), Z-Ordering, Liquid Clustering
  - Delta Live Tables (DLT): декларативный ETL, контроль качества данных (expectations)
  - Change Data Feed (CDF) для CDC-сценариев
- **Apache Iceberg**
  - Архитектура: метаданные, манифесты, файлы данных
  - Преимущества: скрытое партиционирование, эволюция схемы, путешествие во времени
  - Row-level delete/update (merge-on-read vs copy-on-write)
  - Интеграция со Spark: чтение, запись, процедуры (rewrite_manifests, expire_snapshots)
  - Сравнение с Delta Lake и Hudi
- **Apache Hudi**
  - Таблицы Copy-on-Write vs Merge-on-Read
  - Индексация: bloom filter, bucket index, HBase index
  - Incremental Query, Time Travel
  - Clustering и compaction
  - Встроенная обработка CDC
- **Lakehouse Architecture**
  - Концепция Lakehouse: единое хранилище для всех типов нагрузок
  - Медальонная архитектура (Bronze, Silver, Gold) на Spark + Delta/Iceberg
  - Построение многослойных пайплайнов с инкрементальной загрузкой
  - Управление качеством данных в каждом слое
- **Инструменты разработки и тестирования**
  - Локальная разработка: `spark-submit`, Jupyter, Zeppelin, Databricks Notebooks
  - Юнит-тестирование:
    - `chispa` (PySpark assertions)
    - `spark-testing-base` (Scala)
    - `pytest` с фикстурами локального SparkSession
  - Интеграционное тестирование с контейнерами (Testcontainers, MinIO для S3)
  - Отладка: чтение DAG, анализ логов драйвера и экзекьюторов, History Server
  - CI/CD для Spark: сборка fat JAR, упаковка PySpark, пайплайны в GitLab/GitHub Actions
- **Безопасность**
  - Аутентификация: Kerberos, токены, OAuth2
  - Авторизация: политики через Ranger/Sentry, SQL standard
  - Шифрование: на диске (HDFS encryption zone, TDE), в сети (TLS/SSL)
  - Безопасность на уровне строк и столбцов, динамическое маскирование
  - Безопасность в облаке: IAM роли, VPC, приватные соединения
- **Мониторинг и observability**
  - Spark UI глубоко: вкладки Stages (input size, shuffle read/write, GC time), Storage, SQL, Executors
  - History Server: настройка, очистка, сохранение логов событий (S3, HDFS)
  - Метрики: Prometheus PushGateway, Graphite, Ganglia
  - Настройка Spark Listeners для кастомного сбора метрик
  - Оповещения и дашборды (Grafana)
  - Агрегация логов драйверов и экзекьюторов (EFK, ELK, Splunk)
- **Управление зависимостями**
  - Fat JAR для Scala (Maven Shade, sbt-assembly), разрешение конфликтов
  - `--py-files`, `--archives`, `--jars` для PySpark
  - `--packages` (координаты Maven)
  - Проблемы конфликта версий Spark-зависимостей и классовых путей

</details>

<details>
<summary>

### 6. Эксплуатация и Data Engineering Ops
</summary>

- **Оркестрация задач**
  - Apache Airflow: SparkSubmitOperator, датчики файлов, DAG-зависимости
  - Databricks Workflows, Azure Data Factory, AWS Step Functions
  - Управление расписаниями, повторные попытки, оповещения
  - Параметризация заданий (передача дат, окружений)
- **CI/CD для Spark-приложений**
  - Автоматическая сборка и тестирование в PR
  - Версионирование артефактов (JAR, Python wheel)
  - Деплой в разные среды (Dev → Staging → Prod)
  - Canary-запуски, откат версий
- **Управление инфраструктурой (IaC)**
  - Развёртывание Spark-кластеров через Terraform, CloudFormation, Bicep
  - Kubernetes: Spark-on-k8s operator, настройка pod template, GPU/FPGA
  - Мультитенантные кластеры, очереди YARN/Fair Scheduler, изоляция ресурсов
- **Качество данных**
  - Библиотеки: Deequ (Scala), Great Expectations (Python), PyDeequ
  - Валидация данных в пайплайне: ожидания, метрики, профилирование
  - Обработка “плохих” записей (bad records): сохранение в отдельную таблицу, dead letter queue
  - Мониторинг дрейфа данных и схемы
- **Управление схемой данных**
  - Schema Registry (Apicurio, Confluent) для Avro/Protobuf
  - Эволюция схемы в Spark: mergeSchema, overwriteSchema, Delta/Iceberg schema evolution
- **Архивирование и жизненный цикл данных**
  - Time Travel для восстановления на определённый момент
  - Expire snapshots, Vacuum
  - Перемещение данных по storage tier (hot/warm/cold) в облаке
- **Оптимизация стоимости**
  - Spot/Preemptible инстансы, настройка failover
  - Автомасштабирование кластеров под нагрузку
  - Выбор правильного формата хранения и степени сжатия
  - Мониторинг затрат на уровне заданий (теги, метрики)

</details>

<details>
<summary>

### 7. Паттерны проектирования и лучшие практики
</summary>

- **ETL/ELT паттерны**
  - Полная и инкрементальная загрузка (по дате, по watermark, CDC)
  - Медленно меняющиеся измерения (SCD Type 1, 2) на Spark
  - Дедупликация записей (полная и в потоке)
  - Upsert-паттерны с использованием Delta Merge
- **Потоковые паттерны**
  - Обогащение потока статическими или потоковыми данными (stream-table join, stream-stream join)
  - Обработка запоздалых событий и пересчёт итогов
  - Идемпотентная запись в приёмники (checkpoint + уникальный идентификатор)
  - Паттерн “dead letter queue” для некорректных сообщений
- **Антипаттерны (чего НЕ делать)**
  - `collect()` на больших объёмах → драйвер OOM
  - `groupByKey` вместо `reduceByKey`
  - Чтение множества мелких файлов без предварительного слияния
  - Создание огромного числа мелких партиций (и наоборот)
  - Злоупотребление UDF при наличии встроенных функций
  - Неправильный уровень кэширования или отсутствие кэша для повторно используемых данных
  - Использование shuffle без необходимости
- **Организация кода**
  - Структура PySpark-проекта: конфиги (YAML), трансформации как чистые функции, джобы как скрипты
  - Scala проект: sbt, структура пакетов, внедрение конфигураций (Typesafe Config)
  - Повторное использование логики через библиотеки
  - Логирование и обработка ошибок в production-коде
- **Версионирование кода и данных**
  - Git Flow/Trunk-based для кода
  - DVC (Data Version Control) или встроенные возможности Delta/Iceberg для версионирования данных
- **Паттерны Lakehouse**
  - Medallion (бронза → серебро → золото) с пошаговым улучшением качества
  - Обработка персональных данных: GDPR-удаление, маскирование

</details>

<details>
<summary>

### 8. Собеседование и уровень Senior
</summary>

- **Типичные вопросы (с фокусом на глубокое понимание)**
  - Расскажите об архитектуре Spark: что происходит при `spark.sql("...").show()`?
  - RDD vs DataFrame vs Dataset — что использовать и когда?
  - Как работает Catalyst Optimizer? Приведите примеры правил.
  - Что такое shuffle и как его минимизировать? Отличие reduceByKey от groupByKey.
  - Стратегии join-ов, как Spark выбирает план, что такое CBO?
  - AQE: как работает, какие проблемы решает?
  - Data Skew: методы диагностики и решения (salting, AQE, join hints).
  - Как устроена работа с памятью в Tungsten? Что такое off-heap?
  - Как добиться exactly-once в Structured Streaming?
- **Практические кейсы (System Design)**
  - Спроектируйте пайплайн потоковой обработки кликстрима с гарантией exactly-once и задержкой < 1 сек.
  - Постройте озеро данных компании с 200+ источниками, обеспечьте качество, безопасность и самообслуживание.
  - Миграция ETL с классического DWH (Teradata/Redshift) на Spark Lakehouse.
  - Обработка CDC из реляционных БД (Debezium → Kafka → Spark) с обновлением витрин.
  - Архитектура для переобучения ML-моделей на Spark и развёртывания в production.
- **Тренды и развитие**
  - Spark Connect: клиент-серверный API, преимущества для data engineering.
  - Project Zen: улучшения PySpark, упрощение разработки.
  - Lakehouse-форматы: Iceberg v2, Hudi 1.0, Delta 3.0 — сравнение и тренды.
  - Spark + LLM: интеграция с трансформерами, инференс на больших данных, векторные представления.
  - Spark на Kubernetes: запуск и мониторинг в production, Spark Operator.

</details>
