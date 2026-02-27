## Содержание

<details>
<summary>

### 1. Фундамент
</summary>

- [Введение в Spark]()
  - [Проблемы традиционной обработки данных (MapReduce)]()
  - [Что такое Apache Spark?]()
  - [История создания]()
  - [Философия Spark]()
  - [Области применения]()
  - [Версии Spark]()
  - [Плюсы и минусы]()
- [Установка и настройка]()
  - []()
  - []()
  - []()
  - []()
  - []()
- [Архитектура кластера Spark]()
  - [Компоненты]()
  - [Роль и жизненный цикл Driver-а]()
  - [Роль и жизненный цикл Executor-ов]()
  - [Задачи (Tasks), стадии (Stages), этапы (Jobs)]()
  - [Режимы развертывания (Deploy Modes): client vs cluster]()
- [Архитектура приложения]()
  - [SparkContext (SparkContext, SparkSession)]()
  - [SparkConf — конфигурация приложения]()
  - [Spark UI — интерфейс для мониторинга]()
- [Основные абстракции данных (Core Abstractions)]()
  - [RDD (Resilient Distributed Dataset)]()
    - [Что такое RDD?]()
    - [Создание RDD]()
    - [Трансформации (Transformations)]()
    - [Действия (Actions)]()
    - [Узкие и широкие зависимости]()
  - [DataFrame (Распределенная коллекция строк)]()
    - [Отличие от RDD]()
    - [Реляционные операции]()
    - [Бестиповый API]()
  - [Dataset (Типизированный интерфейс)]()
    - [Сочетание преимуществ RDD (типобезопасность) и DataFrame (оптимизация)]()
    - [Энкодеры (Encoders)]()
  - [Сравнение и выбор: RDD vs DataFrame vs Dataset]()
- [Физическое исполнение]()
  - [Процесс выполнения запроса: от кода до результата]()
  - [Анализ логического плана (Logical Plan)]()
  - [Оптимизация логического плана (Catalyst Optimizer)]()
  - [Планирование физического плана (Physical Planning)]()
  - [Генерация кода (Whole-stage Code Generation)]()
  - [Выполнение задач на кластере (Tungsten)]()
- [Менеджеры кластеров (Cluster Managers)]()
  - [Standalone: простой кластер, встроенный в Spark]()
  - [Apache Hadoop YARN: интеграция с Hadoop-экосистемой]()
  - [Apache Mesos: (историческая справка)]()
  - [Kubernetes: современный стандарт для оркестрации контейнеров]()
  - [Сравнение и выбор менеджера]()
- [Система хранения (Storage) для Spark]()
  - [Подключение к различным источникам]()
  - [Важность размещения данных (Data Locality)]()
</details>

<details>
<summary>

### 2. API и работа с данными (Синтаксис и инструменты)
</summary>

- Spark SQL
  - Основы: создание таблиц и представлений
  - Выполнение SQL-запросов в Spark (`spark.sql`)
  - Взаимодействие DataFrames и SQL-таблиц
  - Встроенные функции (Built-in Functions)
- DataFrame API (основной инструмент инженера)
  - Основные операции:
    - select, withColumn, drop, filter/where
    - groupBy, agg, pivot
    - join (все типы: inner, outer, left, right, semi, anti)
    - union, intersect, except
  - Работа со сложными типами:
    - Структуры (StructType, StructField)
    - Массивы (ArrayType) и функции для работы с ними (explode, split, array_contains)
    - Карты (MapType)
    - JSON-данные (from_json, to_json, get_json_object, json_tuple)
  - Работа с null-значениями:
    - Обработка null: fill, drop, replace
    - Функции для работы с null: `isNull`, `isNotNull`, `coalesce`, `when`, `nanvl`
  - Оконные функции (Window Functions)
    - Определение оконных спецификаций (partitionBy, orderBy, rowsBetween)
    - Ранжирующие функции (rank, dense_rank, row_number)
    - Аналитические функции (lead, lag, first_value, last_value)
    - Агрегатные функции с окнами
- RDD API (для legacy-кода и сложных случаев)
  - Основные трансформации: map, flatMap, filter, mapPartitions, groupByKey, reduceByKey, aggregateByKey, sortByKey
  - Действия: reduce, collect, count, take, saveAsTextFile
  - PairRDD и работа с ключ-значение
  - Накопления (Accumulators) и широковещательные переменные (Broadcast Variables)
- Работа с датой и временем
  - Парсинг строк в даты (to_date, to_timestamp)
  - Извлечение частей даты (year, month, day, hour)
  - Арифметика дат (date_add, date_sub, datediff, months_between)
  - Работа с временными зонами
- Пользовательские функции (UDFs)
  - UDF (User Defined Function): "Черный ящик" для Spark, пробуксовка производительности.
  - UDAF (User Defined Aggregation Function): Создание собственных агрегатных функций.
  - Pandas UDF (Vectorized UDF): Революция в производительности (PySpark). Использование Apache Arrow.
  - Pandas Function APIs: groupBy().apply(), groupBy().applyInPandas()
  - Когда использовать UDFs, а когда стараться избегать
</details>

<details>
<summary>

### 3. Оптимизация и тонкая настройка производительности (Performance Tuning)
</summary>

- Понимание планов запросов (Query Plans)
  - Чтение и интерпретация `explain()`
  - Разница между логическим и физическим планом
  - Поиск узких мест в плане (Exchange, Sort, Shuffle)
- Партиционирование (Partitioning) — ключ к производительности
  - Что такое партиция в Spark?
  - Как Spark создает партиции
  - Влияние размера партиции на производительность (слишком маленькие или слишком большие)
  - Методы управления: `repartition()` (полный shuffle) vs `coalesce()` (сужение)
  - Способы разбиения данных: хэш-партиционирование, диапазонное партиционирование
  - Спецификация партиций при записи (partitionBy)
  - Бакетирование (Bucketing) — для оптимизации будущих join'ов
- Управление памятью и исполнителями (Memory Management)
  - Архитектура памяти Executor-а: On-Heap vs Off-Heap
  - Области памяти: Execution, Storage, Reserved, User Memory
  - Понятие Spark Memory Fraction (`spark.memory.fraction`, `spark.memory.storageFraction`)
  - Настройка параллелизма: `spark.sql.shuffle.partitions`, `spark.default.parallelism`
  - Настройка количества исполнителей, ядер и памяти (executor-cores, executor-memory)
- Кэширование и персистентность (Caching & Persistence)
  - Разница между cache() и persist()
  - Уровни хранения (Storage Levels): MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, OFF_HEAP, и их влияние на сериализацию
  - Проверка точек (Checkpointing) — для разрыва длинных цепочек зависимостей
  - Когда кэшировать, а когда нет
- Оптимизация Shuffle (Тасовки)
  - Что такое shuffle и почему это дорого
  - Настройка Shuffle Partitions
  - Избегание shuffle (например, через трансформации типа map-side combine)
  - Использование Salted Keys для решения проблемы перекоса данных (Data Skew)
  - Настройка параметров shuffle (буферы, сжатие)
- Join-оптимизации
  - Типы Join'ов в Spark: Broadcast Hash Join, Sort Merge Join, Shuffled Hash Join
  - Как Spark выбирает стратегию join'а
  - Принуждение к Broadcast Join (`/*+ BROADCAST(t) */`, `spark.sql.autoBroadcastJoinThreshold`)
  - Оптимизация для перекошенных данных при join (Skew Join Optimization)
- Настройка сериализации
  - Java сериализация (по умолчанию, но медленная)
  - Kryo сериализация — быстрее и компактнее. Как ее включить и зарегистрировать классы
- Адаптивное исполнение запросов (Adaptive Query Execution — AQE)
  - Что такое AQE (Spark 3.x)
  - Динамическое объединение перекошенных партиций (Dynamic Coalescing of Shuffle Partitions)
  - Динамическая смена стратегии join'а (Dynamic Switch Join Strategies)
  - Динамическая оптимизация перекоса (Dynamic Optimize Skew Join)
- Динамическое распределение ресурсов (Dynamic Resource Allocation)
  - Как Spark может возвращать неиспользуемые ресурсы обратно в кластер
  - Настройка для динамического масштабирования
</details>

<details>
<summary>

### 4. Экосистема и интеграции
</summary>

- Spark Streaming (Стриминговая обработка)
  - DStream (Discretized Stream)
    - Устаревший подход на базе RDD
  - Structured Streaming (современный стандарт)
    - Модель: бесконечная таблица
    - Режимы вывода (Output Modes): Append, Update, Complete
    - Триггеры (Triggers): Processing time, Event time, Once, Continuous
    - Обработка событийного времени (Event-time) и водяные знаки (Watermarking)
    - Обработка поздних данных (Late Data)
    - Обеспечение отказоустойчивости и exactly-once семантики (WAL, checkpointing)
    - Stateful Streaming: работа с состоянием (mapGroupsWithState, flatMapGroupsWithState)
- Spark MLlib (Машинное обучение)
  - Концепция Pipeline: Transformer, Estimator, Evaluator
  - Feature engineering: векторизация (VectorAssembler), нормализация, кодирование (StringIndexer, OneHotEncoder)
  - Основные алгоритмы: линейная регрессия, деревья решений, случайный лес, кластеризация (K-means)
  - Сохранение и загрузка моделей
- GraphX (Обработка графов)
  - Абстракции: Graph, VertexRDD, EdgeRDD
  - Алгоритмы: PageRank, Connected Components, Triangle Counting
  - Операторы на графах (mapVertices, mapEdges, subgraph, aggregateMessages)
- Глубокое обучение с Spark
  - Spark Deep Learning
  - Интеграция с TensorFlow (Horovod on Spark, TFoS) и PyTorch
- Языки программирования в Spark
  - Scala: Родной язык, максимальная производительность и возможности
  - PySpark: Python API. Популярность, экосистема, особенности (Pandas UDF)
  - SparkR: R API.
  - SQL: Универсальный интерфейс.
  - Сравнение и выбор языка для задач
- Форматы данных
  - Текстовые: CSV, JSON (плюсы и минусы)
  - Структурированные: Parquet (стандарт для Spark, колоночное хранение), Avro, ORC
  - Сравнение Parquet vs Avro vs ORC vs JSON
  - Векторный формат: Delta Lake (см. ниже)
</details>

<details>
<summary>

### 5. Продвинутые темы и инструменты Data Engineer
</summary>

- Delta Lake (Озеро данных с ACID-транзакциями)
  - Что такое Delta Lake и зачем он нужен
  - Основные возможности:
    - ACID-транзакции
    - Время путешествий (Time Travel) и аудит изменений
    - Schema enforcement и schema evolution
    - Upsert/Merge (команда `MERGE`)
    - Vacuum — очистка старых снапшотов
  - Формат хранения: Parquet + транзакционный лог (Delta Log)
  - Оптимизация Delta Lake: Z-Ordering, Liquid Clustering, Compact (bin-compaction)
  - Delta Live Tables (DLT) — декларативный ETL на Databricks
- Apache Iceberg и Apache Hudi
  - Альтернативы Delta Lake для управления таблицами в Data Lake
  - Сравнение: Iceberg, Hudi, Delta Lake — когда что выбирать
- Lakehouse Architecture (Архитектура Озеро-Хранилище)
  - Концепция объединения лучших качеств Data Lake и Data Warehouse
- Инструменты для разработки и тестирования
  - Разработка локально с использованием `spark-submit`
  - Интерактивная разработка: Jupyter Notebooks, Apache Zeppelin, Databricks Notebooks
  - Тестирование Spark-приложений:
    - `chispa` (библиотека для PySpark assertion)
    - `spark-testing-base` (для Scala)
    - Стратегии тестирования: модульные тесты трансформаций, интеграционные тесты с локальным Spark
  - Отладка: логи, Spark UI, история событий (History Server)
- Безопасность (Security)
  - Аутентификация: Kerberos, токены
  - Авторизация: SQL Standard, Ranger/Sentry
  - Шифрование данных: на диске (TDE), в сети (TLS/SSL)
  - Безопасность на уровне строк и столбцов (Row/Column level security)
  - Динамическое маскирование данных (Dynamic Data Masking)
- Мониторинг и логирование (Observability)
  - Spark UI: глубокий анализ (страница Stages, Storage, Environment, Executors, SQL)
  - История событий (Spark History Server)
  - Метрики Spark: интеграция с Prometheus, Graphite, Ganglia
  - Сбор логов драйвера и исполнителей (stdout, stderr) через YARN/Kubernetes
  - Настройка прослушивателей событий (Spark Listeners) для кастомного мониторинга
- Управление зависимостями
  - Упаковка приложения в "толстый" JAR (fat/uber JAR) для Scala
  - `--py-files` для PySpark
  - `--packages` для Maven-координат
  - Решение конфликтов версий библиотек
</details>

<details>
<summary>

### 6. Эксплуатация и инжиниринг данных (Data Engineering Ops)
</summary>

- Оркестрация Spark-задач (Workflow Orchestration)
  - Apache Airflow: DAG для Spark-задач (SparkSubmitOperator, SparkJDBCOperator)
  - Apache Oozie: (устаревший, но legacy)
  - Databricks Workflows / Azure Data Factory / AWS Step Functions
  - Управление зависимостями между задачами, повторные попытки, оповещения
- CI/CD для кода Spark
  - Тестирование кода в CI-пайплайне (GitLab CI, GitHub Actions, Jenkins)
  - Автоматизация сборки и деплоя JAR-файлов или PySpark-пакетов
- Управление инфраструктурой (IaC)
  - Развертывание Spark-кластеров через Terraform, CloudFormation
  - Управление кластерами на Kubernetes (Spark-on-k8s operator)
  - Облачные managed-сервисы: Amazon EMR, Google Cloud Dataproc, Azure HDInsight, Databricks
- Качество данных (Data Quality)
  - Проверка данных в пайплайнах: `Great Expectations`, `Deequ` (библиотека от Amazon)
  - Обработка "плохих" записей (bad records)
- Управление схемой данных (Schema Management)
  - Регистр схем (Schema Registry) для Avro/JSON
  - Эволюция схемы в Delta Lake / Iceberg
- Выбор формата хранения и партиционирования
  - Стратегии для различных типов запросов (OLAP)
  - Оптимизация стоимости в облаке (S3 vs HDD vs SSD)
</details>

<details>
<summary>

### 7. Паттерны проектирования и лучшие практики
</summary>

- ETL паттерны (Extract, Transform, Load)
  - Инкрементальная загрузка
  - Медленно меняющиеся измерения (SCD Type 1, 2) в Spark
  - Обработка дубликатов (дедупликация)
- Паттерны для стриминга
  - Обогащение стрима статическими/динамическими данными (stream-stream join, stream-static join)
  - Обработка опаздывающих событий
  - Обеспечение idempotent записи в приемники
- Антипаттерны Spark (Чего НЕ надо делать)
  - Использование `collect()` на больших данных
  - Группировка по ключу с огромным перекосом
  - Чтение огромного количества мелких файлов
  - Создание большого числа мелких партиций
  - Использование UDF, когда есть встроенная функция
  - Неправильная настройка количества shuffle-партиций
- Организация кода и модульность
  - Структура проектов на Scala (sbt, assembly)
  - Структура проектов на PySpark (пакеты, конфиги, модули с трансформациями)
  - Использование конфигурационных файлов (JSON, YAML) для параметров приложения
- Версионирование кода и данных
  - Git для кода
  - Data Version Control (DVC) или возможности озер данных (time travel) для версионирования данных
</details>

<details>
<summary>

### 8. Собеседование и дополнительные знания для Senior-уровня
</summary>

- Типичные вопросы на собеседовании
  - Расскажите про архитектуру Spark
  - В чем разница между DataFrame и RDD?
  - Как работает Catalyst Optimizer?
  - Что такое Shuffle? Как его избежать?
  - Как вы будете оптимизировать медленный join?
  - Что такое AQE и как оно помогает?
  - Как бы вы обработали перекос данных (data skew)?
- Практические кейсы (System Design)
  - Проектирование пайплайна для обработки кликстрима в реальном времени (миллионы событий/сек)
  - Построение озера данных для компании с множеством отделов и разнородными данными
  - Миграция ETL с хранилища на озеро данных
  - Обеспечение exactly-once обработки в стриминговом пайплайне
- Тренды и будущее
  - Project Zen (улучшение PySpark)
  - Spark Connect (клиент-серверная архитектура)
  - Развитие Lakehouse форматов (Iceberg, Hudi, Delta)
  - Интеграция с LLM и генеративным AI
</details>
