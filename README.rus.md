[![Build Status](https://travis-ci.org/postgrespro/pg_pathman.svg?branch=master)](https://travis-ci.org/postgrespro/pg_pathman)
[![PGXN version](https://badge.fury.io/pg/pg_pathman.svg)](https://badge.fury.io/pg/pg_pathman)

# pg_pathman

Модуль `pg_pathman` предоставляет оптимизированный механизм секционирования, а также функции для создания и управления секциями.

Расширение совместимо с PostgreSQL 9.5 (поддержка 9.6 будет добавлена в одном из ближайших обновлений).

## Концепция pg_pathman

**Секционирование** -- это способ разбиения одной большой таблицы на множество меньших по размеру. Для каждой записи можно однозначно определить секцию, в которой она должна храниться посредством вычисления ключа.
Секционирование в postgres основано на механизме наследования. Каждому наследнику задается условие CHECK CONSTRAINT. Например:

```
CREATE TABLE test (id SERIAL PRIMARY KEY, title TEXT);
CREATE TABLE test_1 (CHECK ( id >= 100 AND id < 200 )) INHERITS (test);
CREATE TABLE test_2 (CHECK ( id >= 200 AND id < 300 )) INHERITS (test);
```

Несмотря на гибкость, этот механизм обладает недостатками. Так при фильтрации данных оптимизатор вынужден перебирать все дочерние секции и сравнивать условие запроса с CHECK CONSTRAINT-ами секции, чтобы определить из каких секций ему следует загружать данные. При большом количестве секций это создает дополнительные накладные расходы, которые могут свести на нет выигрыш в производительности от применения секционирования.

Модуль `pg_pathman` предоставляет функции для создания и управления секциями, а также механизм секционирования, оптимизированный с учетом знания о структуре дочерних таблиц. Конфигурация сохраняется таблице `pathman_config`, каждая строка которой содержит запись для одной секционированной таблицы (название таблицы, атрибут и тип разбиения). В процессе инициализации `pg_pathman` кеширует конфигурацию дочерних таблиц в формате, удобном для быстрого поиска. Получив запрос типа `SELECT` к секционированной таблице, `pg_pathman` анализирует дерево условий запроса и выделяет из него условия вида:

```
ПЕРЕМЕННАЯ ОПЕРАТОР КОНСТАНТА
```
где `ПЕРЕМЕННАЯ` -- это атрибут, по которому было выполнено разбиение, `ОПЕРАТОР` -- оператор сравнения (поддерживаются =, <, <=, >, >=), `КОНСТАНТА` -- скалярное значение. Например:

```
WHERE id = 150
```
Затем основываясь на стратегии секционирования и условиях запроса `pg_pathman` находит в кеше соответствующие секции и строит план.

В текущей версии `pg_pathman` поддерживает следующие типы секционирования:

* **RANGE** - разбивает таблицу на секции по диапазонам ключевого аттрибута; для оптимизации построения плана используется метод бинарного поиска.
* **HASH** - данные равномерно распределяются по секциям в соответствии со значениями hash-функции, вычисленными по заданному целочисленному атрибуту.

More interesting features are yet to come. Stay tuned!

## Roadmap

 * Предоставить возможность установки пользовательских колбеков на создание\уничтожение партиции (issue [#22](https://github.com/postgrespro/pg_pathman/issues/22))
 * LIST-секционирование;
 * Оптимизация hash join для случая, когда обе таблицы секционированы по ключу join’а.

## Установка

Для установки pg_pathman выполните в директории модуля команду:
```
make install USE_PGXS=1
```
Модифицируйте параметр shared_preload_libraries в конфигурационном файле postgres.conf:
```
shared_preload_libraries = 'pg_pathman'
```
Для вступления изменений в силу потребуется перезагрузка сервера PostgreSQL. Затем выполните в psql:
```
CREATE EXTENSION pg_pathman;
```

> **Важно:** Если вы хотите собрать `pg_pathman` для работы с кастомной сборкой PostgreSQL, не забудьте установить переменную окружения `PG_CONFIG` равной пути к исполняемому файлу pg_config. Узнать больше о сборке расширений для PostgreSQL можно по ссылке: [here](https://wiki.postgresql.org/wiki/Building_and_Installing_PostgreSQL_Extension_Modules).

## Функции `pg_pathman`

### Создание секций
```plpgsql
create_hash_partitions(relation         REGCLASS,
                       attribute        TEXT,
                       partitions_count INTEGER,
                       partition_name   TEXT DEFAULT NULL)
```
Выполняет HASH-секционирование таблицы `relation` по целочисленному полю `attribute`. Параметр `partitions_count` определяет, сколько секций будет создано. Если `partition_data` установлен в значение `true`, то данные из родительской таблицы будут автоматически распределены по секциям. Стоит иметь в виду, что миграция данных может занять некоторое время, а данные заблокированы. Для конкурентной миграции данных см. функцию `partition_table_concurrently()`.

```plpgsql
create_range_partitions(relation       REGCLASS,
                        attribute      TEXT,
                        start_value    ANYELEMENT,
                        interval       ANYELEMENT,
                        count          INTEGER DEFAULT NULL
                        partition_data BOOLEAN DEFAULT true)

create_range_partitions(relation       REGCLASS,
                        attribute      TEXT,
                        start_value    ANYELEMENT,
                        interval       INTERVAL,
                        count          INTEGER DEFAULT NULL,
                        partition_data BOOLEAN DEFAULT true)
```
Выполняет RANGE-секционирование таблицы `relation` по полю `attribute`. Аргумент `start_value` задает начальное значение, `interval` -- диапазон значений внутри одной секции, `count` -- количество создаваемых секций (если не задано, то pathman попытается определить количество секций на основе значений аттрибута).

```plpgsql
create_partitions_from_range(relation       REGCLASS,
                             attribute      TEXT,
                             start_value    ANYELEMENT,
                             end_value      ANYELEMENT,
                             interval       ANYELEMENT,
                             partition_data BOOLEAN DEFAULT true)

create_partitions_from_range(relation       REGCLASS,
                             attribute      TEXT,
                             start_value    ANYELEMENT,
                             end_value      ANYELEMENT,
                             interval       INTERVAL,
                             partition_data BOOLEAN DEFAULT true)
```
Выполняет RANGE-секционирование для заданного диапазона таблицы `relation` по полю `attribute`.

### Миграция данных

```plpgsql
partition_table_concurrently(relation REGCLASS)
```
Запускает новый процесс (background worker) для конкурентного перемещения данных из родительской таблицы в дочерние секции. Рабочий процесс использует короткие транзакции для перемещения небольших объемов данных (порядка 10 тысяч записей) и, таким образом, не оказывает существенного влияния на работу пользователей.

```plpgsql
stop_concurrent_part_task(relation REGCLASS)
```
Останавливает процесс конкурентного партиционирования. Обратите внимание, что процесс завершается не мгновенно, а только по завершении текущей транзакции.

### Утилиты
```plpgsql
create_hash_update_trigger(parent REGCLASS)
```
Создает триггер на UPDATE для HASH секций. По-умолчанию триггер на обновление данных не создается, т.к. это создает дополнительные накладные расходы. Триггер полезен только в том случае, когда меняется значение ключевого аттрибута.
```plpgsql
create_range_update_trigger(parent REGCLASS)
```
Аналогично предыдущей, но для RANGE секций.

### Управление секциями
```plpgsql
split_range_partition(partition      REGCLASS,
                      value          ANYELEMENT,
                      partition_name TEXT DEFAULT NULL,)
```
Разбивает RANGE секцию `partition` на две секции по значению `value`.

```plpgsql
merge_range_partitions(partition1 REGCLASS, partition2 REGCLASS)
```
Объединяет две смежные RANGE секции. Данные из `partition2` копируются в `partition1`, после чего секция `partition2` удаляется.

```plpgsql
append_range_partition(p_relation     REGCLASS,
                       partition_name TEXT DEFAULT NULL)
```
Добавляет новую RANGE секцию с диапазоном `pathman_config.range_interval` в конец списка секций.

```plpgsql
prepend_range_partition(p_relation     REGCLASS,
                        partition_name TEXT DEFAULT NULL)
```
Добавляет новую RANGE секцию с диапазоном `pathman_config.range_interval` в начало списка секций.

```plpgsql
add_range_partition(relation       REGCLASS,
                    start_value    ANYELEMENT,
                    end_value      ANYELEMENT,
                    partition_name TEXT DEFAULT NULL)
```
Добавляет новую RANGE секцию с заданным диапазоном к секционированной таблице `relation`.

```plpgsql
drop_range_partition(partition TEXT)
```
Удаляет RANGE секцию вместе с содержащимися в ней данными.

```plpgsql
attach_range_partition(relation    REGCLASS,
                       partition   REGCLASS,
                       start_value ANYELEMENT,
                       end_value   ANYELEMENT)
```
Присоединяет существующую таблицу `partition` в качестве секции к ранее секционированной таблице `relation`. Структура присоединяемой таблицы должна в точности повторять структуру родительской.

```plpgsql
detach_range_partition(partition REGCLASS)
```
Отсоединяет секцию `partition`, после чего она становится независимой таблицей.

```plpgsql
disable_pathman_for(relation REGCLASS)
```
Отключает механизм секционирования `pg_pathman` для заданной таблицы. При этом созданные ранее секции остаются без изменений.

```plpgsql
drop_partitions(parent      REGCLASS,
                delete_data BOOLEAN DEFAULT FALSE)
```
Удаляет все секции таблицы `parent`. Если параметр `delete_data` задан как `false` (по-умолчанию `false`), то данные из секций копируются в родительскую таблицу.

### Дополнительные параметры

```plpgsql
enable_parent(relation  REGCLASS)
disable_parent(relation REGCLASS)
```
Включает/исключает родительскую таблицу в план запроса. В оригинальном планировщике PostgreSQL родительская таблица всегда включается в план запроса, даже если она пуста. Это создает дополнительные накладные расходы. Выполните `disable_parent()`, если вы не собираетесь хранить какие-либо данные в родительской таблице. Значение по-умолчанию зависит от того, был ли установлен параметр `partition_data` при первоначальном разбиении таблицы (см. функции `create_range_partitions()` и `create_partitions_from_range()`). Если он был установлен в значение `true`, то все данные были перемещены в секции, а родительская таблица отключена. В противном случае родительская таблица по-умолчанию влючена.

```plpgsql
enable_auto(relation  REGCLASS)
disable_auto(relation REGCLASS)
```
Включает/выключает автоматическое создание секций (только для RANGE секционирования). По-умолчанию включено.

## Custom plan nodes
`pg_pathman` вводит три новых узла плана (см. [custom plan nodes](https://wiki.postgresql.org/wiki/CustomScanAPI)), предназначенных для оптимизации времени выполнения:

- `RuntimeAppend` (замещает узел типа `Append`)
- `RuntimeMergeAppend` (замещает узел типа `MergeAppend`)
- `PartitionFilter` (выполняет работу INSERT-триггера)

`PartitionFilter` работает как прокси-узел для INSERT-запросов, распределяя новые записи по соответствующим секциям:

```
EXPLAIN (COSTS OFF)
INSERT INTO partitioned_table
SELECT generate_series(1, 10), random();
               QUERY PLAN
-----------------------------------------
 Insert on partitioned_table
   ->  Custom Scan (PartitionFilter)
         ->  Subquery Scan on "*SELECT*"
               ->  Result
(4 rows)
```

Узлы `RuntimeAppend` и `RuntimeMergeAppend` имеют между собой много общего: они нужны в случает, когда условие WHERE принимает форму:
```
ПЕРЕМЕННАЯ ОПЕРАТОР ПАРАМЕТР
```
Подобные выражения не могут быть оптимизированы во время планирования, т.к. значение параметра неизвестно до стадии выполнения. Проблема может быть решена путем встраивания дополнительной процедуры анализа в код `Append` узла, таким образом позволяя ему выбирать лишь необходимые субпланы из всего списка дочерних планов.

----------

Есть по меньшей мере несколько ситуаций, которые демонстрируют полезность таких узлов:

```
/* создаем таблицу, которую хотим секционировать */
CREATE TABLE partitioned_table(id INT NOT NULL, payload REAL);

/* заполняем данными */
INSERT INTO partitioned_table
SELECT generate_series(1, 1000), random();

/* выполняем секционирование */
SELECT create_hash_partitions('partitioned_table', 'id', 100);

/* создаем обычную таблицу */
CREATE TABLE some_table AS SELECT generate_series(1, 100) AS VAL;
```


 - **`id = (select ... limit 1)`**
```
EXPLAIN (COSTS OFF, ANALYZE) SELECT * FROM partitioned_table
WHERE id = (SELECT * FROM some_table LIMIT 1);
                                             QUERY PLAN
----------------------------------------------------------------------------------------------------
 Custom Scan (RuntimeAppend) (actual time=0.030..0.033 rows=1 loops=1)
   InitPlan 1 (returns $0)
     ->  Limit (actual time=0.011..0.011 rows=1 loops=1)
           ->  Seq Scan on some_table (actual time=0.010..0.010 rows=1 loops=1)
   ->  Seq Scan on partitioned_table_70 partitioned_table (actual time=0.004..0.006 rows=1 loops=1)
         Filter: (id = $0)
         Rows Removed by Filter: 9
 Planning time: 1.131 ms
 Execution time: 0.075 ms
(9 rows)

/* выключаем узел RuntimeAppend */
SET pg_pathman.enable_runtimeappend = f;

EXPLAIN (COSTS OFF, ANALYZE) SELECT * FROM partitioned_table
WHERE id = (SELECT * FROM some_table LIMIT 1);
                                    QUERY PLAN
----------------------------------------------------------------------------------
 Append (actual time=0.196..0.274 rows=1 loops=1)
   InitPlan 1 (returns $0)
     ->  Limit (actual time=0.005..0.005 rows=1 loops=1)
           ->  Seq Scan on some_table (actual time=0.003..0.003 rows=1 loops=1)
   ->  Seq Scan on partitioned_table_0 (actual time=0.014..0.014 rows=0 loops=1)
         Filter: (id = $0)
         Rows Removed by Filter: 6
   ->  Seq Scan on partitioned_table_1 (actual time=0.003..0.003 rows=0 loops=1)
         Filter: (id = $0)
         Rows Removed by Filter: 5
         ... /* more plans follow */
 Planning time: 1.140 ms
 Execution time: 0.855 ms
(306 rows)
```

 - **`id = ANY (select ...)`**
```
EXPLAIN (COSTS OFF, ANALYZE) SELECT * FROM partitioned_table
WHERE id = any (SELECT * FROM some_table limit 4);
                                                QUERY PLAN
-----------------------------------------------------------------------------------------------------------
 Nested Loop (actual time=0.025..0.060 rows=4 loops=1)
   ->  Limit (actual time=0.009..0.011 rows=4 loops=1)
         ->  Seq Scan on some_table (actual time=0.008..0.010 rows=4 loops=1)
   ->  Custom Scan (RuntimeAppend) (actual time=0.002..0.004 rows=1 loops=4)
         ->  Seq Scan on partitioned_table_70 partitioned_table (actual time=0.001..0.001 rows=10 loops=1)
         ->  Seq Scan on partitioned_table_26 partitioned_table (actual time=0.002..0.003 rows=9 loops=1)
         ->  Seq Scan on partitioned_table_27 partitioned_table (actual time=0.001..0.002 rows=20 loops=1)
         ->  Seq Scan on partitioned_table_63 partitioned_table (actual time=0.001..0.002 rows=9 loops=1)
 Planning time: 0.771 ms
 Execution time: 0.101 ms
(10 rows)

/* выключаем узел RuntimeAppend */
SET pg_pathman.enable_runtimeappend = f;

EXPLAIN (COSTS OFF, ANALYZE) SELECT * FROM partitioned_table
WHERE id = any (SELECT * FROM some_table limit 4);
                                       QUERY PLAN
-----------------------------------------------------------------------------------------
 Nested Loop Semi Join (actual time=0.531..1.526 rows=4 loops=1)
   Join Filter: (partitioned_table.id = some_table.val)
   Rows Removed by Join Filter: 3990
   ->  Append (actual time=0.190..0.470 rows=1000 loops=1)
         ->  Seq Scan on partitioned_table (actual time=0.187..0.187 rows=0 loops=1)
         ->  Seq Scan on partitioned_table_0 (actual time=0.002..0.004 rows=6 loops=1)
         ->  Seq Scan on partitioned_table_1 (actual time=0.001..0.001 rows=5 loops=1)
         ->  Seq Scan on partitioned_table_2 (actual time=0.002..0.004 rows=14 loops=1)
... /* 96 scans follow */
   ->  Materialize (actual time=0.000..0.000 rows=4 loops=1000)
         ->  Limit (actual time=0.005..0.006 rows=4 loops=1)
               ->  Seq Scan on some_table (actual time=0.003..0.004 rows=4 loops=1)
 Planning time: 2.169 ms
 Execution time: 2.059 ms
(110 rows)
```

 - **`NestLoop` involving a partitioned table**, which is omitted since it's occasionally shown above.

----------

Узнать больше о работе RuntimeAppend можно в [блоге](http://akorotkov.github.io/blog/2016/06/15/pg_pathman-runtime-append/) Александра Короткова.

## Примеры

### Common tips
- You can easily add **_partition_** column containing the names of the underlying partitions using the system attribute called **_tableoid_**:
```
SELECT tableoid::regclass AS partition, * FROM partitioned_table;
```
- Несмотря на то, что индексы на родительской таблице не очень полезны (т.к. таблица пуста), они тем не менее выполняют роль прототипов для создания индексов в дочерних таблицах: `pg_pathman` автоматически создает аналогичные индексы для каждой новой секции.

- Получить все текущие процессы конкурентного секционирования можно из представления `pathman_concurrent_part_tasks`:
```plpgsql
SELECT * FROM pathman_concurrent_part_tasks;
 userid | pid  | dbid  | relid | processed | status  
--------+------+-------+-------+-----------+---------
 dmitry | 7367 | 16384 | test  |    472000 | working
(1 row)
```

### HASH секционирование
Рассмотрим пример секционирования таблицы, используя HASH-стратегию на примере таблицы товаров.
```
CREATE TABLE items (
    id       SERIAL PRIMARY KEY,
    name     TEXT,
    code     BIGINT);

INSERT INTO items (id, name, code)
SELECT g, md5(g::text), random() * 100000
FROM generate_series(1, 100000) as g;
```
Если дочерние секции подразумевают наличие индексов, то стоит их создать в родительской таблице до разбиения. Тогда при разбиении pg_pathman автоматически создаст соответствующие индексы в дочерних.таблицах. Разобьем таблицу `hash_rel` на 100 секций по полю `value`:
```
SELECT create_hash_partitions('items', 'id', 100);
```
Пример построения плана для запроса с фильтрацией по ключевому полю:
```
SELECT * FROM items WHERE id = 1234;
  id  |               name               | code 
------+----------------------------------+------
 1234 | 81dc9bdb52d04dc20036dbd8313ed055 | 1855
(1 row)

EXPLAIN SELECT * FROM items WHERE id = 1234;
                                     QUERY PLAN                                     
------------------------------------------------------------------------------------
 Append  (cost=0.28..8.29 rows=0 width=0)
   ->  Index Scan using items_34_pkey on items_34  (cost=0.28..8.29 rows=0 width=0)
         Index Cond: (id = 1234)
```
Стоит отметить, что pg_pathman исключает из плана запроса родительскую таблицу, и чтобы получить данные из нее, следует использовать модификатор ONLY:
```
EXPLAIN SELECT * FROM ONLY items;
                      QUERY PLAN                      
------------------------------------------------------
 Seq Scan on items  (cost=0.00..0.00 rows=1 width=45)
```

### RANGE секционирование
Рассмотрим пример разбиения таблицы по диапазону дат. Пусть у нас имеется таблица логов:
```
CREATE TABLE journal (
    id      SERIAL,
    dt      TIMESTAMP NOT NULL,
    level   INTEGER,
    msg     TEXT
);
CREATE INDEX ON journal(dt);

INSERT INTO journal (dt, level, msg)
SELECT g, random()*6, md5(g::text)
FROM generate_series('2015-01-01'::date, '2015-12-31'::date, '1 minute') as g;
```
Разобьем таблицу на 365 секций так, чтобы каждая секция содержала данные за один день:
```
SELECT create_range_partitions('journal', 'dt', '2015-01-01'::date, '1 day'::interval);
```
Новые секции добавляются автоматически при вставке новых записей в непокрытую область. Однако есть возможность добавлять секции вручную. Для этого можно воспользоваться следующими функциями:
```
SELECT add_range_partition('journal', '2016-01-01'::date, '2016-01-07'::date);
SELECT append_range_partition('journal');
```
Первая создает новую секцию с заданным диапазоном. Вторая создает новую секцию с интервалом, заданным при первоначальном разбиении, и добавляет ее в конец списка секций. Также можно присоеднинить существующую таблицу в качестве секции. Например, это может быть таблица с архивными данными, расположенная на другом сервере и подключенная с помощью fdw:

```
CREATE FOREIGN TABLE journal_archive (
    id      INTEGER NOT NULL,
    dt      TIMESTAMP NOT NULL,
    level   INTEGER,
    msg     TEXT
) SERVER archive_server;
```
> Важно: структура подключаемой таблицы должна полностью совпадать с родительской.

Подключим ее к имеющемуся разбиению:
```
SELECT attach_range_partition('journal', 'journal_archive', '2014-01-01'::date, '2015-01-01'::date);
```
Устаревшие секции можно сливать с архивной:
```
SELECT merge_range_partitions('journal_archive', 'journal_1');
```
Разделить ранее созданную секцию на две можно с помощью следующей функции, указав точку деления:
```
SELECT split_range_partition('journal_366', '2016-01-03'::date);
```
Чтобы отсоединить ранее созданную секцию, воспользуйтесь функцией:
```
SELECT detach_range_partition('journal_archive');
```

Пример построения плана для запроса с фильтрацией по ключевому полю:
```
SELECT * FROM journal WHERE dt >= '2015-06-01' AND dt < '2015-06-03';
   id   |         dt          | level |               msg
--------+---------------------+-------+----------------------------------
 217441 | 2015-06-01 00:00:00 |     2 | 15053892d993ce19f580a128f87e3dbf
 217442 | 2015-06-01 00:01:00 |     1 | 3a7c46f18a952d62ce5418ac2056010c
 217443 | 2015-06-01 00:02:00 |     0 | 92c8de8f82faf0b139a3d99f2792311d
 ...
(2880 rows)

EXPLAIN SELECT * FROM journal WHERE dt >= '2015-06-01' AND dt < '2015-06-03';
                            QUERY PLAN
------------------------------------------------------------------
 Append  (cost=0.00..58.80 rows=0 width=0)
   ->  Seq Scan on journal_152  (cost=0.00..29.40 rows=0 width=0)
   ->  Seq Scan on journal_153  (cost=0.00..29.40 rows=0 width=0)
(3 rows)
```

### Деакцивация pg_pathman
Для включения и отключения модуля `pg_pathman` и отдельных его копонентов существует ряд [GUC](https://www.postgresql.org/docs/9.5/static/config-setting.html) переменных:

 - `pg_pathman.enable` --- полная отключение (или включение) модуля `pg_pathman`
 - `pg_pathman.enable_runtimeappend` --- включение/отключение функционала `RuntimeAppend`
 - `pg_pathman.enable_runtimemergeappend` --- включение/отключение функционала `RuntimeMergeAppend`
 - `pg_pathman.enable_partitionfilter` --- включение/отключение функционала `PartitionFilter`

Чтобы **безвозвратно** отключить механизм `pg_pathman` для отдельной таблицы, используйте фунцию `disable_pathman_for()`. В результате этой операции структура таблиц останется прежней, но для планирования и выполнения запросов будет использоваться стандартный механизм PostgreSQL.
```
SELECT disable_pathman_for('range_rel');
```

## Обратная связь
Если у вас есть вопросы или предложения, а также если вы обнаружили ошибки, напишите нам в разделе [issues](https://github.com/postgrespro/pg_pathman/issues).

## Авторы
Ильдар Мусин <i.musin@postgrespro.ru> Postgres Professional, Россия		
Александр Коротков <a.korotkov@postgrespro.ru> Postgres Professional, Россия		
Дмитрий Иванов <d.ivanov@postgrespro.ru> Postgres Professional, Россия		
