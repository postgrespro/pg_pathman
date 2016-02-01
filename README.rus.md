# pg_pathman

Модуль `pg_pathman` предоставляет оптимизированный механизм секционирования, а также функции для создания и управления секциями.

## Концепция pg_pathman

Секционирование -- это способ разбиения одной большой таблицы на множество меньших по размеру. Для каждой записи можно однозначно определить секцию, в которой она должна храниться посредством вычисления ключа. Традиционно выделяют три стратегии секционирования:

* HASH - данные равномерно распределяются по секциям в соответствии со значениями hash-функции, вычисленными по некоторому атрибуту;
* RANGE - данные распределяются по секциям, каждая из которых ответственна за заданный диапазон значений аттрибута;
* LIST - для каждой секции определяется набор конкретных значений атрибута.

Секционирование в postgres основано на механизме наследования. Каждому наследнику задается условие CHECK CONSTRAINT. Например:

```
CREATE TABLE test (id SERIAL PRIMARY KEY, title TEXT);
CREATE TABLE test_1 (CHECK ( id >= 100 AND id < 200 )) INHERITS (test);
CREATE TABLE test_2 (CHECK ( id >= 200 AND id < 300 )) INHERITS (test);
```

Несмотря на гибкость, этот механизм обладает недостатками. Так при фильтрации данных оптимизатор вынужден перебирать все дочерние секции и сравнивать условие запроса с CHECK CONSTRAINT-ами секции, чтобы определить из каких секций ему следует загружать данные. При большом количестве секций это создает дополнительные накладные расходы, которые могут свести на нет выигрыш в производительности от применения секционирования.

Модуль `pg_pathman` предоставляет функции для создания и управления
секциями (см. следующий раздел) и механизм секционирования,
оптимизированный с учетом знания о структуре дочерних таблиц. Конфигурация сохраняется таблице `pathman_config`, каждая строка которой содержит запись для одной секционированной таблицы (название таблицы, атрибут и тип разбиения). В процессе инициализации модуля в разделяемую память сохраняется конфигурация дочерних таблиц в удобном для поиска формате. Получив запрос типа `SELECT` к секционированной таблице, `pg_pathman` анализирует дерево условий запроса и выделяет из него условия вида:

```
ПЕРЕМЕННАЯ ОПЕРАТОР КОНСТАНТА
```
где `ПЕРЕМЕННАЯ` -- это атрибут, по которому было выполнено разбиение, `ОПЕРАТОР` -- оператор сравнения (поддерживаются =, <, <=, >, >=), `КОНСТАНТА` -- скалярное значение. Например:

```
WHERE id = 150
```
Затем основываясь на стратегии секционирования и условиях запроса `pg_pathman` выбирает соответствующие секции и строит план.

## Installation

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

## Функции pathman

### Создание секций
```
create_hash_partitions(
    relation TEXT,
    attribute TEXT,
    partitions_count INTEGER)
```
Выполняет HASH-секционирование таблицы `relation` по целочисленному полю `attribute`. Создает `partitions_count` дочерних секций, а также триггер на вставку. Данные из родительской таблицы не копируются автоматически в дочерние. Миграцию данных можно выполнить с помощью функции `partition_data()` (см. ниже), либо вручную.

```
create_range_partitions(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    interval ANYELEMENT,
    premake INTEGER)

create_range_partitions(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    interval INTERVAL,
    premake INTEGER)
```
Выполняет RANGE-секционирование таблицы `relation` по полю `attribute`. Аргумент `start_value` задает начальное значение, `interval` -- диапазон значений внутри одной секции, `premake` -- количество заранее создаваемых секций.

```
create_partitions_from_range(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    end_value ANYELEMENT,
    interval ANYELEMENT)

create_partitions_from_range(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    end_value ANYELEMENT,
    interval INTERVAL)
```
Выполняет RANGE-секционирование для заданного диапазона таблицы `relation` по полю `attribute`.

### Утилиты
```
partition_data(parent text)
```
Копирует данные из родительской таблицы `parent` в дочерние секции.
```
create_hash_update_trigger(parent TEXT)
```
Создает триггер на UPDATE для HASH секций. По-умолчанию триггер на обновление данных не создается, т.к. это создает дополнительные накладные расходы. Триггер полезен только в том случае, когда меняется значение ключевого аттрибута.
```
create_hash_update_trigger(parent TEXT)
```
Аналогично предыдущей, но для RANGE секций.

### Управление секциями
```
split_range_partition(partition TEXT, value ANYELEMENT)
```
Разбивает RANGE секцию `partition` на две секции по значению `value`.
```
merge_range_partitions(partition1 TEXT, partition2 TEXT)
```
Объединяет две смежные RANGE секции. Данные из `partition2` копируются в `partition1`, после чего секция `partition2` удаляется.
```
append_partition(p_relation TEXT)
```
Добавляет новую секцию в конец списка секций. Диапазон значений устанавливается равным последней секции.
```
prepend_partition(p_relation TEXT)
```
Добавляет новую секцию в начало списка секций.
```
disable_partitioning(relation TEXT)
```
Отключает механизм секционирования `pg_pathman` для заданной таблицы и удаляет триггер на вставку. При этом созданные ранее секции остаются без изменений.

## Примеры использования
### HASH
Рассмотрим пример секционирования таблицы, используя HASH-стратегию на примере таблицы.
```
CREATE TABLE hash_rel (
    id      SERIAL PRIMARY KEY,
    value   INTEGER);
INSERT INTO hash_rel (value) SELECT g FROM generate_series(1, 10000) as g;
```
Если дочерние секции подразумевают наличие индексов, то стоит их создать в родительской таблице до разбиения. Тогда при разбиении pg_pathman автоматически создаст соответствующие индексы в дочерних.таблицах. Разобьем таблицу `hash_rel` на 100 секций по полю `value`:
```
SELECT create_hash_partitions('hash_rel', 'value', 100);
```
Перенесем данные из родительской таблицы в дочерние секции.
```
SELECT partition_data('hash_rel');
```
Пример построения плана для запроса с фильтрацией по ключевому полю:
```
SELECT * FROM hash_rel WHERE value = 1234;
  id  | value 
------+-------
 1234 |  1234

EXPLAIN SELECT * FROM hash_rel WHERE value = 1234;
                           QUERY PLAN                            
-----------------------------------------------------------------
 Append  (cost=0.00..2.00 rows=0 width=0)
   ->  Seq Scan on hash_rel_34  (cost=0.00..2.00 rows=0 width=0)
         Filter: (value = 1234)
```
Стоит отметить, что pg_pathman исключает из плана запроса родительскую таблицу, и чтобы получить данные из нее, следует использовать модификатор ONLY:
```
EXPLAIN SELECT * FROM ONLY hash_rel;
                       QUERY PLAN                       
--------------------------------------------------------
 Seq Scan on hash_rel  (cost=0.00..0.00 rows=1 width=8)
```

### RANGE
Пример секционирования таблицы с использованием стратегии RANGE.
```
CREATE TABLE range_rel (
    id SERIAL PRIMARY KEY,
    dt TIMESTAMP);
INSERT INTO range_rel (dt) SELECT g FROM generate_series('2010-01-01'::date, '2014-12-31'::date, '1 day') as g;
```
Разобьем таблицу на 60 секций так, чтобы каждая секция содержала данные за один месяц:
```
SELECT create_range_partitions('range_rel', 'dt', '2010-01-01'::date, '1 month'::interval, 60);
```

Перенесем данные из родительской таблицы в дочерние секции.
```
SELECT partition_data('range_rel');
```
Объединим секции первые две секции:
```
SELECT merge_range_partitions('range_rel_1', 'range_rel_2');
```
Разделим первую секцию на две по дате '2010-02-15':
```
SELECT split_range_partition('range_rel_1', '2010-02-15'::date);
```
Добавим новую секцию в конец списка секций:
```
SELECT append_partition('range_rel');
```
Пример построения плана для запроса с фильтрацией по ключевому полю:
```
SELECT * FROM range_rel WHERE dt >= '2012-04-30' AND dt <= '2012-05-01';
 id  |         dt          
-----+---------------------
 851 | 2012-04-30 00:00:00
 852 | 2012-05-01 00:00:00

EXPLAIN SELECT * FROM range_rel WHERE dt >= '2012-04-30' AND dt <= '2012-05-01';
                                 QUERY PLAN                                 
----------------------------------------------------------------------------
 Append  (cost=0.00..60.80 rows=0 width=0)
   ->  Seq Scan on range_rel_28  (cost=0.00..30.40 rows=0 width=0)
         Filter: (dt >= '2012-04-30 00:00:00'::timestamp without time zone)
   ->  Seq Scan on range_rel_29  (cost=0.00..30.40 rows=0 width=0)
         Filter: (dt <= '2012-05-01 00:00:00'::timestamp without time zone)
```

### Деакцивация pathman
Деактивировать pathman для некоторой ранее разделенной таблицы можно следующей командой disable_partitioning():
```
SELECT disable_partitioning('range_rel');
```
Все созданные секции и данные останутся по прежнему доступны и будут обрабатываться стандартным планировщиком PostgreSQL.
### Ручное управление секциями
Когда набора функций pg_pathman недостаточно для управления секциями, предусмотрено ручное управление. Можно создавать или удалять дочерние таблицы вручную, но после этого необходимо вызывать функцию:
```
on_update_partitions(oid),
```
которая обновит внутреннее представление структуры секций в памяти pg_pathman. Например, добавим новую секцию к ранее созданной range_rel:
```
CREATE TABLE range_rel_archive (CHECK (dt >= '2000-01-01' AND dt < '2010-01-01')) INHERITS (range_rel);
SELECT on_update_partitions('range_rel'::regclass::oid);
```
CHECK CONSTRAINT должен иметь строго определенный формат:
* (VARIABLE >= CONST AND VARIABLE < CONST) для RANGE секционированных таблиц;
* (VARIABLE % CONST = CONST) для HASH секционированных таблиц.

Также можно добавить секцию, расположенную на удаленном сервере:
```
CREATE FOREIGN TABLE range_rel_archive (
    id INTEGER NOT NULL,
    dt TIMESTAMP)
SERVER archive_server;
ALTER TABLE range_rel_archive INHERIT range_rel;
ALTER TABLE range_rel_archive ADD CHECK (dt >= '2000-01-01' AND dt < '2010-01-01');
SELECT on_update_partitions('range_rel'::regclass::oid);
```
Структура таблицы должна полностью совпадать с родительской.

В случае, если родительская таблица была удалена вручную с использованием инструкции DROP TABLE, необходимо удалить соответствующую строку из таблицы pathman_config и вызывать on_remove_partitions():
```
SELECT on_remove_partitions('range_rel'::regclass::oid);
DROP TABLE range_rel CASCADE;
DELETE FROM pathman_config WHERE relname = 'public.range_rel';
```