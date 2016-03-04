# pg_pathman

Модуль `pg_pathman` предоставляет оптимизированный механизм секционирования, а также функции для создания и управления секциями.

## Концепция pg_pathman

Секционирование -- это способ разбиения одной большой таблицы на множество меньших по размеру. Для каждой записи можно однозначно определить секцию, в которой она должна храниться посредством вычисления ключа.
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

* RANGE - разбивает таблицу на секции по диапазонам ключевого аттрибута; для оптимизации построения плана используется метод бинарного поиска.
* HASH - данные равномерно распределяются по секциям в соответствии со значениями hash-функции, вычисленными по заданному целочисленному атрибуту.

## Roadmap

 * Выбор секций на этапе выполнения запроса (полезно для nested loop join, prepared statements);
 * Оптимизация выдачи упорядоченных результатов из секционированных таблиц (полезно для merge join, order by);
 * Оптимизация hash join для случая, когда обе таблицы секционированы по ключу join’а;
 * LIST-секционирование;
 * HASH-секционирование по ключевому атрибуту с типом, отличным от INTEGER.

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

## Функции pg_pathman

### Создание секций
```
create_hash_partitions(
    relation TEXT,
    attribute TEXT,
    partitions_count INTEGER)
```
Выполняет HASH-секционирование таблицы `relation` по целочисленному полю `attribute`. Создает `partitions_count` дочерних секций, а также триггер на вставку. Данные из родительской таблицы будут автоматически скопированы в дочерние.

```
create_range_partitions(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    interval ANYELEMENT,
    premake INTEGER DEFAULT NULL)

create_range_partitions(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    interval INTERVAL,
    premake INTEGER DEFAULT NULL)
```
Выполняет RANGE-секционирование таблицы `relation` по полю `attribute`. Аргумент `start_value` задает начальное значение, `interval` -- диапазон значений внутри одной секции, `premake` -- количество заранее создаваемых секций (если не задано, то pathman попытается определить количество секций на основе значений аттрибута). Данные из родительской таблицы будут автоматически скопированы в дочерние.

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
Выполняет RANGE-секционирование для заданного диапазона таблицы `relation` по полю `attribute`. Данные также будут скопированы в дочерние секции.

### Утилиты
```
create_hash_update_trigger(parent TEXT)
```
Создает триггер на UPDATE для HASH секций. По-умолчанию триггер на обновление данных не создается, т.к. это создает дополнительные накладные расходы. Триггер полезен только в том случае, когда меняется значение ключевого аттрибута.
```
create_range_update_trigger(parent TEXT)
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
append_range_partition(p_relation TEXT)
```
Добавляет новую RANGE секцию в конец списка секций.
```
prepend_range_partition(p_relation TEXT)
```
Добавляет новую RANGE секцию в начало списка секций.

```
add_range_partition(
    relation TEXT,
    start_value ANYELEMENT,
    end_value ANYELEMENT)
```
Добавляет новую RANGE секцию с заданным диапазоном к секционированной таблице `relation`.

```
drop_range_partition(partition TEXT)
```
Удаляет RANGE секцию вместе с содержащимися в ней данными.

```
attach_range_partition(
    relation TEXT,
    partition TEXT,
    start_value ANYELEMENT,
    end_value ANYELEMENT)
```
Присоединяет существующую таблицу `partition` в качестве секции к ранее секционированной таблице `relation`. Структура присоединяемой таблицы должна в точности повторять структуру родительской.

```
detach_range_partition(partition TEXT)
```
Отсоединяет секцию `partition`, после чего она становится независимой таблицей.

```
disable_partitioning(relation TEXT)
```
Отключает механизм секционирования `pg_pathman` для заданной таблицы и удаляет триггер на вставку. При этом созданные ранее секции остаются без изменений.

## Примеры использования
### HASH
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

### RANGE
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
Деактивировать механизм pg_pathman для некоторой ранее разделенной таблицы можно следующей командой disable_partitioning():
```
SELECT disable_partitioning('journal');
```
Все созданные секции и данные останутся по прежнему доступны и будут обрабатываться стандартным планировщиком PostgreSQL.

## Авторы

Ильдар Мусин <i.musin@postgrespro.ru> Postgres Professional, Россия

Александр Коротков <a.korotkov@postgrespro.ru> Postgres Professional, Россия
