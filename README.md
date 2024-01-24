## Allegro

Allegro: a program to test and analyze performance of Presto. 

### Build

```bash
mvn clean install
```

### Install

After successfully building Allegro, you can find Allegro's package 
at `<allegro_source_dir>/target/allegro-0.1.zip`. Unzip it to
something like `/usr/local/allegro-0.1`, put the following into
your `~/.bashrc` or `~/.zshrc`:

```bash
export ALLEGRO_HOME=/usr/local/allegro-0.1
export PATH=$ALLEGRO_HOME/bin:$PATH
```

Then prepare Allegro's config file at `~/.allegro/config.yaml`:

```yaml
baseline:
  querySet: "allegro:tpch"
  coordinatorIp: "127.0.0.1"
  coordinatorPort: 8080
  dbName: "hive/tpch100g"
  user: prestotest
  password: 123456
  sessionProperties: ""
```

The meaning of these properties are:

| Name              | Description                                                                         |
|-------------------|-------------------------------------------------------------------------------------|
| querySet          | The queries to run, can be `allegro:tpch` or directory path to sql files            |
| coordinatorIp     | Ip of the coordinator                                                               |
| coordinatorPort   | Port of the coordinator                                                             |
| dbName            | Name of the db. Note catalog name should be included, e.g. `hive/tpch100g`          |
| user              | User name to authenticate to Presto server                                          |
| password          | Password to authenticate to Presto server or empty if authentication is not enabled | 
| sessionProperties | Session properties to set before run query(not implemented yet.)                    |

## Usage

### Help

```bash
$ allegro
Missing required subcommand
Usage: Allegro [-h] [COMMAND]
Run & Analyze queries on Presto.
  -h, --help   display this help message
Commands:
  help  Displays help information about the specified command
  view  view the result of a run.
  list  List the runs.
  run   Run queries.
```

### Run queries

```bash
$ allegro run test_1
==== Allegro Conf ====
== test_1 ==
QuerySet         : allegro:tpch
JdbcUrl          : jdbc:presto://xxx/hive/tpch100g?applicationNamePrefix=hello
User             : prestotest
Password         : 1****6
SessionProperties:

Queries: [q1.sql, q2.sql, q3.sql, q4.sql, q5.sql, q6.sql, q7.sql, q8.sql, q9.sql, q10.sql, q11.sql, q12.sql, q13.sql, q14.sql, q15.sql, q16.sql, q17.sql, q18.sql, q19.sql, q20.sql, q21.sql, q22.sql]

Start running query: q1

Query: /*+traceId=test_1_q1*/select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '90' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
```

### List all the results

```bash
$ allegro list
┏━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Run                   ┃ QuerySet     ┃ Schema                ┃ Date                ┃ Elapse Time   ┃
┡━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ vanilla_q1_1          │ /tmp/q1      │ hive/tpch100g         │ 2024-01-21 20:04:11 │ 31s 832ms     │
│ vanilla_q1_2          │ /tmp/q1      │ hive/tpch100g         │ 2024-01-21 20:04:47 │ 27s 649ms     │
│ vanilla_q1_3          │ /tmp/q1      │ hive/tpch100g         │ 2024-01-21 20:05:21 │ 28s 546ms     │
│ native_q1_1           │ /tmp/q1      │ hive/tpch100g         │ 2024-01-21 20:13:06 │ 11s 701ms     │
│ native_q1_2           │ /tmp/q1      │ hive/tpch100g         │ 2024-01-21 20:13:15 │ 4s 256ms      │
│ native_q1_3           │ /tmp/q1      │ hive/tpch100g         │ 2024-01-21 20:13:25 │ 4s 124ms      │
└───────────────────────┴──────────────┴───────────────────────┴─────────────────────┴───────────────┘
```

### View result

```bash
$ allegro view vanilla_q18_1
== vanilla_q18_1 ==
┏━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━┓
┃ Query ┃ Time      ┃ State   ┃
┡━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━┩
│ q18   │ 47s 560ms │ SUCCESS │
│ Total │ 47s 560ms │ SUCCESS │
└───────┴───────────┴─────────┘

┏━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Name ┃ Value     ┃ Percentage ┃ Bar                                                                                                  ┃
┡━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ q18  │ 47s 560ms │ 100.00%    │ ████████████████████████████████████████████████████████████████████████████████████████████████████ │
└──────┴───────────┴────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────┘

== CPU Time ==
┏━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Query ┃ CPU Time      ┃
┡━━━━━━━╇━━━━━━━━━━━━━━━┩
│ q18   │ 19m 54s 600ms │
└───────┴───────────────┘

== Peak Memory ==
┏━━━━━━━┳━━━━━━━━━━━━━┓
┃ Query ┃ Peak Memory ┃
┡━━━━━━━╇━━━━━━━━━━━━━┩
│ q18   │ 13.6 GB     │
└───────┴─────────────┘

== Top Operators(CPU Time) ==
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
┃ Operator             ┃ Time/Percentage      ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
│ LookupJoin           │ 8m 15s 607ms(41.50%) │
│ ScanFilterAndProject │ 5m 1s 180ms(25.22%)  │
│ PartitionedOutput    │ 2m 46s 407ms(13.93%) │
│ Aggregation          │ 2m 34s 51ms(12.90%)  │
│ HashBuilder          │ 41s 827ms(3.50%)     │
│ LocalExchangeSink    │ 26s 607ms(2.23%)     │
│ Exchange             │ 6s 946ms(0.58%)      │
│ LocalExchangeSource  │ 1s 357ms(0.11%)      │
│ TopN                 │ 193ms(0.02%)         │
│ TaskOutput           │ 2ms(0.00%)           │
└──────────────────────┴──────────────────────┘

== Top Operators(Wall Time) ==
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Operator             ┃ Time/Percentage       ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━┩
│ ScanFilterAndProject │ 13m 35s 32ms(33.43%)  │
│ LookupJoin           │ 13m 31s 433ms(33.29%) │
│ PartitionedOutput    │ 5m 40s 290ms(13.96%)  │
│ Aggregation          │ 5m 31s 746ms(13.61%)  │
│ LocalExchangeSink    │ 56s 690ms(2.33%)      │
│ HashBuilder          │ 54s 831ms(2.25%)      │
│ Exchange             │ 23s 674ms(0.97%)      │
│ LocalExchangeSource  │ 3s 448ms(0.14%)       │
│ TopN                 │ 578ms(0.02%)          │
│ TaskOutput           │ 2ms(0.00%)            │
└──────────────────────┴───────────────────────┘
```

### Compare results

```bash
$ allegro view vanilla_q18_1 native_q18_1
== vanilla_q18_1 ==
QuerySet         : /tmp/q18
JdbcUrl          : jdbc:presto://xxxxxxxxx/hive/tpch100g?applicationNamePrefix=hello
User             : prestotest
Password         : <****>
SessionProperties:

== native_q18_1 ==
QuerySet         : /tmp/q18
JdbcUrl          : jdbc:presto://xxxxxxxxx/hive/tpch100g?applicationNamePrefix=hello
User             : prestotest
Password         : <****>
SessionProperties:

== Elapse Time ==
┏━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ Query ┃ vanilla_q18_1 ┃ native_q18_1 ┃ Speedup ┃
┡━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━┩
│ q18   │ 47s 560ms     │ 36s 456ms    │ 1.30x   │
│ Total │ 47s 560ms     │ 36s 456ms    │ 1.30x   │
└───────┴───────────────┴──────────────┴─────────┘

== Cpu Time ==
┏━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ Query ┃ vanilla_q18_1 ┃ native_q18_1 ┃ Speedup ┃
┡━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━┩
│ q18   │ 19m 54s 600ms │ 6m 36s       │ 3.02x   │
│ Total │ 19m 54s 600ms │ 6m 36s       │ 3.02x   │
└───────┴───────────────┴──────────────┴─────────┘

== Peak Memory ==
┏━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ Query ┃ vanilla_q18_1 ┃ native_q18_1 ┃ Speedup ┃
┡━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━┩
│ q18   │ 13.6 GB       │ 14.6 GB      │ 0.93x   │
│ Total │ 13.6 GB       │ 14.6 GB      │ 0.93x   │
└───────┴───────────────┴──────────────┴─────────┘

== Top Operators(CPU Time) ==
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ Operator             ┃ vanilla_q18_1 ┃ native_q18_1 ┃ Speedup ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━┩
│ LookupJoin           │ 8m 15s 607ms  │ 1m 40s 936ms │ 4.91x   │
│ ScanFilterAndProject │ 5m 1s 180ms   │ 1m 17s 688ms │ 3.88x   │
│ PartitionedOutput    │ 2m 46s 407ms  │ 57s 333ms    │ 2.90x   │
│ Aggregation          │ 2m 34s 51ms   │ 1m 41s 735ms │ 1.51x   │
│ HashBuilder          │ 41s 827ms     │ 34s 506ms    │ 1.21x   │
│ LocalExchangeSink    │ 26s 607ms     │ 8s 223ms     │ 3.24x   │
│ Exchange             │ 6s 946ms      │ 14s 148ms    │ 0.49x   │
│ LocalExchangeSource  │ 1s 357ms      │ 1s 277ms     │ 1.06x   │
│ TopN                 │ 193ms         │ 5ms          │ 38.60x  │
│ TaskOutput           │ 2ms           │ 0s           │ 0.00x   │
└──────────────────────┴───────────────┴──────────────┴─────────┘

== Top Operators(Wall Time) ==
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ Operator             ┃ vanilla_q18_1 ┃ native_q18_1  ┃ Speedup ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━┩
│ ScanFilterAndProject │ 13m 35s 32ms  │ 16m 37s 518ms │ 0.82x   │
│ LookupJoin           │ 13m 31s 433ms │ 1m 59s 239ms  │ 6.81x   │
│ PartitionedOutput    │ 5m 40s 290ms  │ 2m 1s 253ms   │ 2.81x   │
│ Aggregation          │ 5m 31s 746ms  │ 2m 42s 917ms  │ 2.04x   │
│ LocalExchangeSink    │ 56s 690ms     │ 15s 924ms     │ 3.56x   │
│ HashBuilder          │ 54s 831ms     │ 51s 1ms       │ 1.08x   │
│ Exchange             │ 23s 674ms     │ 1m 12s 695ms  │ 0.33x   │
│ LocalExchangeSource  │ 3s 448ms      │ 4s 414ms      │ 0.78x   │
│ TopN                 │ 578ms         │ 5ms           │ 115.60x │
│ TaskOutput           │ 2ms           │ 0s            │ 0.00x   │
└──────────────────────┴───────────────┴───────────────┴─────────┘
```
