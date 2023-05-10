# Preparation

## Install extensions
   - [Bazel](https://marketplace.visualstudio.com/items?itemName=BazelBuild.vscode-bazel)
   - [CodeLLDB](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb)
   - [C/C++ Extension Pack](https://marketplace.visualstudio.com/items?itemName=ms-vscode.cpptools-extension-pack)

## Build exec query tool
```
bazel build //zetasql/tools/execute_query:execute_query -c dbg --spawn_strategy=local
```

## Build one package
```
bazel build //zetasql/parser/... --features=-supports_dynamic_linker
```

## Run tests for one package
```
bazel test //zetasql/parser/... --features=-supports_dynamic_linker
```

## Debug grammar
Conflicts could appear in finite-state machine created by Bison.
Running bison with the option `--report=THINGS` will provide you with detailed info about generated finite-state machine.
   - Use `--report=THINGS` and `--report-file=FILE` keys in `bazel/bison.bzl` file
   - Update Bison to receive more detailed info. Change version in file `bazel/zetasql_deps_step_2.bzl` (e.g. `bison-3.8.2`)
   - Use `bazel-out/host/bin/bazel/bison/bin/bison --help` command to see detailed list of `THINGS` options

## Build and debug tests

### Build needed package
```
bazel build //zetasql/analyzer:all --features=-supports_dynamic_linker -c dbg --spawn_strategy=local --config=clang
```
or
```
bazel build //zetasql/analyzer:resolver_test --features=-supports_dynamic_linker -c dbg --spawn_strategy=local --config=clang
```
### Add command to `Execute test` configuration in `launch.json`
```
./bazel-bin/zetasql/analyzer/analyzer_aggregation_test --test_file=./zetasql/analyzer/testdata/aggregation.test
```
`analyzer_aggregation_test` = `<package_name>` + `'_'` + `<test_name>`<br>
`<test_name>` = file name from `'zetasql/<package_name>/testdata'` without `'.test'` extension (see `'gen_analyzer_test'` in `'zetasql/analyzer/BUILD'`).

# Support Snowflake syntax

## Data types

### ARRAY in Snowflake and BigQuery
`Snowflake`: An `ARRAY` contains 0 or more pieces of data. Each value in an `ARRAY` is of type `VARIANT`. A `VARIANT` can store a value of any other type, including `OBJECT` and `ARRAY`.

`BigQuery`: An `ARRAY` is an ordered list consisting of zero or more values of the same data type. `ARRAY`s of `ARRAY`s are not allowed.

### INT, INTEGER, BIGINT, SMALLINT, TINYINT, BYTEINT
In Snowflake these types are synonymous with NUMBER, except that precision and scale cannot be specified (i.e. always defaults to NUMBER(38, 0)).<br>
Currently INTEGER types added as synonyms for INT64.

### FLOAT, FLOAT4, FLOAT8
The names FLOAT, FLOAT4, and FLOAT8 are for compatibility with other systems; Snowflake treats all three as 64-bit floating-point numbers.<br>
Analog of these types in BigQuery is DOUBLE.

### DOUBLE, DOUBLE PRECISION, REAL
Synonymous with FLOAT.

## Functions

### Show Snowflake functions signatures
Show all Snowflake functions: `SHOW FUNCTIONS`

Example: `APPROX_TOP_K_ACCUMULATE(ANY, NUMBER) RETURN OBJECT`

**Note:** Some Snowflake aggregate functions return OBJECT data type. To simplify a description of new functions BigQuery types were used.<br> ****Should be fixed after implementation of VARIANT and OBJECT data types.**

## Referencing an alias in the same SELECT statement
1. If a clause contains `GROUP BY` or some aggregation function (e.g. `COUNT(*)`) then referencing aliases in the same `SELECT` will be incorrect.
<br>It should be implemented later.
The next query is valid in Snowflake but fails in zetasql-snowflake:
```sql
select 12.5 as key, key * count(*)
from (
   select * from UNNEST(GENERATE_ARRAY(1, 5))
);
```

2. Name resolution is performed in the next order:
   - Function argument (Only if FEATURE_FUNCTION_ARGUMENT_NAMES_HIDE_LOCAL_NAMES is on)
   - Name target
   - Expression column
   - Function argument
   - Named constant
   - **Select list aliases**

   **Note:** It should be considered during further implementation.
