

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

# Aggregate functions

ZetaSQL supports the following general aggregate functions.
To learn about the syntax for aggregate function calls, see
[Aggregate function calls][agg-function-calls].

### Function list

<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Summary</th>
    </tr>
  </thead>
  <tbody>

<tr>
  <td><a href="#any_value"><code>ANY_VALUE</code></a>

</td>
  <td>
    Returns an expression for some row.
  </td>
</tr>

<tr>
  <td><a href="#array_agg"><code>ARRAY_AGG</code></a>

</td>
  <td>
    Returns an array of values.
  </td>
</tr>

<tr>
  <td><a href="#array_concat_agg"><code>ARRAY_CONCAT_AGG</code></a>

</td>
  <td>
    Concatenates arrays and returns a single array as a result.
  </td>
</tr>

<tr>
  <td><a href="#avg"><code>AVG</code></a>

</td>
  <td>
    Returns the average of non-<code>NULL</code> values.
  </td>
</tr>

<tr>
  <td><a href="#bit_and"><code>BIT_AND</code></a>

</td>
  <td>
    Performs a bitwise AND operation on an expression.
  </td>
</tr>

<tr>
  <td><a href="#bit_or"><code>BIT_OR</code></a>

</td>
  <td>
    Performs a bitwise OR operation on an expression.
  </td>
</tr>

<tr>
  <td><a href="#bit_xor"><code>BIT_XOR</code></a>

</td>
  <td>
    Performs a bitwise XOR operation on an expression.
  </td>
</tr>

<tr>
  <td><a href="#count"><code>COUNT</code></a>

</td>
  <td>
    Returns the number of rows in the input, or the number of rows with an
    expression evaluated to any value other than <code>NULL</code>.
  </td>
</tr>

<tr>
  <td><a href="#countif"><code>COUNTIF</code></a>

</td>
  <td>
    Returns the count of <code>TRUE</code> values for an expression.
  </td>
</tr>

<tr>
  <td><a href="#logical_and"><code>LOGICAL_AND</code></a>

</td>
  <td>
    Returns the logical AND of all non-<code>NULL</code> expressions.
  </td>
</tr>

<tr>
  <td><a href="#logical_or"><code>LOGICAL_OR</code></a>

</td>
  <td>
    Returns the logical OR of all non-<code>NULL</code> expressions.
  </td>
</tr>

<tr>
  <td><a href="#max"><code>MAX</code></a>

</td>
  <td>
    Returns the maximum non-<code>NULL</code> value.
  </td>
</tr>

<tr>
  <td><a href="#min"><code>MIN</code></a>

</td>
  <td>
    Returns the minimum non-<code>NULL</code> value.
  </td>
</tr>

<tr>
  <td><a href="#string_agg"><code>STRING_AGG</code></a>

</td>
  <td>
    Returns a <code>STRING</code> or <code>BYTES</code> value obtained by
    concatenating non-<code>NULL</code> values.
  </td>
</tr>

<tr>
  <td><a href="#sum"><code>SUM</code></a>

</td>
  <td>
    Returns the sum of non-<code>NULL</code> values.
  </td>
</tr>

  </tbody>
</table>

### `ANY_VALUE`

```sql
ANY_VALUE(
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns `expression` for some row chosen from the group. Which row is chosen is
nondeterministic, not random. Returns `NULL` when the input produces no
rows. Returns `NULL` when `expression` is `NULL` for all rows in the group.

`ANY_VALUE` behaves as if `RESPECT NULLS` is specified;
rows for which `expression` is `NULL` are considered and may be selected.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

Any

**Returned Data Types**

Matches the input data type.

**Examples**

```sql
SELECT ANY_VALUE(fruit) as any_value
FROM UNNEST(["apple", "banana", "pear"]) as fruit;

/*-----------*
 | any_value |
 +-----------+
 | apple     |
 *-----------*/
```

```sql
SELECT
  fruit,
  ANY_VALUE(fruit) OVER (ORDER BY LENGTH(fruit) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS any_value
FROM UNNEST(["apple", "banana", "pear"]) as fruit;

/*--------+-----------*
 | fruit  | any_value |
 +--------+-----------+
 | pear   | pear      |
 | apple  | pear      |
 | banana | apple     |
 *--------+-----------*/
```

```sql
WITH
  Store AS (
    SELECT 20 AS sold, "apples" AS fruit
    UNION ALL
    SELECT 30 AS sold, "pears" AS fruit
    UNION ALL
    SELECT 30 AS sold, "bananas" AS fruit
    UNION ALL
    SELECT 10 AS sold, "oranges" AS fruit
  )
SELECT ANY_VALUE(fruit HAVING MAX sold) AS a_highest_selling_fruit FROM Store;

/*-------------------------*
 | a_highest_selling_fruit |
 +-------------------------+
 | pears                   |
 *-------------------------*/
```

```sql
WITH
  Store AS (
    SELECT 20 AS sold, "apples" AS fruit
    UNION ALL
    SELECT 30 AS sold, "pears" AS fruit
    UNION ALL
    SELECT 30 AS sold, "bananas" AS fruit
    UNION ALL
    SELECT 10 AS sold, "oranges" AS fruit
  )
SELECT ANY_VALUE(fruit HAVING MIN sold) AS a_lowest_selling_fruit FROM Store;

/*-------------------------*
 | a_lowest_selling_fruit  |
 +-------------------------+
 | oranges                 |
 *-------------------------*/
```

### `ARRAY_AGG`

```sql
ARRAY_AGG(
  [ DISTINCT ]
  expression
  [ { IGNORE | RESPECT } NULLS ]
  [ HAVING { MAX | MIN } expression2 ]
  [ ORDER BY key [ { ASC | DESC } ] [, ... ] ]
  [ LIMIT n ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns an ARRAY of `expression` values.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

All data types except ARRAY.

**Returned Data Types**

ARRAY

If there are zero input rows, this function returns `NULL`.

**Examples**

```sql
SELECT ARRAY_AGG(x) AS array_agg FROM UNNEST([2, 1,-2, 3, -2, 1, 2]) AS x;

/*-------------------------*
 | array_agg               |
 +-------------------------+
 | [2, 1, -2, 3, -2, 1, 2] |
 *-------------------------*/
```

```sql
SELECT ARRAY_AGG(DISTINCT x) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

/*---------------*
 | array_agg     |
 +---------------+
 | [2, 1, -2, 3] |
 *---------------*/
```

```sql
SELECT ARRAY_AGG(x IGNORE NULLS) AS array_agg
FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x;

/*-------------------*
 | array_agg         |
 +-------------------+
 | [1, -2, 3, -2, 1] |
 *-------------------*/
```

```sql
SELECT ARRAY_AGG(x ORDER BY ABS(x)) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

/*-------------------------*
 | array_agg               |
 +-------------------------+
 | [1, 1, 2, -2, -2, 2, 3] |
 *-------------------------*/
```

```sql
SELECT ARRAY_AGG(x LIMIT 5) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

/*-------------------*
 | array_agg         |
 +-------------------+
 | [2, 1, -2, 3, -2] |
 *-------------------*/
```

```sql
WITH vals AS
  (
    SELECT 1 x UNION ALL
    SELECT -2 x UNION ALL
    SELECT 3 x UNION ALL
    SELECT -2 x UNION ALL
    SELECT 1 x
  )
SELECT ARRAY_AGG(DISTINCT x ORDER BY x) as array_agg
FROM vals;

/*------------*
 | array_agg  |
 +------------+
 | [-2, 1, 3] |
 *------------*/
```

```sql
WITH vals AS
  (
    SELECT 1 x, 'a' y UNION ALL
    SELECT 1 x, 'b' y UNION ALL
    SELECT 2 x, 'a' y UNION ALL
    SELECT 2 x, 'c' y
  )
SELECT x, ARRAY_AGG(y) as array_agg
FROM vals
GROUP BY x;

/*---------------*
 | x | array_agg |
 +---------------+
 | 1 | [a, b]    |
 | 2 | [a, c]    |
 *---------------*/
```

```sql
SELECT
  x,
  ARRAY_AGG(x) OVER (ORDER BY ABS(x)) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

/*----+-------------------------*
 | x  | array_agg               |
 +----+-------------------------+
 | 1  | [1, 1]                  |
 | 1  | [1, 1]                  |
 | 2  | [1, 1, 2, -2, -2, 2]    |
 | -2 | [1, 1, 2, -2, -2, 2]    |
 | -2 | [1, 1, 2, -2, -2, 2]    |
 | 2  | [1, 1, 2, -2, -2, 2]    |
 | 3  | [1, 1, 2, -2, -2, 2, 3] |
 *----+-------------------------*/
```

### `ARRAY_CONCAT_AGG`

```sql
ARRAY_CONCAT_AGG(
  expression
  [ HAVING { MAX | MIN } expression2 ]
  [ ORDER BY key [ { ASC | DESC } ] [, ... ] ]
  [ LIMIT n ]
)
```

**Description**

Concatenates elements from `expression` of type `ARRAY`, returning a single
array as a result.

This function ignores `NULL` input arrays, but respects the `NULL` elements in
non-`NULL` input arrays. Returns `NULL` if there are zero input rows or
`expression` evaluates to `NULL` for all rows.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

`ARRAY`

**Returned Data Types**

`ARRAY`

**Examples**

```sql
SELECT ARRAY_CONCAT_AGG(x) AS array_concat_agg FROM (
  SELECT [NULL, 1, 2, 3, 4] AS x
  UNION ALL SELECT NULL
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

/*-----------------------------------*
 | array_concat_agg                  |
 +-----------------------------------+
 | [NULL, 1, 2, 3, 4, 5, 6, 7, 8, 9] |
 *-----------------------------------*/
```

```sql
SELECT ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x)) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

/*-----------------------------------*
 | array_concat_agg                  |
 +-----------------------------------+
 | [5, 6, 7, 8, 9, 1, 2, 3, 4]       |
 *-----------------------------------*/
```

```sql
SELECT ARRAY_CONCAT_AGG(x LIMIT 2) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

/*--------------------------*
 | array_concat_agg         |
 +--------------------------+
 | [1, 2, 3, 4, 5, 6]       |
 *--------------------------*/
```

```sql
SELECT ARRAY_CONCAT_AGG(x ORDER BY ARRAY_LENGTH(x) LIMIT 2) AS array_concat_agg FROM (
  SELECT [1, 2, 3, 4] AS x
  UNION ALL SELECT [5, 6]
  UNION ALL SELECT [7, 8, 9]
);

/*------------------*
 | array_concat_agg |
 +------------------+
 | [5, 6, 7, 8, 9]  |
 *------------------*/
```

### `AVG`

```sql
AVG(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the average of non-`NULL` values in an aggregated group.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

`AVG` can be used with differential privacy. For more information, see
[Differentially private aggregate functions][dp-functions].

Caveats:

+ If the aggregated group is empty or the argument is `NULL` for all rows in
  the group, returns `NULL`.
+ If the argument is `NaN` for any row in the group, returns `NaN`.
+ If the argument is `[+|-]Infinity` for any row in the group, returns either
  `[+|-]Infinity` or `NaN`.
+ If there is numeric overflow, produces an error.
+ If a [floating-point type][floating-point-types] is returned, the result is
  [non-deterministic][non-deterministic], which means you might receive a
  different result each time you use this function.

[floating-point-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_types

[non-deterministic]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating-point-semantics

**Supported Argument Types**

+ Any numeric input type
+ `INTERVAL`

**Returned Data Types**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th><th><code>INTERVAL</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>INTERVAL</code></td></tr>
</tbody>

</table>

**Examples**

```sql
SELECT AVG(x) as avg
FROM UNNEST([0, 2, 4, 4, 5]) as x;

/*-----*
 | avg |
 +-----+
 | 3   |
 *-----*/
```

```sql
SELECT AVG(DISTINCT x) AS avg
FROM UNNEST([0, 2, 4, 4, 5]) AS x;

/*------*
 | avg  |
 +------+
 | 2.75 |
 *------*/
```

```sql
SELECT
  x,
  AVG(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg
FROM UNNEST([0, 2, NULL, 4, 4, 5]) AS x;

/*------+------*
 | x    | avg  |
 +------+------+
 | NULL | NULL |
 | 0    | 0    |
 | 2    | 1    |
 | 4    | 3    |
 | 4    | 4    |
 | 5    | 4.5  |
 *------+------*/
```

[dp-functions]: https://github.com/google/zetasql/blob/master/docs/aggregate-dp-functions.md

### `BIT_AND`

```sql
BIT_AND(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Performs a bitwise AND operation on `expression` and returns the result.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_AND(x) as bit_and FROM UNNEST([0xF001, 0x00A1]) as x;

/*---------*
 | bit_and |
 +---------+
 | 1       |
 *---------*/
```

### `BIT_OR`

```sql
BIT_OR(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Performs a bitwise OR operation on `expression` and returns the result.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_OR(x) as bit_or FROM UNNEST([0xF001, 0x00A1]) as x;

/*--------*
 | bit_or |
 +--------+
 | 61601  |
 *--------*/
```

### `BIT_XOR`

```sql
BIT_XOR(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
```

**Description**

Performs a bitwise XOR operation on `expression` and returns the result.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

+ UINT32
+ UINT64
+ INT32
+ INT64

**Returned Data Types**

INT64

**Examples**

```sql
SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([5678, 1234]) AS x;

/*---------*
 | bit_xor |
 +---------+
 | 4860    |
 *---------*/
```

```sql
SELECT BIT_XOR(x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x;

/*---------*
 | bit_xor |
 +---------+
 | 5678    |
 *---------*/
```

```sql
SELECT BIT_XOR(DISTINCT x) AS bit_xor FROM UNNEST([1234, 5678, 1234]) AS x;

/*---------*
 | bit_xor |
 +---------+
 | 4860    |
 *---------*/
```

### `COUNT`

1.

```sql
COUNT(*)
[OVER over_clause]
```

2.

```sql
COUNT(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

1. Returns the number of rows in the input.
2. Returns the number of rows with `expression` evaluated to any value other
   than `NULL`.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

This function with DISTINCT supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

`COUNT` can be used with differential privacy. For more information, see
[Differentially private aggregate functions][dp-functions].

**Supported Argument Types**

`expression` can be any data type. If
`DISTINCT` is present, `expression` can only be a data type that is
[groupable][agg-data-type-properties].

**Return Data Types**

INT64

**Examples**

You can use the `COUNT` function to return the number of rows in a table or the
number of distinct values of an expression. For example:

```sql
SELECT
  COUNT(*) AS count_star,
  COUNT(DISTINCT x) AS count_dist_x
FROM UNNEST([1, 4, 4, 5]) AS x;

/*------------+--------------*
 | count_star | count_dist_x |
 +------------+--------------+
 | 4          | 3            |
 *------------+--------------*/
```

```sql
SELECT
  x,
  COUNT(*) OVER (PARTITION BY MOD(x, 3)) AS count_star,
  COUNT(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) AS count_dist_x
FROM UNNEST([1, 4, 4, 5]) AS x;

/*------+------------+--------------*
 | x    | count_star | count_dist_x |
 +------+------------+--------------+
 | 1    | 3          | 2            |
 | 4    | 3          | 2            |
 | 4    | 3          | 2            |
 | 5    | 1          | 1            |
 *------+------------+--------------*/
```

```sql
SELECT
  x,
  COUNT(*) OVER (PARTITION BY MOD(x, 3)) AS count_star,
  COUNT(x) OVER (PARTITION BY MOD(x, 3)) AS count_x
FROM UNNEST([1, 4, NULL, 4, 5]) AS x;

/*------+------------+---------*
 | x    | count_star | count_x |
 +------+------------+---------+
 | NULL | 1          | 0       |
 | 1    | 3          | 3       |
 | 4    | 3          | 3       |
 | 4    | 3          | 3       |
 | 5    | 1          | 1       |
 *------+------------+---------*/
```

If you want to count the number of distinct values of an expression for which a
certain condition is satisfied, this is one recipe that you can use:

```sql
COUNT(DISTINCT IF(condition, expression, NULL))
```

Here, `IF` will return the value of `expression` if `condition` is `TRUE`, or
`NULL` otherwise. The surrounding `COUNT(DISTINCT ...)` will ignore the `NULL`
values, so it will count only the distinct values of `expression` for which
`condition` is `TRUE`.

For example, to count the number of distinct positive values of `x`:

```sql
SELECT COUNT(DISTINCT IF(x > 0, x, NULL)) AS distinct_positive
FROM UNNEST([1, -2, 4, 1, -5, 4, 1, 3, -6, 1]) AS x;

/*-------------------*
 | distinct_positive |
 +-------------------+
 | 3                 |
 *-------------------*/
```

Or to count the number of distinct dates on which a certain kind of event
occurred:

```sql
WITH Events AS (
  SELECT DATE '2021-01-01' AS event_date, 'SUCCESS' AS event_type
  UNION ALL
  SELECT DATE '2021-01-02' AS event_date, 'SUCCESS' AS event_type
  UNION ALL
  SELECT DATE '2021-01-02' AS event_date, 'FAILURE' AS event_type
  UNION ALL
  SELECT DATE '2021-01-03' AS event_date, 'SUCCESS' AS event_type
  UNION ALL
  SELECT DATE '2021-01-04' AS event_date, 'FAILURE' AS event_type
  UNION ALL
  SELECT DATE '2021-01-04' AS event_date, 'FAILURE' AS event_type
)
SELECT
  COUNT(DISTINCT IF(event_type = 'FAILURE', event_date, NULL))
    AS distinct_dates_with_failures
FROM Events;

/*------------------------------*
 | distinct_dates_with_failures |
 +------------------------------+
 | 2                            |
 *------------------------------*/
```

[agg-data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

[dp-functions]: https://github.com/google/zetasql/blob/master/docs/aggregate-dp-functions.md

### `COUNTIF`

```sql
COUNTIF(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the count of `TRUE` values for `expression`. Returns `0` if there are
zero input rows, or if `expression` evaluates to `FALSE` or `NULL` for all rows.

Since `expression` must be a `BOOL`, the form
`COUNTIF(DISTINCT ...)` is generally not useful: there is only one distinct
value of `TRUE`. So `COUNTIF(DISTINCT ...)` will return 1 if `expression`
evaluates to `TRUE` for one or more input rows, or 0 otherwise.
Usually when someone wants to combine `COUNTIF` and `DISTINCT`, they
want to count the number of distinct values of an expression for which a certain
condition is satisfied. One recipe to achieve this is the following:

```sql
COUNT(DISTINCT IF(condition, expression, NULL))
```

Note that this uses `COUNT`, not `COUNTIF`; the `IF` part has been moved inside.
To learn more, see the examples for [`COUNT`](#count).

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

BOOL

**Return Data Types**

INT64

**Examples**

```sql
SELECT COUNTIF(x<0) AS num_negative, COUNTIF(x>0) AS num_positive
FROM UNNEST([5, -2, 3, 6, -10, -7, 4, 0]) AS x;

/*--------------+--------------*
 | num_negative | num_positive |
 +--------------+--------------+
 | 3            | 4            |
 *--------------+--------------*/
```

```sql
SELECT
  x,
  COUNTIF(x<0) OVER (ORDER BY ABS(x) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS num_negative
FROM UNNEST([5, -2, 3, 6, -10, NULL, -7, 4, 0]) AS x;

/*------+--------------*
 | x    | num_negative |
 +------+--------------+
 | NULL | 0            |
 | 0    | 1            |
 | -2   | 1            |
 | 3    | 1            |
 | 4    | 0            |
 | 5    | 0            |
 | 6    | 1            |
 | -7   | 2            |
 | -10  | 2            |
 *------+--------------*/
```

### `LOGICAL_AND`

```sql
LOGICAL_AND(
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the logical AND of all non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

`BOOL`

**Return Data Types**

`BOOL`

**Examples**

`LOGICAL_AND` returns `FALSE` because not all of the values in the array are
less than 3.

```sql
SELECT LOGICAL_AND(x < 3) AS logical_and FROM UNNEST([1, 2, 4]) AS x;

/*-------------*
 | logical_and |
 +-------------+
 | FALSE       |
 *-------------*/
```

### `LOGICAL_OR`

```sql
LOGICAL_OR(
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the logical OR of all non-`NULL` expressions. Returns `NULL` if there
are zero input rows or `expression` evaluates to `NULL` for all rows.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

`BOOL`

**Return Data Types**

`BOOL`

**Examples**

`LOGICAL_OR` returns `TRUE` because at least one of the values in the array is
less than 3.

```sql
SELECT LOGICAL_OR(x < 3) AS logical_or FROM UNNEST([1, 2, 4]) AS x;

/*------------*
 | logical_or |
 +------------+
 | TRUE       |
 *------------*/
```

### `MAX`

```sql
MAX(
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the maximum non-`NULL` value in an aggregated group.

Caveats:

+ If the aggregated group is empty or the argument is `NULL` for all rows in
  the group, returns `NULL`.
+ If the argument is `NaN` for any row in the group, returns `NaN`.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Supported Argument Types**

Any [orderable data type][agg-data-type-properties] except for `ARRAY`.

**Return Data Types**

The data type of the input values.

**Examples**

```sql
SELECT MAX(x) AS max
FROM UNNEST([8, 37, 55, 4]) AS x;

/*-----*
 | max |
 +-----+
 | 55  |
 *-----*/
```

```sql
SELECT x, MAX(x) OVER (PARTITION BY MOD(x, 2)) AS max
FROM UNNEST([8, NULL, 37, 55, NULL, 4]) AS x;

/*------+------*
 | x    | max  |
 +------+------+
 | NULL | NULL |
 | NULL | NULL |
 | 8    | 8    |
 | 4    | 8    |
 | 37   | 55   |
 | 55   | 55   |
 *------+------*/
```

[agg-data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

### `MIN`

```sql
MIN(
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the minimum non-`NULL` value in an aggregated group.

Caveats:

+ If the aggregated group is empty or the argument is `NULL` for all rows in
  the group, returns `NULL`.
+ If the argument is `NaN` for any row in the group, returns `NaN`.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

This function supports specifying [collation][collation].

[collation]: https://github.com/google/zetasql/blob/master/docs/collation-concepts.md#collate_about

**Supported Argument Types**

Any [orderable data type][agg-data-type-properties] except for `ARRAY`.

**Return Data Types**

The data type of the input values.

**Examples**

```sql
SELECT MIN(x) AS min
FROM UNNEST([8, 37, 4, 55]) AS x;

/*-----*
 | min |
 +-----+
 | 4   |
 *-----*/
```

```sql
SELECT x, MIN(x) OVER (PARTITION BY MOD(x, 2)) AS min
FROM UNNEST([8, NULL, 37, 4, NULL, 55]) AS x;

/*------+------*
 | x    | min  |
 +------+------+
 | NULL | NULL |
 | NULL | NULL |
 | 8    | 4    |
 | 4    | 4    |
 | 37   | 37   |
 | 55   | 37   |
 *------+------*/
```

[agg-data-type-properties]: https://github.com/google/zetasql/blob/master/docs/data-types.md#data_type_properties

### `STRING_AGG`

```sql
STRING_AGG(
  [ DISTINCT ]
  expression [, delimiter]
  [ HAVING { MAX | MIN } expression2 ]
  [ ORDER BY key [ { ASC | DESC } ] [, ... ] ]
  [ LIMIT n ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns a value (either `STRING` or `BYTES`) obtained by concatenating
non-`NULL` values. Returns `NULL` if there are zero input rows or `expression`
evaluates to `NULL` for all rows.

If a `delimiter` is specified, concatenated values are separated by that
delimiter; otherwise, a comma is used as a delimiter.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

**Supported Argument Types**

Either `STRING` or `BYTES`.

**Return Data Types**

Either `STRING` or `BYTES`.

**Examples**

```sql
SELECT STRING_AGG(fruit) AS string_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;

/*------------------------*
 | string_agg             |
 +------------------------+
 | apple,pear,banana,pear |
 *------------------------*/
```

```sql
SELECT STRING_AGG(fruit, " & ") AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

/*------------------------------*
 | string_agg                   |
 +------------------------------+
 | apple & pear & banana & pear |
 *------------------------------*/
```

```sql
SELECT STRING_AGG(DISTINCT fruit, " & ") AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

/*-----------------------*
 | string_agg            |
 +-----------------------+
 | apple & pear & banana |
 *-----------------------*/
```

```sql
SELECT STRING_AGG(fruit, " & " ORDER BY LENGTH(fruit)) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

/*------------------------------*
 | string_agg                   |
 +------------------------------+
 | pear & pear & apple & banana |
 *------------------------------*/
```

```sql
SELECT STRING_AGG(fruit, " & " LIMIT 2) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

/*--------------*
 | string_agg   |
 +--------------+
 | apple & pear |
 *--------------*/
```

```sql
SELECT STRING_AGG(DISTINCT fruit, " & " ORDER BY fruit DESC LIMIT 2) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;

/*---------------*
 | string_agg    |
 +---------------+
 | pear & banana |
 *---------------*/
```

```sql
SELECT
  fruit,
  STRING_AGG(fruit, " & ") OVER (ORDER BY LENGTH(fruit)) AS string_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;

/*--------+------------------------------*
 | fruit  | string_agg                   |
 +--------+------------------------------+
 | NULL   | NULL                         |
 | pear   | pear & pear                  |
 | pear   | pear & pear                  |
 | apple  | pear & pear & apple          |
 | banana | pear & pear & apple & banana |
 *--------+------------------------------*/
```

### `SUM`

```sql
SUM(
  [ DISTINCT ]
  expression
  [ HAVING { MAX | MIN } expression2 ]
)
[ OVER over_clause ]

over_clause:
  { named_window | ( [ window_specification ] ) }

window_specification:
  [ named_window ]
  [ PARTITION BY partition_expression [, ...] ]
  [ ORDER BY expression [ { ASC | DESC }  ] [, ...] ]
  [ window_frame_clause ]

```

**Description**

Returns the sum of non-`NULL` values in an aggregated group.

To learn more about the optional aggregate clauses that you can pass
into this function, see
[Aggregate function calls][aggregate-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[aggregate-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

<!-- mdlint on -->

To learn more about the `OVER` clause and how to use it, see
[Window function calls][window-function-calls].

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[window-function-calls]: https://github.com/google/zetasql/blob/master/docs/window-function-calls.md

<!-- mdlint on -->

`SUM` can be used with differential privacy. For more information, see
[Differentially private aggregate functions][dp-functions].

Caveats:

+ If the aggregated group is empty or the argument is `NULL` for all rows in
  the group, returns `NULL`.
+ If the argument is `NaN` for any row in the group, returns `NaN`.
+ If the argument is `[+|-]Infinity` for any row in the group, returns either
  `[+|-]Infinity` or `NaN`.
+ If there is numeric overflow, produces an error.
+ If a [floating-point type][floating-point-types] is returned, the result is
  [non-deterministic][non-deterministic], which means you might receive a
  different result each time you use this function.

[floating-point-types]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating_point_types

[non-deterministic]: https://github.com/google/zetasql/blob/master/docs/data-types.md#floating-point-semantics

**Supported Argument Types**

+ Any supported numeric data type
+ `INTERVAL`

**Return Data Types**

<table>

<thead>
<tr>
<th>INPUT</th><th><code>INT32</code></th><th><code>INT64</code></th><th><code>UINT32</code></th><th><code>UINT64</code></th><th><code>NUMERIC</code></th><th><code>BIGNUMERIC</code></th><th><code>FLOAT</code></th><th><code>DOUBLE</code></th><th><code>INTERVAL</code></th>
</tr>
</thead>
<tbody>
<tr><th>OUTPUT</th><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>INT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>UINT64</code></td><td style="vertical-align:middle"><code>NUMERIC</code></td><td style="vertical-align:middle"><code>BIGNUMERIC</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>DOUBLE</code></td><td style="vertical-align:middle"><code>INTERVAL</code></td></tr>
</tbody>

</table>

**Examples**

```sql
SELECT SUM(x) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

/*-----*
 | sum |
 +-----+
 | 25  |
 *-----*/
```

```sql
SELECT SUM(DISTINCT x) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

/*-----*
 | sum |
 +-----+
 | 15  |
 *-----*/
```

```sql
SELECT
  x,
  SUM(x) OVER (PARTITION BY MOD(x, 3)) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

/*---+-----*
 | x | sum |
 +---+-----+
 | 3 | 6   |
 | 3 | 6   |
 | 1 | 10  |
 | 4 | 10  |
 | 4 | 10  |
 | 1 | 10  |
 | 2 | 9   |
 | 5 | 9   |
 | 2 | 9   |
 *---+-----*/
```

```sql
SELECT
  x,
  SUM(DISTINCT x) OVER (PARTITION BY MOD(x, 3)) AS sum
FROM UNNEST([1, 2, 3, 4, 5, 4, 3, 2, 1]) AS x;

/*---+-----*
 | x | sum |
 +---+-----+
 | 3 | 3   |
 | 3 | 3   |
 | 1 | 5   |
 | 4 | 5   |
 | 4 | 5   |
 | 1 | 5   |
 | 2 | 7   |
 | 5 | 7   |
 | 2 | 7   |
 *---+-----*/
```

```sql
SELECT SUM(x) AS sum
FROM UNNEST([]) AS x;

/*------*
 | sum  |
 +------+
 | NULL |
 *------*/
```

[dp-functions]: https://github.com/google/zetasql/blob/master/docs/aggregate-dp-functions.md

[agg-function-calls]: https://github.com/google/zetasql/blob/master/docs/aggregate-function-calls.md

