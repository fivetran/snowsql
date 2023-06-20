

# Function calls

When you call a function, specific rules may apply. You can also add the
`SAFE.` prefix, which prevents functions from generating some types of errors.
To learn more, see the next sections.

## Function call rules

The following rules apply to all built-in ZetaSQL functions unless
explicitly indicated otherwise in the function description:

+ If an operand is `NULL`, the function result is `NULL`.
+ For functions that are time zone sensitive, the default time zone,
  which is implementation defined, is used when a time zone is not specified.

## Lambdas 
<a id="lambdas"></a>

**Syntax:**

```sql
(arg[, ...]) -> body_expression
```

```sql
arg -> body_expression
```

**Description**

For some functions, ZetaSQL supports lambdas as builtin function
arguments. A lambda takes a list of arguments and an expression as the lambda
body.

+   `arg`:
    +   Name of the lambda argument is defined by the user.
    +   No type is specified for the lambda argument. The type is inferred from
        the context.
+   `body_expression`:
    +   The lambda body can be any valid scalar expression.

## SAFE. prefix

**Syntax:**

```
SAFE.function_name()
```

**Description**

If you begin a function with
the `SAFE.` prefix, it will return `NULL` instead of an error.
The `SAFE.` prefix only prevents errors from the prefixed function
itself: it doesn't prevent errors that occur while evaluating argument
expressions. The `SAFE.` prefix only prevents errors that occur because of the
value of the function inputs, such as "value out of range" errors; other
errors, such as internal or system errors, may still occur. If the function
doesn't return an error, `SAFE.` has no effect on the output.

**Exclusions**

+ [Operators][link-to-operators], such as `+` and `=`, don't support the
  `SAFE.` prefix. To prevent errors from a
   division operation, use [SAFE_DIVIDE][link-to-SAFE_DIVIDE].
+ Some operators, such as `IN`, `ARRAY`, and `UNNEST`, resemble functions but
  don't support the `SAFE.` prefix.
+ The `CAST` and `EXTRACT` functions don't support the `SAFE.`
  prefix. To prevent errors from casting, use
  [TRY_CAST][link-to-TRY_CAST].

**Example**

In the following example, the first use of the `SUBSTR` function would normally
return an error, because the function doesn't support length arguments with
negative values. However, the `SAFE.` prefix causes the function to return
`NULL` instead. The second use of the `SUBSTR` function provides the expected
output: the `SAFE.` prefix has no effect.

```sql
SELECT SAFE.SUBSTR('foo', 0, -2) AS safe_output UNION ALL
SELECT SAFE.SUBSTR('bar', 0, 2) AS safe_output;

+-------------+
| safe_output |
+-------------+
| NULL        |
| ba          |
+-------------+
```

<!-- mdlint off(WHITESPACE_LINE_LENGTH) -->

[lambdas]: #lambdas

[link-to-operators]: https://github.com/google/zetasql/blob/master/docs/operators.md

[link-to-SAFE_DIVIDE]: https://github.com/google/zetasql/blob/master/docs/mathematical_functions.md#safe_divide

[link-to-TRY_CAST]: https://github.com/google/zetasql/blob/master/docs/conversion_functions.md#try_casting

<!-- mdlint on -->

