---
title: Count-distinct Expression
language: en
sidebar_label: Count-distinct Expression
pagination_label: Count-distinct Expression
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - count distinct expression
    - case when
draft: false
last_update:
    date: 09/13/2024
---


In some data analysis scenarios, you maybe encounter the SQL usage of Count Distinct Case When Expression.

In previous versions, if you want to speed up such queries through model pre-calculation, you need to set Case When Expression as a computable column, and then set the Count Distinct Computed Column metric to answer such queries.

Starting from Kylin V5, we have provided special optimizations for this type of query, allowing users to only set the Count (Distinct Column) measure,
the system uses the pre-calculated results and adds some Case When Expression online calculations to fully answer the query, reducing the complexity of model settings and improving user experience.

### How to Use

1. Enable optimization

This function is disable by default, and it can be enabled on the system or project level.

To enable it on the system level, configure the parameters in `$KYLIN_HOME/conf/kylin.properties` . To enable it on project level, add the configuration in **Setting-Advanced Settings-Custom Project Configuration**.

```sql
kylin.query.convert-count-distinct-expression-enabled=true
```

2. Supported Count Distinct Case When Expression syntax

```sql
count(distinct case when {condition} then {column} else null end)
```

Notice:

a. `{condition}` is dimension column expression, for example `cal_dt = '2012-01-01'`.

b. The `{column}` must be set to the `count (distinct column)` measure.

c.When selecting the error option in the function parameter, the return type must be selected: precisely, otherwise the optimization of this syntax cannot be triggered

After the function is enabled, queries that conform to the above grammar can be answered by indexes that include **dimension column** and `count(distinct column)`**measure** in the `condition` expression.

Example:

```sql
count(distinct (case when cal_dt = date'2012-01-01' then price else null end))
```

It can be answered by indexes including `cal_dt` dimension and `count(distinct price)` measure.


### Known Limitation

1. Else can only be with null, constants are not supported temporarily, such as `case when ... then column1 else 1 end`.
Starting from Kylin 5.0 version, after else can be cast(null as `type`), such as `case when ... then column1 else cast(null as double) end`.
It should be noted that `type` should be as close as possible to `column1` The type is the same or the same category,
otherwise it may not conform to the sql syntax and an error will be reported, or this function cannot be applied. 
The major category refers to the same numeric type, date type, Boolean type, etc.

2. Only one pair of `when .. then ..` is supported after case, and multiple pairs are not supported for now, such as `case when .. then column1 when ... then column2 else null end`.
