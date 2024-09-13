---
title: SQL Query
language: en
sidebar_label: SQL Query
pagination_label: SQL Query
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - sql query
draft: false
last_update:
    date: 09/13/2024
---

Kylin offers an Intelligent OLAP Platform designed to simplify multidimensional analytics on big data. By leveraging precomputation and index building jobs, Kylin achieves sub-second query latency on massive datasets.

Unlike traditional query execution, Kylin utilizes precalculated data to answer queries, eliminating the need for real-time calculations and significantly improving query performance.

This chapter will delve into the details of Kylin query-related topics, including:
* Language Specifications
* Web UI
    * Query Insight
    * Query History
* Execution Principles
    * Pre-calculation
    * Query Transformations
    * Query answered by Model
    * Query answered by InternalTable
    * Query answered by Snapshot
    * Query answered by Push-down Engine
* Query Optimization
* Asynchronous Query
