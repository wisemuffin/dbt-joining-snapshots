# CDC in DBT

Change data capture in BDT can be broken down into a few steps

- snapshots - recording effective from and to dates
- remove partial duplicates

# Partitial Duplicates

These special dupes are not the basic ones that have same exact values in every column and duplicate primary keys that can be easily fixed by haphazardly throwing in a distinct.

- You may be capturing historical, type-two slowly changing dimensional data
- incrementally building a table with an append-only strategy, because you actually want to capture some change over time for the entity your recording.
- your loader may just be appending data indiscriminately on a schedule without much care for your time and sanity.

[dbt on partial duplicates removing](https://docs.getdbt.com/blog/how-we-remove-partial-duplicates)

## grain id column

using the macro: `build_key_from_columns` (similar to how we at NBN create a row natural id) to create a unique key for the grain of the table you want to capture.

- **What is the primary key of the entity which you are tacking historical data for?** You should be able to group by this id in order to identify the duplicates for each id.
- **What other column(s) are capturing the changes in the data you want to track in your new model?** These columns will become part of your new unique primary key.
- **What timestamp provides the most reliable update time for these records?** You’ll need this to ensure you’re picking the most recent row among the partial duplicates.
- **What column value(s) are changing, but you don’t care about tracking in your new model?** These are the columns that you’ll ignore when building your grain_id.

The combination of `product_id` + [changing column(s)] you want to capture becomes the grain of your model. In our example, we are looking to capture data at the grain of `entity_important_status` which we'll call our `entity_grain` for now.

## how to remove partial duplicates

After we have the grain id defined simply then remove any duplicates by filtering on just the first occurance.


# Snapshots

[How to track data changes with dbt snapshots](https://www.getdbt.com/blog/track-data-changes-with-dbt-snapshots/)

## Types of Data

**Mutable:** Records are updated in-place over time. A typical example is an orders table,  where the status column changes as the order is processed.

**Immutable:** Once a record is created, it is never updated again. A typical example is clickstream data, like a page_viewed or link_clicked. Once that event is recorded, it won’t be updated again.

## Why do we need snapshots:

Applications often store data in mutable tables. The engineers that design these applications typically want to read and modify the current state of a row – recording and managing the historical values for every row in a database is extra work that costs brain power and CPU cycles.

## limitations

**snapshotting will not replace having a full history table.**

Snapshots, by their very nature, are not idempotent. The results of a snapshot operation will vary depending on if you run dbt snapshot once per hour or once per day. Further, there’s no way to go back in time and re-snapshot historical data. Once a source record has been mutated, the previous state of that record is effectively lost forever. By snapshotting your sources, you can maximize the amount of data that you track, and in turn, maximize your modeling optionality.

# Joining Snapshots

prequisite = removing partial duplicates

[dbt on joining snapshots](https://docs.getdbt.com/blog/joining-snapshot-complexity?utm_content=210039579&utm_medium=social&utm_source=linkedin&hss_channel=lcp-10893210)

Ultimately, our goal is to capture the history for the `product_id` and join the rows that are valid at the same time. As a result, we can get a view of our data at a given point in time that accurately represents the valid state of any given date.

For historical_table_1 and historical_table_2, we will join on `product_id` where historical_table_1.valid_from to historical_table_1.valid_to has overlapping time with historical_table_2.valid_from to historical_table_2.valid_to.

## NBNs joining of snapshots / historic records

TODO

# Examples

## Remove Partial Duplicates

dbt seed will be our mutable source command.

### First get the initial application state into dbt source tables

./seeds/snapshot_example_seeds/product_important_status.csv
```csv
product_id,unimportant_value,important_status,updated_at
1,cool,not_available,2021-10-01 10:00:00
2,cool,not_available,2021-11-15 15:30:00

```

```bash
dbt seed
dbt snapshot
```

### Second update mutable seeds and re run snapshots

./seeds/snapshot_example_seeds/product_important_status.csv
```csv
product_id,unimportant_value,important_status,updated_at
1,not cool,pending,2021-11-10 08:00:00
2,not cool,not_available,2021-11-16 15:30:00

```

```bash
dbt seed
dbt snapshot
```

### Third Update

./seeds/snapshot_example_seeds/product_important_status.csv
```csv
product_id,unimportant_value,important_status,updated_at
1,cool,available,2021-11-15 16:00:00
2,cool,not_available,2021-11-17 15:30:00

```

```bash
dbt seed
dbt snapshot
```

## Second Example

```csv
product_id,order_id,product_order_id,order_status,updated_at
1,A,1A,pending,2021-10-31 12:00:00
1,B,1B,pending,2021-11-10 10:00:00
2,C,2C,available,2021-11-10 15:00:00
```

```bash
dbt seed
dbt snapshot
```


```csv
product_id,order_id,product_order_id,order_status,updated_at
1,A,1A,available,2021-11-15 16:00:00
1,B,1B,available,2021-11-15 15:30:00
```

```bash
dbt seed
dbt snapshot
```

# TODO

## build_key_from_columns -- update with star

to get dbt_utils.star working in this macro [source](https://docs.getdbt.com/blog/how-we-remove-partial-duplicates)
```jinja
{% macro build_key_from_columns(table_name, exclude=[]) %}

{% set cols = {{ dbt_utils.star(from=ref('table_name'), except = exclude) }} %}
 
{%- for col in cols -%}

    {%- do col_list.append("coalesce(cast(" ~ col.column ~ " as " ~ dbt_utils.type_string() ~ "), '')")  -%}

{%- endfor -%}

{{ return(dbt_utils.surrogate_key(col_list)) }}

{% endmacro %}
```
##

valid_to uses a high_date instead of the default null that comes from dbt_valid_to. This is enable joining snapshots together based on dates.

```bash
coalesce(dbt_valid_to, cast('{{ var("high_date") }}' as timestamp)) as valid_to
```