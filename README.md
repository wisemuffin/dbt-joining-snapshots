# CDC in DBT

Change data capture in BDT can be broken down into a few steps

- snapshots - recording effective from and to dates
- remove partial duplicates

## Snapshots

[How to track data changes with dbt snapshots](https://www.getdbt.com/blog/track-data-changes-with-dbt-snapshots/)

### Types of Data

Mutable: Records are updated in-place over time. A typical example is an orders table,  where the status column changes as the order is processed.
Immutable: Once a record is created, it is never updated again. A typical example is clickstream data, like a page_viewed or link_clicked. Once that event is recorded, it won’t be updated again.

### Why do we need snapshots:

Applications often store data in mutable tables. The engineers that design these applications typically want to read and modify the current state of a row – recording and managing the historical values for every row in a database is extra work that costs brain power and CPU cycles.

### limitations

snapshotting will not replace having a full history table.

Snapshots, by their very nature, are not idempotent. The results of a snapshot operation will vary depending on if you run dbt snapshot once per hour or once per day. Further, there’s no way to go back in time and re-snapshot historical data. Once a source record has been mutated, the previous state of that record is effectively lost forever. By snapshotting your sources, you can maximize the amount of data that you track, and in turn, maximize your modeling optionality.

## how-we-remove-partial-duplicates

[dbt on partial duplicates removing](https://docs.getdbt.com/blog/how-we-remove-partial-duplicates)

using the macro: build_key_from_columns (similar to how we at NBN create a row natural id)

## Joining Snapshots

prequisite = removing partial duplicates

[dbt on joining snapshots](https://docs.getdbt.com/blog/joining-snapshot-complexity?utm_content=210039579&utm_medium=social&utm_source=linkedin&hss_channel=lcp-10893210)

# Example

dbt seed will be our mutable source command.

## First get the initial application state into dbt source tables

./seeds/snapshot_example_seeds/product_important_status.csv
```csv
entity_id,unimportant_value,important_status,updated_at
1,cool,not_available,2021-10-01 10:00:000
2,cool,not_available,2021-11-15 15:30:0000

```

```bash
dbt seed
dbt snapshot
```

## Second update mutable seeds and re run snapshots

./seeds/snapshot_example_seeds/product_important_status.csv
```csv
entity_id,unimportant_value,important_status,updated_at
1,not cool,pending,2021-11-10 08:00:000
2,not cool,not_available,2021-11-16 15:30:0000

```

```bash
dbt seed
dbt snapshot
```

## Third Update

./seeds/snapshot_example_seeds/product_important_status.csv
```csv
entity_id,unimportant_value,important_status,updated_at
1,cool,available,2021-11-15 16:00:0000
2,cool,not_available,2021-11-17 15:30:0000

```

```bash
dbt seed
dbt snapshot
```