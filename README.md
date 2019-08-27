## Current state
- Has been tested with 50k rows for integer and bigint; 32k rows for smallint.
- Integer 2/4/8, float and double work as"regular" medians and in moving aggregate mode
- Timestamps  work, also in moving aggregate mode. 
- VARCHAR and TEXT works for regular medians, not moving aggregate mode.
- Some additional tests are in place - median2.sql 

Given the data type restrictions, above, these SQL examples "work".   :)
 
Vanilla:
```sql
SELECT id, temp, median(temp) FROM newtable;
```
Or a simple windowing function like: 
```sql 
SELECT id, temp, median(temp) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM newtable;
```

## What's left...
- Parallelism.  need to learn more about this.
- Moving aggregates for VARCHAR/TEXT types - this seems like a memory allocation/scoping issue.
- Has not been tested with huge data sets. 
- Has not been performance tested.