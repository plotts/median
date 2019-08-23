## Current state
- Currently being tested with integer types as they are all passed by value. 
- Has been tested with 50k rows for integer and bigint; 32k rows for smallint.  (more tests, and test data soon.) 


## What's working: 
Given the data type restrictions, above, these SQL examples "work".   :)
 
Vanilla:
```sql
SELECT id, temp, median(temp) FROM newtable;
```
Or a simple windowing function like: 
```sql 
SELECT id, temp, median(temp) OVER (ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) FROM newtable;
```

## What's not working. 
- Need to clean up and appropriately format the code.
- Parallelism.  need to learn more about this.