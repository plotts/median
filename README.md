## Current state
- Currently being tested with integer types as they are all passed by value. 
- Has been tested with 50k rows for integer and bigint; 32k rows for smallint.  (more tests, and test data soon.) 


## What's working: 
Given the data type restrictions, above, these SQL examples "work".   :)
 
Vanilla:
```sql
SELECT id, temp, median(temp) FROM newtable;
```
Or a windowing function like: 
```sql 
SELECT id, temp, median(temp) OVER (ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) FROM newtable;
```

## What's not working. 
- Need to clean up and appropriately format the code.
- Polymorphic types, including the pass-by-reference types.  Depending on platform, this probably includes DOUBLE and NUMBER. 
- Parallelism.  need to learn more about this.  The idea of median seems like we need all the data from all partitions, sorted in order to select the median.  
- Need more tests.