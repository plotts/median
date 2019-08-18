## Current state
- Currently works with integer types as they are all passed by value). 
- Has been tested with 50k rows, integer and bigint.  (more test data soon.) 


## What's working: 
These SQL examples "work".   :) 
```sql
SELECT id, temp, median(temp) FROM newtable;
```
or a windowing function like: 
```sql 
SELECT id, temp, median(temp) OVER (ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) FROM newtable;
```

## What's not working. 
- Polymorphic types, including the pass-by-reference types.  Depending on platform, this might include DOUBLE, possibly NUMBER. 
- Parallelism.  need to learn more about this.  The idea of median seems like we need all the data from all partitions, sorted in order to select the median.  
- Need more tests.