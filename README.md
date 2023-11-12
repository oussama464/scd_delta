# scd_delta

### step1:
- design dimension table that includes the following columns:
    * **pk**: unique identifier for each dimension record 
    * **Natural Key (nk)** : a set of attributes that uniquely identify a record 
                        typically buisness related information 
    * **Hash Value**: **a hash of the natural key and other relevant attributes used to quickly detect change**
    * **effective start date (start date)**: The date when the record become active 
    * **effective end data (End Date)**: The date when the record is no longer active 
    * **current flag**: a flag indicating if the record is the current version 

### step2:
- **initial load**: 
    * load your initial dimension data into the dimension table 
    * set the start date to the **inital load** and set the end date a far future date (e.g., 9999–12–31)
    * set the Current Flag to 'Y' for the initial version 
### setp3:
- **Hash Function**: 
    * Create a Hash Function that takes the natural key and any other relevant attributes and produces a hash value 
### step4:
- **change Detection**:
    * When new data arrives calculate the hash value for the new record using the hash function , 
        compare the hash value of the existsing record in the dimension table with the same 
        natural key , if the hash values are the same => no changes are detected 
        if the hash value are diffiren , changes are detected , and you proceed with next steps 
### step5: 
-   **update Current record**:
    * set the end date of the current version of the record in the dimension table to the date before the change.
      set the current flag of this record to 'N' 
### step6:
-   **Insert new Record**:
    *   insert a new version of the current record with the updated attributes , the new hash value , new start date (the date of the change)
        , a fat-current END date , and a CurrentFlag of 'Y' 
### step7:
-    **Historical Queries**:
    * when querying the dimension table , filter the data by the current Flag and the desired date range to get the correct version of the data at that point in time 
-------------------------------------------------------------------------------

## additional notes :
* identify: 
    - records to be inserted (from the incremental updates)
    - record to be updated (in the existsing data)
* if a record with **existing primary** key has got some **fields changed** it will have **to effects**:
    - INSERT that record from incremental update to the main table 
    - Update (expire) the existsing reord of the main table 
* if a record with existing primary keys has got unchanged fields , it will have no effect 
* all new records will have only one effect they will be inserted in the main table 

* construct a merge key field : this additional key will be used in matching the records of incremental updates with exiting data 
* if a record in the incremental update can update the record of existing data ,
    we'll generate two copies of that record 
    - one copy with original merge key will be used to update the existing data 
    - the other copy will have NULL as merge key , hence will not match the existing data and will get inserted 
* the mechanisme of introducing merge key in the intermediate dataframe will keep the original fields unchanged 
* if a record in the incremental update is not going to update the existing data , the merge key will be the same as the original merge key 
* to generate the records participating in the merge:
    - select evryting from the incremental updates and add an additional field mergeKey (hash)
    - join the incremental update with active and existing data on primary key 
        -   if there is a change in the field values of incremental update , select the record with NULL as mergeKey 
    - union the above two dataframes. 
-----
## Further Enhancment 
- take advantage of the partition column in the filter if it doeas not change across records (boost performance)
- in a single batch if there are multiple updates , apply window function on the incremental batch to expire all the records , except for the most recent ones 
- the merge keys can be a collection of multiple keys (like composite keys)
- can keep a list of fields over wich you decide if records have changed , don't consider a user record to be changed if his name updated 
    but consider it to be changed if his address or date of birth has beeen updated 
    