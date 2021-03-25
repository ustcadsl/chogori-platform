## Drop Collection

### Overview of Steps

- Client issues drop collection request to CPO
- CPO moves the collection metadata to a deleted list. The collection will no longer be returned as part of get collection requests. Its schemas are deleted.
- CPO issues unload partition request to all collection partitions
- A server node receiving an unload partition request will immediately drop the partition, without waiting for ongoing operations and with no persistence needed.
- After receiving responses from all partitions, CPO responds to the client.
- From this point on, a new collection with the same name may be recreated.
- Asynchronously, the CPO cleans the WAL persistence data (not implemented yet)

### Notes

The CPO is the authoratative truth of collection metadata such as the partition map, version, and the 
existence of a collection. The drop collection operations does not need to be persisted to the 
WAL on the individual partitions, it is instead persisted by the CPO as with other CPO metadata. Therefore, 
the partitions can simply be sent partition unload RPCs which will drop the partition in memory (the state 
will be the same as if the partition crashed, or as if the partition was being migrated). Even though it may 
take some time to clean up the in-memory state in the partitions (running destructors, etc.) it should be 
well within acceptable range for a heavy admin request like dropping a collection.

The CPO will use the collection metadata in the deleted list to perform asynchronous clean up of the 
persistence WALs. This is potentially a large amount of work and could be delegated to another thread or 
another component, but it is not time sensitive.

Drop collection is not transactional (e.g. there is no timestamp associated with it, and there is no 
rollback) but there is one consistency issue we want to handle which relates to creating a new collection 
with the same name as a previously deleted collection. Consider the scenario:
- Client 1: txn1.read("key", "mycollection")
- Client 2: delete("mycollection")
- Client 2: create("mycollection")
- Client 1: txn1.write("key2", "mycollection")
- Client 1: txn1.commit()
Here, Client 1 started a transaction before the collection was deleted and recreated, and then tries to 
do another operation and commit. This could create an inconsistent state in the new collection since the 
write could be dependent on the read in the old collection. We cannot use partition versioning here to 
solve the problem because the action on a partition version mismatch is to refresh the map from the CPO, 
but here we need to abort the transaction. The solution is to use a globally unique  collection ID generated 
by the CPO when communicating to the storage nodes. If the ID does not match what the storage node has, the 
request is rejected with an error so the client will abort. The user API will remain the same; client API 
calls will still take the collection name as the argument and the client library will translate that to its 
cached ID which was dispensed by the CPO.