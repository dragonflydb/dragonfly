## Seeder library

Please use the testing frameworks factories to obtain proper seeder instances!

### 1. Filling data

The seeder tries to maintain a specific number of keys, quickly filling or emptying the instance to reach the target. Once reached, it will issue also modification commands, trying to maintain an equilibrium with mixed load

```python
# Configure how many keys we want
s = Seeder(key_target=10_000)

# Fill instance with keys until it's 10k +- 1%
# Will create many new keys with data and reach equilibrium
await s.run(client, target_deviation=0.01)
assert abs(client.dbsize() - 10_000) <= 100

# Run 5k operations, balanced mix of create/delete/modify
await s.run(client, target_ops=5000)

# Now we want only 500 keys, issue many deletes
s.change_key_target(500)
await s.run(client, target_deviation=0.01)
```

### 2. Checking consistency

Use `Seeder.capture()` to calculate a "state hashes" based on all the data inside an instance. Equal data produces equal hashes (equal hashes don't guarantee equal data but what are the odds...).

```python
# Fill master with 10k (+- 1%) keys
s = Seeder(key_target=10_000)
await seeder.run(master, target_deviation=0.01)

# "Replicate" or other operations
replicate(master, replica)

# Ensure master and replica have same state hashes
master_hashes, replica_hashes = asyncio.gather(
    Seeder.capture(master), # note it's a static method
    Seeder.capture(replica)
)
assert master_hashes == replica_hashes
```

### 3. Working with load

A seeders `run(client)` can be called without any target. It can only be stopped with

```python
# Fill instance with keys
s = Seeder()
await seeder.run(client, target_deviation=0.01)

# Start seeder without target
# Because the instance reached its key target, the seeder
# will issue a balanced mix of modifications/additions/deletions
seeding_task = asyncio.create_task(s.run(client))

# Do operations under fuzzy load
save(client)

await s.stop(client) # request stop, no immediate effect
await seeding_task # wait for actual stop and cleanup
```
