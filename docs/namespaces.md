# Namespaces in Dragonfly

Dragonfly added an _experimental_ feature, allowing complete separation of data by different users.
We call this feature _namespaces_, and it allows using a single Dragonfly server with multiple
tenants, each using their own data, without being able to mix them together.

Note that this feature can alternatively be achieved by having each user `SELECT` a different
(numeric) database, or by asking that each user uses a unique prefix for their keys. This approach
has several disadvantages, like users forgetting to `SELECT` / use their prefix, accessing data
logically belonging to other users.

The advantage of using Namespaces is that data is completely isolated, and users cannot accidentally
use data they do not own. A user must authenticate in order to access the namespace it was assigned.
And as a bonus, each namespace can have multiple databases, switched via `SELECT` like any regular
data store.

However, before using this feature, please note that it is experimental. This means that:

* Some features are not supported for non-default namespaces, such as replication and save to RDB
* Some tools are missing, like breakdown of memory / load per namespace
* We do not yet consider this production ready, and it might still have some uncovered bugs

So kindly use it at your own risk.

## Usage

This section describes how, as a Dragonfly user / administrator, you could use namespaces.

A namespace is identified by a unique string id, defined by the user / admin. Each Dragonfly user
is associated with a single namespace. If not set explicitly, then the default namespace is used,
which is the empty string id.

Multiple users can use the same namespace if they are all assigned the same namespace id. This can
allow, for example, creating a read-only user as well as a mutating user over the same data.

To associate user `user1` with the namespace `namespace1`, use the `ACL` command with the
`NAMESPACE:namespace1` flag:

```
ACL SETUSER user1 NAMESPACE:namespace1 ON >user_pass +@all ~*
```

This sets / creates user `user`, using password `user_pass`, using namespace `namespace1`.

For more examples check out `tests/dragonfly/acl_family_test.py` - specifically the
`test_namespaces` function.

## Technical Details

This section describes how we _implemented_ namespaces in Dragonfly. It is meant to be used by those
who wish to contribute pull requests to Dragonfly.

Prior to adding namespaces to Dragonfly, each _shard_ had a single `DbSlice` that it owned. They
were thread-local, global-scope instances.

To support namespaces, we created a `Namespace` class (see `src/server/namespaces.h`) which contains
a `vector<DbSlice>`, with a `DbSlice` per shard. When first used, a `Namespace` calls the engine
shard set to initialize the array of `DbSlice`s.

To access all `Namespace`s, we also added a registry with the original name `Namespaces`. It is a
global, thread safe class that allows accessing all registered namespaces, and registering new ones
on the fly. Note that, while it is thread safe, it shouldn't be a bottle neck because it is supposed
to only be used during the authentication of a connection (or when adding new namespaces).

When a new connection is authenticated with Dragonfly, we look up (and create, if needed) the
namespace it is associated with. We then save a `Namespace* ns` inside the `dfly::ConnectionContext`
class to associate the user with the namespaces. Because we removed the global `DbSlice` objects,
this is now the only way to access namespaces, which protects users from accessing unowned data.

Currently, we do not have any support for removing namespaces, so they hang in memory until the
server exits.
