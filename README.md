Redis Memory Cache
------------------

Local memory caching proxy for redis client.

This module can give a performance boost if large or many objects need to be cached on a remote Redis cache. Instead of
sending them over the network every time, it is cached locally so it can be returned from memory.

If multiple servers and/or workers all have their local caches, some mechanism is needed to mark cached keys as 'dirty'.
This is also implemented by this module, by publishing dirty keys to a Redis channel. Other workers will clear their
local cache automatically.

The user of this module must specify which paths are to be enabled for memcaching. Any operation on keys on non-enabled
paths will simply be passed to redis directly. This design decision was made because memcaching may not always be
possible for all paths. Sometimes you will want to include or exclude a specific key or group of keys. The following
cases may cause problems:
- If other systems (or persons) changes Redis keys without the use of this cache wrapper.
- If other operations than the supported GET, SET, MGET, MSET, DEL need to be used. More operations may be supported in
  future.
- If keys have an expiry date (EXPIRE command).

If some or more of the conditions above are true, memcaching may lead to incorrect results. For paths that this is the
case, disable memcaching. By default, all paths are disabled.

Notice that only key-value keys are cached. Commands for lists, sets and other structures are not cached and handled via
the 'normal' Redis client.

Usage:

1. Create a memcache-enabled client from a redis client:

```javascript
var RedisMemoryCacheClient = require('redis-memory-cache');
var rmcClient = new RedisMemoryCacheClient(redisClient, redisSubClient);
```


Notice that the 'redisSubClient' should be a separate connection which listens specifically to channel messages.

Both clients should be redis clients from the redis module: https://www.npmjs.com/package/redis.

The returned client is actually a proxy to the original client. All RedisClient methods are automatically proxied to the
original object. Only GET, MGET, SET, MSET and DEL are overridden to provide cache capabilities.

2. Specify the paths that have memcaching enabled:

```javascript
rmcClient.setCachePath(['key1','key2'], true);
rmcClient.setCachePath(['key1','key2','key3'], false);
rmcClient.setCachePath(['other-key'], true);
```

The above causes all keys with the pattern 'key1:key2' or 'key1:key2:*' to be memcached, with the exception of
'key1:key2:key3' and 'key1:key2:key3:*'. Also 'other-key' and 'other-key:*' is memcached, but nothing else because the
default is to not use memcaching on any path. To enable global memcache:

```javascript
rmcClient.setCachePath([], true);
```

3. Use your redis client as you normally would:

```javascript
rmcClient.set('key1:key2:something', 'val');
rmcClient.set('key1:key2:something2', 'val', function(err, res) {console.log(err)});
rmcClient.get('key1:key2:something', function(err, res) {console.log(res);});
rmcClient.mset('key1', 'val1', 'key2', 'val2');
rmcClient.mget(['key1', 'key2'], function(err, res) {console.log(res);});
rmcClient.del('key1:key2:something');
rmcClient.incr('noncached');
```

The results should be exactly the same as without memcaching, as long as the following rules are followed:
- for mutating a memcached (key-value, as lists and sets are not cached) key, only the following operations can be used:
  SET, MSET, DEL
- in theory, the following options could easily be supported but this is currently not the case. Support may be added
  when necessary. Don't use them for now because this will cause caching problems:
  MSETNX, SETNX, RENAME, RENAMENX, APPEND, BITOP, SETBIT, INCR, INCRBY
- other operations that do not mutate key-value pairs can also be used, but some that certainly may not be used:
  EXPIRE, EXPIREAT, MOVE, MIGRATE, RENAME, RENAMENX, APPEND, BITOP, INCR, INCRBY
- only the following fetch operations respect the local cache
  GET, MGET
- MULTI/EXEC blocks shouldn't be used on memcached sets at all because it will neither use nor update the cache.

4. Make use of json cache:

```javascript
rmcClient.set('key1:key2:something', JSON.stringify({test: 1}));
rmcClient.get('key1:key2:something', function(err, res) {console.log(JSON.stringify(res));}, true);
```

When adding true after the callback argument in GET or MGET operations, the string is automatically parsed to a json
object.

Subsequent calls do not have to parse the cached result again and return the json object from memory. By default the
object is cloned so that it can be changed without affecting the cache. If you want to prevent this and improve
performance, add another true parameter to prevent cloning. Take care that this json object is then returned by
reference, so it should be used in a read-only way! Example:

```javascript
rmcClient.set('key1:key2:something', JSON.stringify({test: 1}));
console.log()
rmcClient.get('key1:key2:something', function(err, res) {
	console.log(res.test); //1
	res.test = 2;
	console.log(res.test); //2

	rmcClient.get('key1:key2:something', function(err, res) {
		console.log(res.test); //2
	}, true, true);
}, true, true);
```

