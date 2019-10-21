var _ = require('lodash');
var crypto = require('crypto');

/**
 * Proxy that keeps redis values in a local memory cache.
 *
 * See README.md
 *
 * @param redisClient
 *   Redis client for writing and reading from the Redis database.
 * @param redisSubClient
 *   Redis client used for subscribing to the 'dirty key' channel. This should be a subscription-only client (a new
 *   client not used for writing and reading).
 * @param options
 * @param cb
 *   Initialization callback.
 * @constructor
 * @note
 *   We are using the RedisClient constructor name so that it appears to be a proper Redis client.
 */
function RedisMemcacheClient(redisClient, redisSubClient, options, cb) {

    var self = this;

    /**
     * Specifies which keys are allowed.
     * @type {{a: boolean, c: {}, p: {object}}}
     */
    var enabledTree = {a: false, c: {}, p: null};

    /**
     * The cache, a hashmap containing all of the cached key-values.
     * @type {Object}
     */
    var cache = {};

    /**
     * JSON-parsed cache objects. These are also cached to speed things up.
     * @type {Object}
     */
    var jsonCache = {};

    /**
     * Keys which are currently being downloaded
     * @type {Array}
     */
    var commandBeingDownloaded = {};

    /**
     * Cache statistics.
     * @type {{hits: number, misses: number, recvDirty: number}}
     */
    var stats = {hits: 0, misses: 0, recvDirty: 0};

    /**
     * Enables caching on the specified path.
     * @param {String[]} path
     * @param {boolean} enable
     */
    this.setCachePath = function(path, enable) {
        var treeItem = getEnabledTreeItem(path, true);
        treeItem.a = enable;
    };

    /**
     * Resets the cache paths to 'disable all'.
     */
    this.resetCachePaths = function() {
        enabledTree = {a: false, c: {}, p: null};
    };

    /**
     * Returns true if the cache is enabled for the specified path.
     * @param {String[]} path
     * @returns {Boolean}
     */
    this.pathIsEnabled = function(path) {
        var treeItem = getEnabledTreeItem(path, false);
        while (!treeItem.hasOwnProperty('a')) {
            treeItem = treeItem.p;
        }
        return treeItem.a;
    };

    /**
     * Returns the item in the enabled tree for the specified path.
     * Branches are automatically created if they do not yet exist.
     * @param path
     * @param create
     *   If true, the tree is automatically created up to the specified item.
     *   Otherwise, the deepest parent item is returned.
     */
    var getEnabledTreeItem = function(path, create) {
        return traverseTree(enabledTree, path, create);
    };

    /**
     * Returns the item in the enabled tree for the specified path.
     * @param item
     * @param remainingPath
     * @param create
     *   If true, the tree is automatically created up to the specified item.
     *   Otherwise, the deepest parent item is returned.
     * @return {Object}
     */
    var traverseTree = function(item, remainingPath, create) {
        if (!remainingPath.length) {
            // Done traversing.
            return item;
        }

        var p = remainingPath.shift();
        if (!item.c.hasOwnProperty(p)) {
            if (!create) {
                // Return the deepest known parent.
                return item;
            }

            // Create new item.
            item.c[p] = {c: {}, p: item};
        }
        return traverseTree(item.c[p], remainingPath, create);
    };

    /**
     * Returns parsed json object.
     * @param str
     */
    var getJson = function(str) {
        try {
            return JSON.parse(str);
        } catch(e) {
            return null;
        }
    };

    /**
     * Returns the value from cache.
     * @param key
     * @param json
     *   JSON-parsed?
     * @param jsonByRef
     *   Return by ref or clone?
     */
    var getCache = function(key, json, jsonByRef) {
        if (json) {
            if (!jsonCache.hasOwnProperty(key)) {
                jsonCache[key] = getJson(cache[key]);
            }
            return (jsonByRef ? jsonCache[key] : _.cloneDeep(jsonCache[key]));
        }
        return cache[key];
    };

    /**
     * Sets the value in cache.
     * @param key
     * @param value
     */
    var setCache = function(key, value) {
        cache[key] = value;
        delete jsonCache[key];
    };

    /**
     * Deletes the value from cache.
     * @param key
     */
    var delCache = function(key) {
        delete cache[key];
        delete jsonCache[key];
    };

    /**
     * Invalidates the cached value for the specified key.
     * @param key
     */
    this.setDirty = function(key) {
        delCache(key);
    };

    /**
     * Returns if the specified key can be fetched from cache (exists and is not dirty).
     * @param key
     */
    this.isCached = function(key) {
        return (key in cache);
    };

    /**
     * Flag to indicate that this redis client is capable of converting to json.
     * @type {boolean}
     */
    this.jsonSupport = true;

    /**
     * Wraps the callback (last argument) in the arguments.
     * @param {String[]} args
     * @param func
     * @param [funcCtx]
     */
    var wrapArgsCb = function(args, func, funcCtx) {
        var cb = args.length ? args[args.length - 1] : null;
        if (!_.isFunction(cb)) {
            cb = null;
        }

        var newCb = function(err) {
            var rv = func.apply(funcCtx || self, arguments);
            if (rv !== false) {
                // Post-redis-function does not handle cb.
                if (cb) {
                    cb.apply(redisClient, arguments);
                }
            }
        };

        // Set or add new callback.
        if (cb) {
            args[args.length - 1] = newCb;
        } else {
            args.push(newCb);
        }
    };

    this.getRedisClient = function() {
        return redisClient;
    }

    /**
     * Redis GET method.
     * @param key
     * @param [cb]
     * @param {boolean} [json]
     *   If true, returns as parsed json.
     * @param {boolean} [jsonByRef]
     *   If true, json object is not cloned before sending.
     *   This improves performance, but make sure you don't change the object because it will affect the cached version!
     */
    this.get = function(key, cb, json, jsonByRef) {
        var args = Array.prototype.slice.apply(arguments);
        if (args.length > 2) {
            // Remove json argument for further argument handling.
            args = args.slice(0, 2);
        }

        if (this.pathIsEnabled(key.split(':'))) {
            // Check if in cache.
            if (key in cache) {
                // Return from cache.
                stats.hits++;
                if (cb) {
                    cb(null, getCache(key, json, jsonByRef));
                }
                return;
            } else {
                wrapArgsCb(args, function(err, res) {
                    if (!err) {
                        // Save to cache.
                        stats.misses++;
                        setCache(key, res);
                        if (cb) {
                            cb(null, getCache(key, json, jsonByRef));
                        }
                        return false;
                    }
                });
            }
        } else {
            if (json) {
                wrapArgsCb(args, function(err, res) {
                    if (err) {
                        return cb(err);
                    }
                    if (cb) {
                        cb(null, getJson(res));
                    }
                    return false;
                });
            }
        }

        // Just use Redis get.
        return redisClient.get.apply(redisClient, args);
    };

    /**
     * Redis MGET method.
     * @param keys
     * @param [cb]
     * @param [json]
     *   If true, returns as parsed json.
     * @param {boolean} [jsonByRef]
     *   If true, json object is not cloned before sending.
     *   This improves performance, but make sure you don't change the object because it will affect the cached version!
     */
    this.mget = function(keys, cb, json, jsonByRef) {
        var values = {};
        var fromRedis = [];
        _.each(keys, function(key) {
            if (key in cache) {
                // Get from cache.
                stats.hits++;
                values[key] = getCache(key, json, jsonByRef);
            } else {
                // Do this to maintain the correct ordering.
                values[key] = null;
                fromRedis.push(key);
            }
        });

        if (fromRedis.length) {
            var hash = crypto.createHash('md5').update(Buffer.from(keys)).digest("hex");
            if (Array.isArray(commandBeingDownloaded[hash])) {
                //if the command is already being downloaded register a callback
                commandBeingDownloaded[hash].push(this.mget.bind(this,keys, cb, json, jsonByRef));
            } else {
                commandBeingDownloaded[hash] = [];
                redisClient.mget(fromRedis, function (err, vals) {
                    if (err) {
                        if (cb) {
                            cb(err);
                        }
                        return;
                    }

                    for (var i in fromRedis) {
                        if (self.pathIsEnabled(fromRedis[i].split(':'))) {
                            // Save in cache.
                            stats.misses++;
                            setCache(fromRedis[i], vals[i]);
                            values[fromRedis[i]] = getCache(fromRedis[i], json, jsonByRef);
                        } else {
                            values[fromRedis[i]] = json ? getJson(vals[i]) : vals[i];
                        }
                    }

                    if (cb) {
                        var vals = [];
                        _.each(keys, function (key) {
                            vals.push(values[key]);
                        });
                        cb(null, vals);
                    }
                    for (var i in commandBeingDownloaded[hash])
                        commandBeingDownloaded[hash][i]();
                    delete commandBeingDownloaded[hash];
                });

            }
        } else {
            if (cb) {
                var vals = [];
                _.each(keys, function(key) {
                    vals.push(values[key]);
                });
                cb(null, vals);
            }
        }
    };

    /**
     * Redis SET method.
     */
    this.set = function(key, val) {
        var args = Array.prototype.slice.apply(arguments);

        if (this.pathIsEnabled(key.split(':'))) {
            // Override callback, clearing the cache and signalling of dirty key.
            wrapArgsCb(args, function(err) {
                if (!err) {
                    if (!self.isCached(key) || (getCache(key, false) !== val)) {
                        setCache(key, val);
                        publishDirtyKey(key);
                    }
                }
            });
        }

        // Just use Redis set.
        return redisClient.set.apply(redisClient, args);
    };

    /**
     * Redis MSET method.
     */
    this.mset = function() {
        var args = Array.prototype.slice.apply(arguments);

        var keys = {};
        var hasKeys = false;
        var i = 0;
        _.each(arguments, function(key) {
            if (i % 2 == 0) {
                if (_.isString(key) && self.pathIsEnabled(key.split(':'))) {
                    keys[key] = args[i + 1];
                    hasKeys = true;
                }
            }
            i++;
        });

        if (hasKeys) {
            // Override callback, clearing the cache and signalling of dirty key.
            wrapArgsCb(args, function(err) {
                if (!err) {
                    _.each(keys, function(val, key) {
                        if (!self.isCached(key) || (getCache(key, false) !== val)) {
                            setCache(key, val);
                            publishDirtyKey(key);
                        }
                    });
                }
            });
        }

        // Just use Redis set.
        return redisClient.mset.apply(redisClient, args);
    };

    /**
     * Redis DEL method.
     */
    this.del = function() {
        var args = Array.prototype.slice.apply(arguments);

        var keys = [];
        var delKeys = _.isArray(args[0]) ? args[0] : [args[0]];
        _.each(delKeys, function(key) {
            if (_.isString(key) && self.pathIsEnabled(key.split(':'))) {
                keys.push(key);
            }
        });

        if (keys.length) {
            // Override callback, clearing the cache and signalling of dirty key.
            wrapArgsCb(args, function(err) {
                if (!err) {
                    _.each(keys, function(key) {
                        setCache(key, null);
                        publishDirtyKey(key);
                    });
                }
            });
        }
        // Just use Redis set.
        return redisClient.del.apply(redisClient, args);
    };

    /**
     * Returns cache statistics.
     * @returns {{hits: number, misses: number, recvDirty: number}}
     */
    this.getCacheStats = function() {
        return stats;
    };

    /**
     * Clears the cache statistics.
     */
    this.resetCacheStats = function() {
        stats.hits = 0;
        stats.misses = 0;
        stats.recvDirty = 0;
    };

    /**
     * Enables listening to the dirty cache channel.
     * @param cb
     */
    var enableDirtyCacheChannel = function(cb) {
        // Start listening for dirty keys.
        redisSubClient.subscribe(RedisMemcacheClient.DIRTY_KEY_CHANNEL, function(err) {
            if (err) {
                console.error('[redis subscribe]', err);
                return cb(err);
            }

            cb();
        });

        redisSubClient.on("message", function (channel, message) {
            if (channel == RedisMemcacheClient.DIRTY_KEY_CHANNEL) {
                receiveDirtyKey(message);
            }
        });
    };

    /**
     * Gather dirty keys and publish them every second.
     * @type {String[]}
     */
    var dirtyKeys = [];

    /**
     * The dirty key timeout, if currently active.
     * @type {Object}
     */
    var dirtyKeyTimeout = null;

    /**
     * Identifier for the owner of the dirty keys, so that we can prevent publishes from affecting this worker.
     * @type {string}
     */
    var ownerId = "" + Math.floor(1000000 + Math.random() * 8999999);

    /**
     * Publishes the dirty key.
     * @param key
     *   Either a single key, or multiple separated by newlines.
     */
    var publishDirtyKey = function(key) {
        if (getDirtyKeyPublishDelay()) {
            dirtyKeys.push(key);

            if (!dirtyKeyTimeout) {
                dirtyKeyTimeout = setTimeout(function() {
                    dirtyKeyTimeout = null;
                    var message = dirtyKeys.join("\n");
                    dirtyKeys = [];
                    redisClient.publish(RedisMemcacheClient.DIRTY_KEY_CHANNEL, ownerId + "\n" + message);
                }, getDirtyKeyPublishDelay());
            }
        } else {
            redisClient.publish(RedisMemcacheClient.DIRTY_KEY_CHANNEL, ownerId + "\n" + key);
        }
    };

    /**
     * Returns the delay, in millis, before dirty keys are actually sent.
     * During this time other dirty keys are gathered and sent in the same command for efficiency reasons.
     * @return {Number}
     *   Null if no publish delay.
     */
    var getDirtyKeyPublishDelay = function() {
        return options.hasOwnProperty('dirtyKeyPublishDelay') ? options.dirtyKeyPublishDelay : 1000;
    };

    /**
     * Receives dirty cache key.
     * @param key
     *   Either a single key, or multiple separated by newlines.
     */
    var receiveDirtyKey = function(key) {
        var keys = key.split("\n");
        var senderId = keys.shift();

        if (senderId !== ownerId) {
            _.each(keys, function(key) {
                stats.recvDirty++;
                delCache(key);
            });
        }
    };

    /**
     * Proxies all unknown functions to the redis client.
     */
    var enableProxy = function() {
        for (var prop in redisClient) {
            if (_.isFunction(redisClient[prop])) {
                if (!(prop in self)) {
                    (function(prop) {
                        self[prop] = function() {
                            return redisClient[prop].apply(redisClient, arguments);
                        };
                    })(prop);
                }
            }
        }
    };

    // Start proxying calls immediately.
    enableProxy();

    // Start listening for dirty caches.
    enableDirtyCacheChannel(cb || function(err) {
        if (err) {
            console.error(err);
        }
    });

}

/**
 * Redis channel on which dirty keys are exchanged.
 * @type {string}
 */
RedisMemcacheClient.DIRTY_KEY_CHANNEL = '_dcache';

module.exports = RedisMemcacheClient;