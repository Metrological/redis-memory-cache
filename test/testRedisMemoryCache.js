var redis = require('redis');
var RedisMemcacheClient = require('../lib/redis-memory-cache');
var async = require('async');
var should = require('should');
var _ = require('lodash');

describe('redis-memory-cache module', function() {
    var host = "localhost";
    var port = 6379;
    var testGroup = ['redis-memcache-test'];

    // Create two separate redis clients.
    var redis1, redisSub1, redis2, redisSub2;

    /**
     * @type RedisMemcacheClient.
     */
    var mcRedis1, mcRedis2;

    // Start redis clients.
    before(function(done) {
        var tasks = [];

        // Create required redis clients.
        redis1 = redis.createClient(port, host);
        redis2 = redis.createClient(port, host);
        redisSub1 = redis.createClient(port, host);
        redisSub2 = redis.createClient(port, host);

        // Test them all.
        tasks.push(function(cb) {redis1.ping(cb)});
        tasks.push(function(cb) {redis2.ping(cb)});
        tasks.push(function(cb) {redisSub1.ping(cb)});
        tasks.push(function(cb) {redisSub2.ping(cb)});

        // Create memcached-wrapped clients.
        tasks.push(function(cb) {
            // E1 uses a delay.
            mcRedis1 = new RedisMemcacheClient(redis1, redisSub1, {dirtyKeyPublishDelay: 100});
            // E2 uses direct keys.
            mcRedis2 = new RedisMemcacheClient(redis2, redisSub2, {dirtyKeyPublishDelay: 100});
            cb();
        });

        tasks.push(clearRedisTestKeys);

        async.series(tasks, done);
    });

    var clearRedisTestKeys = function(done) {
        // Clear all old redis test keys.
        redis1.keys(testGroup.join(":") + "*", function(err, keys) {
            if (keys.length) {
                redis1.del(keys, done);
            } else {
                done();
            }
        });
    };


    describe('Test enabling / disabling paths', function() {
        it('should have root disabled initially', function() {
            should(mcRedis1.pathIsEnabled([])).equal(false);
        });

        it('should allow complex hierarchical path settings', function() {
            mcRedis1.setCachePath(['key1'], true);
            mcRedis1.setCachePath(['key1', 'sub'], false);
            should(mcRedis1.pathIsEnabled([])).equal(false);
            should(mcRedis1.pathIsEnabled(['key1'])).equal(true);
            should(mcRedis1.pathIsEnabled(['key1', 'sub2'])).equal(true);
            should(mcRedis1.pathIsEnabled(['key1', 'sub2', 'subsub'])).equal(true);
            should(mcRedis1.pathIsEnabled(['key1', 'sub'])).equal(false);
            should(mcRedis1.pathIsEnabled(['key1', 'sub', 'sub2'])).equal(false);
            mcRedis1.setCachePath(['key1'], false);
            should(mcRedis1.pathIsEnabled(['key1'])).equal(false);
            should(mcRedis1.pathIsEnabled(['key1', 'sub2'])).equal(false);
        });

        it('should allow resetting cache paths', function() {
            mcRedis1.resetCachePaths();
            should(mcRedis1.pathIsEnabled([])).equal(false);
            should(mcRedis1.pathIsEnabled(['key1'])).equal(false);
        });

        it('should allow root enabled', function() {
            mcRedis1.resetCachePaths();
            mcRedis1.setCachePath([], true);
            should(mcRedis1.pathIsEnabled([])).equal(true);
            should(mcRedis1.pathIsEnabled(['key1'])).equal(true);
        });

        after(function() {
            mcRedis1.resetCachePaths();
        });
    });

    var getPath = function(path) {
        return testGroup.concat(path);
    };

    var getKey = function(path) {
        return getPath(path).join(":");
    };

    var shouldGet = function(rc, key, val, cb) {
        rc.get(key, function(err, res) {
            if (err) {
                return cb(err);
            }

            should(res).equal(val);
            cb();
        });
    };

    var shouldGetJson = function(rc, key, val, cb) {
        rc.get(key, function(err, res) {
            if (err) {
                return cb(err);
            }

            should(_.isEqual(val, res)).equal(true);
            cb();
        }, true);
    };

    var shouldMget = function(rc, expected, cb) {
        var keys = _.keys(expected);
        var values = _.values(expected);
        rc.mget(keys, function(err, res) {
            if (err) {
                return cb(err);
            }

            should(_.isEqual(res, values)).equal(true);
            cb();
        });
    };

    var shouldMgetJson = function(rc, expected, cb) {
        var keys = _.keys(expected);
        var values = _.values(expected);
        rc.mget(keys, function(err, res) {
            if (err) {
                return cb(err);
            }

            should(_.isEqual(res, values)).equal(true);
            cb();
        }, true);
    };

    var shouldCacheStatus = function(rc, hits, misses, recvDirty, reset, cb) {
        var check = function() {
            var rStats = rc.getCacheStats();
            should(rStats.hits).equal(hits);
            should(rStats.misses).equal(misses);
            should(rStats.recvDirty).equal(recvDirty);

            if (reset !== false) {
                rc.resetCacheStats();
            }

            cb();
        };

        if (recvDirty) {
            // Wait for a small time to really receive all expected.
            setTimeout(check, 200);
        } else {
            check();
        }
    };

    var shouldResetCache = function(rc) {
        return function(cb) {
            rc.resetCacheStats();
            cb();
        };
    };

    describe('not-cached path', function() {
        it('should set and get without using a cache', function(done) {
            var tasks = [];
            tasks.push(function(cb) {
                mcRedis1.set(getKey(["test"]), "val", cb);
            });
            tasks.push(function(cb) {
                shouldGet(mcRedis1, getKey(["test"]), "val", cb);
            });
            tasks.push(function(cb) {
                shouldGet(mcRedis2, getKey(["test"]), "val", cb);
            });
            tasks.push(function(cb) {
                shouldCacheStatus(mcRedis1, 0, 0, 0, true, cb);
            });
            tasks.push(function(cb) {
                shouldCacheStatus(mcRedis2, 0, 0, 0, true, cb);
            });
            async.series(tasks, done);
        });
    });

    describe('cache key life cycle', function() {
        before(function() {
            mcRedis1.setCachePath(getPath(['cached']), true);
            mcRedis2.setCachePath(getPath(['cached']), true);
        });

        describe('simple set and get', function(done) {
            it('should set cached=val1 on E1', function(cb) {
                mcRedis1.set(getKey(["cached"]), "val1", cb)
            });
            it('should cause dirty key on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 0, 1, true, cb);
            });
            it('should get cached=val1 on E1', function(cb) {
                shouldGet(mcRedis1, getKey(["cached"]), "val1", cb);
            });
            it('should cause cache hit on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 1, 0, 0, true, cb);
            });
            it('should get cached=val1 on E2', function(cb) {
                shouldGet(mcRedis2, getKey(["cached"]), "val1", cb);
            });
            it('should cause cache miss on E1', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 1, 0, true, cb);
            });
            it('should get cached=val1 on E2 again', function(cb) {
                shouldGet(mcRedis2, getKey(["cached"]), "val1", cb);
            });
            it('should cause cache hit on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 1, 0, 0, true, cb);
            });
        });

        describe('updating currently cached', function(done) {
            it('should set cached=val2 on E1', function(cb) {
                mcRedis1.set(getKey(["cached"]), "val2", cb);
            });
            it('should cause dirty key on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 0, 1, true, cb);
            });
            it('should get cached=val2 on E1', function(cb) {
                shouldGet(mcRedis1, getKey(["cached"]), "val2", cb);
            });
            it('should cause cache hit on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 1, 0, 0, true, cb);
            });
            it('should get cached=val2 on E2', function(cb) {
                shouldGet(mcRedis2, getKey(["cached"]), "val2", cb);
            });
            it('should cause cache miss on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 1, 0, true, cb);
            });
            it('should get cached=val2 on E2 again', function(cb) {
                shouldGet(mcRedis2, getKey(["cached"]), "val2", cb);
            });
            it('should cause cache hit on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 1, 0, 0, true, cb);
            });
        });

        describe('deleting cached', function(done) {
            it('should delete cached on E1', function(cb) {
                mcRedis1.del(getKey(["cached"]), cb);
            });
            it('should cause dirty key on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 0, 1, true, cb);
            });
            it('should get cached=null on E1', function(cb) {
                shouldGet(mcRedis1, getKey(["cached"]), null, cb);
            });
            it('should cause cache hit on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 1, 0, 0, true, cb);
            });
            it('should get cached=null on E1 again', function(cb) {
                shouldGet(mcRedis1, getKey(["cached"]), null, cb);
            });
            it('should cause cache hit on E1 again', function(cb) {
                shouldCacheStatus(mcRedis1, 1, 0, 0, true, cb);
            });
            it('should get cached=null on E2', function(cb) {
                shouldGet(mcRedis2, getKey(["cached"]), null, cb);
            });
            it('should cause cache miss on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 1, 0, true, cb);
            });
            it('should get cached=null on E2 again', function(cb) {
                shouldGet(mcRedis2, getKey(["cached"]), null, cb);
            });
            it('should cause cache hit on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 1, 0, 0, true, cb);
            });
        });

        describe('test mset,mget with both cached and not-cached keys', function(done) {
            it('should mset 2 cached and 2 not-cached keys on E1', function(cb) {
                mcRedis1.mset(
                    getKey(["cached", "key1"]), "a",
                    getKey(["cached", "key2"]), "b",
                    getKey(["not-cached", "key1"]), "c",
                    getKey(["not-cached", "key2"]), "d",
                    cb
                );
            });
            it('should cause 2 dirty keys on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 0, 2, true, cb);
            });
            it('should mget 4 keys with correct values on E1', function(cb) {
                var expected = {};
                expected[getKey(["cached", "key1"])] = "a";
                expected[getKey(["cached", "key2"])] = "b";
                expected[getKey(["not-cached", "key1"])] = "c";
                expected[getKey(["not-cached", "key2"])] = "d";
                shouldMget(mcRedis1, expected, cb);
            });
            it('should cause 2 cache hits on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 2, 0, 0, true, cb);
            });
            it('should mget 4 keys with correct values on E2', function(cb) {
                var expected = {};
                expected[getKey(["cached", "key1"])] = "a";
                expected[getKey(["cached", "key2"])] = "b";
                expected[getKey(["not-cached", "key1"])] = "c";
                expected[getKey(["not-cached", "key2"])] = "d";
                shouldMget(mcRedis2, expected, cb);
            });
            it('should cause 2 cache misses on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 2, 0, true, cb);
            });
            it('should mget 4 keys with correct values on E2 again', function(cb) {
                var expected = {};
                expected[getKey(["cached", "key1"])] = "a";
                expected[getKey(["cached", "key2"])] = "b";
                expected[getKey(["not-cached", "key1"])] = "c";
                expected[getKey(["not-cached", "key2"])] = "d";
                shouldMget(mcRedis2, expected, cb);
            });
            it('should cause 2 cache hits on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 2, 0, 0, true, cb);
            });

            it('should change cached:key1, cached:key2 and not-cached:key1 using mset on E2', function(cb) {
                mcRedis2.mset(
                    getKey(["cached", "key1"]), "a2",
                    getKey(["not-cached", "key1"]), "c2",
                    cb
                );
            });
            it('should cause dirty key on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 0, 0, 1, true, cb);
            });
            it('should mget 4 keys with updated values on E2', function(cb) {
                var expected = {};
                expected[getKey(["cached", "key1"])] = "a2";
                expected[getKey(["cached", "key2"])] = "b";
                expected[getKey(["not-cached", "key1"])] = "c2";
                expected[getKey(["not-cached", "key2"])] = "d";
                shouldMget(mcRedis2, expected, cb);
            });
            it('should cause 2 cache hits on E2 again', function(cb) {
                shouldCacheStatus(mcRedis2, 2, 0, 0, true, cb);
            });
            it('should mget 4 keys with updated values on E1', function(cb) {
                var expected = {};
                expected[getKey(["cached", "key1"])] = "a2";
                expected[getKey(["cached", "key2"])] = "b";
                expected[getKey(["not-cached", "key1"])] = "c2";
                expected[getKey(["not-cached", "key2"])] = "d";
                shouldMget(mcRedis1, expected, cb);
            });
            it('should cause cache hit and cache miss on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 1, 1, 0, true, cb);
            });

            // Delete all.
            it('should delete all keys on E1', function(cb) {
                mcRedis1.del([getKey(["cached", "key1"]), getKey(["cached", "key2"]), getKey(["not-cached", "key1"]), getKey(["not-cached", "key2"])], cb);
            });
            it('should cause 2 dirty keys on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 0, 2, true, cb);
            });
            it('should mget 4 keys with all null values on E1', function(cb) {
                var expected = {};
                expected[getKey(["cached", "key1"])] = null;
                expected[getKey(["cached", "key2"])] = null;
                expected[getKey(["not-cached", "key1"])] = null;
                expected[getKey(["not-cached", "key2"])] = null;
                shouldMget(mcRedis1, expected, cb);
            });
            it('should cause 2 cache hits on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 2, 0, 0, true, cb);
            });
            it('should mget 4 keys with all null values on E2', function(cb) {
                var expected = {};
                expected[getKey(["cached", "key1"])] = null;
                expected[getKey(["cached", "key2"])] = null;
                expected[getKey(["not-cached", "key1"])] = null;
                expected[getKey(["not-cached", "key2"])] = null;
                shouldMget(mcRedis2, expected, cb);
            });
            it('should cause 2 cache misses on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 2, 0, true, cb);
            });
        });

        describe('should work when fetching json', function(done) {
            var tasks = [];

            var obj1 = {"test": 1};
            var obj2 = [1,2];
            var obj3 = "test";
            var obj4 = false;

            it('should mset some keys on E1', function(cb) {
                mcRedis1.mset(
                    getKey(["cached", "key1"]), JSON.stringify(obj1),
                    getKey(["cached", "key2"]), JSON.stringify(obj2),
                    getKey(["not-cached", "key1"]), JSON.stringify(obj3),
                    getKey(["not-cached", "key2"]), JSON.stringify(obj4),
                    cb
                );
            });

            it('should cause 2 dirty keys on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 0, 2, true, cb);
            });

            it('should produce correct result when referencing non-existing key', function(cb) {
                shouldGetJson(mcRedis1, getKey(["cached", "doesnotexist"]), null, cb);
            });
            it('should cause 1 cache miss on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 0, 1, 0, true, cb);
            });
            it('should produce correct result on E1 (cached:key1)', function(cb) {
                shouldGetJson(mcRedis1, getKey(["cached", "key1"]), obj1, cb);
            });
            it('should cause 1 cache hit on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 1, 0, 0, true, cb);
            });
            it('should not be affected by changing the local object', function(cb) {
                mcRedis1.get(getKey(["cached", "key1"]), function(err, res) {
                    if (err) {
                        return cb(err);
                    }
                    res.test = 2;
                    shouldGetJson(mcRedis1, getKey(["cached", "key1"]), obj1, function(err) {
                        if (err) {
                            return cb(err);
                        }
                        mcRedis1.resetCacheStats();
                        cb();
                    });
                }, true);
            });
            it('should affect the local cached object when setting the noClone flag', function(cb) {
                var key = getKey(["cached", "key2"]);
                mcRedis1.get(key, function(err, res) {
                    if (err) {
                        return cb(err);
                    }
                    res.test = 2;
                    mcRedis1.get(key, function(err, res2) {
                        if (err) {
                            return cb(err);
                        }
                        should(res === res2).equal(true);
                        should(_.isEqual(res, res2)).equal(true);

                        res.test = obj1.test;
                        cb();
                    }, true, true);
                }, true, true);
            });
            it('should affect the local cached object when setting the noClone flag (mget)', function(cb) {
                var key = getKey(["cached", "key2"]);
                mcRedis1.get(key, function(err, res) {
                    if (err) {
                        return cb(err);
                    }
                    res.test = 2;
                    mcRedis1.mget([key], function(err, res3) {
                        if (err) {
                            return cb(err);
                        }
                        should(res === res3[0]).equal(true);
                        should(res.length === res3[0].length).equal(true);
                        res.test = obj1.test;
                        mcRedis1.resetCacheStats();
                        cb();
                    }, true, true);
                }, true, true);
            });
            it('should produce correct result on E1 (cached:key2)', function(cb) {
                shouldGetJson(mcRedis1, getKey(["cached", "key2"]), obj2, cb);
            });
            it('should produce correct result on E1 (not-cached:key1)', function(cb) {
                shouldGetJson(mcRedis1, getKey(["not-cached", "key1"]), obj3, cb);
            });
            it('should produce correct result on E1 (not-cached:key2)', function(cb) {
                shouldGetJson(mcRedis1, getKey(["not-cached", "key2"]), obj4, cb);
            });
            it('should cause 1 cache hit on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 1, 0, 0, true, cb);
            });
            it('should produce correct result (mget) on E1', function(cb) {
                var expected = {};
                expected[getKey(["cached", "key1"])] = obj1;
                expected[getKey(["cached", "key2"])] = obj2;
                expected[getKey(["not-cached", "key1"])] = obj3;
                expected[getKey(["not-cached", "key2"])] = obj4;
                shouldMgetJson(mcRedis1, expected, cb);
            });
            it('should cause 2 cache hits on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 2, 0, 0, true, cb);
            });

            // Dirty key test.
            it('set new value on E2', function(cb) {
                obj2 = [1,2,3];
                mcRedis2.set(getKey(["cached", "key2"]), JSON.stringify(obj2), cb);
            });
            it('should cause dirty key on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 0, 0, 1, true, cb);
            });
            it('set new value on E1', function(cb) {
                obj3 = {"qwerty":123};
                mcRedis1.set(getKey(["not-cached", "key1"]), JSON.stringify(obj3), cb);
            });
            it('should affect the local cached object when setting the noClone flag, when no cache available', function(cb) {
                var key = getKey(["cached", "key2"]);
                mcRedis1.get(key, function(err, res) {
                    if (err) {
                        return cb(err);
                    }
                    res.push(4);
                    mcRedis1.get(key, function(err, res2) {
                        if (err) {
                            return cb(err);
                        }
                        should(res === res2).equal(true);
                        should(res.length === res2.length).equal(true);

                        res.pop();
                        cb();
                    }, true, true);
                }, true, true);
            });
            it('should affect the local cached object when setting the noClone flag, when no cache available (mget)', function(cb) {
                var key = getKey(["cached", "key2"]);
                mcRedis1.get(key, function(err, res) {
                    if (err) {
                        return cb(err);
                    }
                    res.push(4);
                    mcRedis1.mget([key], function(err, res3) {
                        if (err) {
                            return cb(err);
                        }
                        should(res === res3[0]).equal(true);
                        should(res.length === res3[0].length).equal(true);
                        res.pop();

                        cb();
                    }, true, true);
                }, true, true);
            });
            it('should produce corrected result (mget) on E1', function(cb) {
                var expected = {};
                expected[getKey(["cached", "key1"])] = obj1;
                expected[getKey(["cached", "key2"])] = obj2;
                expected[getKey(["not-cached", "key1"])] = obj3;
                expected[getKey(["not-cached", "key2"])] = obj4;
                shouldMgetJson(mcRedis1, expected, cb);
            });
            it('should cause 5 cache hits and 1 cache miss on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 5, 1, 0, true, cb);
            });

            // Deletion.
            it('should delete 2 keys on E1', function(cb) {
                mcRedis1.del([getKey(["cached", "key1"]), getKey(["not-cached", "key1"])], cb);
            });
            it('should cause dirty key on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 0, 1, true, cb);
            });
            it('should produce corrected result (mget) on E2', function(cb) {
                var expected = {};
                expected[getKey(["cached", "key1"])] = null;
                expected[getKey(["cached", "key2"])] = obj2;
                expected[getKey(["not-cached", "key1"])] = null;
                expected[getKey(["not-cached", "key2"])] = obj4;
                shouldMgetJson(mcRedis2, expected, cb);
            });
            it('should cause 1 cache hit and 1 cache miss on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 1, 1, 0, true, cb);
            });

            // Clean up.
            it('should delete remaining keys on E1', function(cb) {
                mcRedis1.del([getKey(["cached", "key2"]), getKey(["not-cached", "key4"])], cb);
            });
            it('should cause dirty key on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 0, 1, true, cb);
            });
        });

        describe('calls without callbacks', function() {
            it('should work for set', function(cb) {
                mcRedis1.set(getKey(["cached"]), "s");
                setTimeout(cb, 100);
            });
            it('should cause dirty key on E2', function(cb) {
                shouldCacheStatus(mcRedis2, 0, 0, 1, true, cb);
            });
            it('should work for get', function(cb) {
                mcRedis1.get(getKey(["cached"]));
                setTimeout(function() {
                    shouldGet(mcRedis1, getKey(["cached"]), "s", function(err) {
                        if (err) {
                            return cb(err);
                        }
                        // Should have 2 cache hits.
                        shouldCacheStatus(mcRedis1, 2, 0, 0, true, cb);
                    });
                }, 100);
            });
            it('should work for mset', function(cb) {
                mcRedis2.mset(
                    getKey(["cached"]), "t"
                );
                setTimeout(cb, 100);
            });
            it('should cause dirty key on E1', function(cb) {
                shouldCacheStatus(mcRedis1, 0, 0, 1, true, cb);
            });
            it('should work for mget', function(cb) {
                mcRedis1.mget([getKey(["cached"])]);
                setTimeout(function() {
                    var expected = {};
                    expected[getKey(["cached"])] = "t";
                    shouldMget(mcRedis1, expected, function(err) {
                        if (err) {
                            return cb(err);
                        }
                        // Should have 1 cache hit and 1 cache miss.
                        shouldCacheStatus(mcRedis1, 1, 1, 0, true, cb);
                    });
                }, 100);
            });
        });

        after(function(done) {
            clearRedisTestKeys(done);
        });

    });

});