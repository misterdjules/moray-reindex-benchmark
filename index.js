var assert = require('assert-plus');
var bunyan = require('bunyan');
var dashdash = require('dashdash');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var moray = require('moray');
var path = require('path');
var vasync = require('vasync');

function addObjects(morayClient, options, objectTemplate, callback) {
    assert.object(morayClient, 'morayClient');
    assert.object(options, 'options');
    assert.string(options.bucketName, 'options.bucketName');
    assert.number(options.nb, 'options.nb');
    assert.ok(options.nb > 0, 'options.nb > 0');
    assert.number(options.addConcurrency, 'options.addConcurrency');

    var bucketName = options.bucketName;
    var totalNbObjectsCreated = 0;
    var totalNbObjectsToCreate = options.nb;

    function _addObjects() {
        var i = 0;
        var keys = [];
        var nbObjectsToCreate =
            Math.min(totalNbObjectsToCreate - totalNbObjectsCreated,
                options.addConcurrency);

        if (nbObjectsToCreate === 0) {
            callback();
            return;
        }

        for (i = 0; i < nbObjectsToCreate; ++i) {
            keys.push(libuuid.create());
        }

        vasync.forEachParallel({
            func: function addObject(key, done) {
                morayClient.putObject(bucketName, key, objectTemplate,
                    function onObjectAdded(addErr) {
                        var nonTransientErrorNames = [
                            'InvalidIndexTypeError',
                            'UniqueAttributeError'
                        ];

                        if (addErr &&
                            nonTransientErrorNames.indexOf(addErr.name)
                                !== -1) {
                            done(addErr);
                            return;
                        }

                        if (!addErr) {
                            ++totalNbObjectsCreated;
                        }

                        done();
                    });
            },
            inputs: keys
        }, function onObjectsAdded(err) {
            if (err) {
                callback(err);
            } else {
                setImmediate(_addObjects);
            }
        });
    }

    _addObjects();
}

function reindexObjects(morayClient, bucketName, options, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketname');
    assert.object(options, 'options');
    assert.number(options.reindexCount, 'options.reindexCount');
    assert.func(callback, 'callback');

    function _reindex() {
        morayClient.reindexObjects(bucketName, options.reindexCount,
            function onObjectsReindex(reindexErr, count) {
                if (reindexErr) {
                    callback(reindexErr);
                    return;
                } else {
                    if (count.remaining === 0) {
                        callback();
                        return;
                    } else {
                        setImmediate(_reindex);
                    }
                }
            });
    }

    _reindex();
}

function runBenchmark(options) {
    var config = require('./config.json');
    var log = bunyan.createLogger({name: 'moray-client'});
    var morayClient;
    var morayConfig = jsprim.deepCopy(config);
    var TEST_BUCKET_CFG_V0 = {
        options: {
            version: 0
        }
    };
    var TEST_BUCKET_CFG_V1 = {
        index: {
            indexed_field_string_1: {
                type: 'string'
            }
        },
        options: {
            version: 1
        }
    };
    var TEST_BUCKET_NAME = 'moray_reindex_benchmark_' +
        libuuid.create().substr(0, 7);

    morayConfig.log = log;
    morayClient = moray.createClient(morayConfig);

    morayClient.on('connect', function onMorayConnected() {
        vasync.pipeline({funcs: [
            function createTestBucket(arg, next) {
                console.log('creating moray bucket...');

                morayClient.createBucket(TEST_BUCKET_NAME, TEST_BUCKET_CFG_V0,
                    next);
            },
            function addTestObjects(arg, next) {
                console.log('adding tests objects...');

                addObjects(morayClient, {
                    bucketName: TEST_BUCKET_NAME,
                    nb: options.nbVms,
                    addConcurrency: options.addVmsConcurrency
                }, {
                    indexed_field_string_1: 'foo'
                }, next);
            },
            function upgradeBucket(arg, next) {
                morayClient.updateBucket(TEST_BUCKET_NAME, TEST_BUCKET_CFG_V1,
                    next);
            },
            function reindexTestObjects(arg, next) {
                console.log('reindexing objects...');

                var reindexObjectsStartTime = process.hrtime();
                var reindexObjectsTime;
                var reindexObjectsDurationInMs;

                reindexObjects(morayClient, TEST_BUCKET_NAME, {
                    reindexCount: options.reindexCount
                }, function onObjectsReindexed(reindexErr) {
                    reindexObjectsTime =
                        process.hrtime(reindexObjectsStartTime);
                    reindexObjectsDurationInMs = (reindexObjectsTime[0] *
                        1e9 + reindexObjectsTime[1]) / 1e6;

                    console.log('reindexObjects duration: %dms',
                        reindexObjectsDurationInMs);

                    next(reindexErr);
                });
            }
        ]}, function allDone(err) {
            morayClient.close();
            if (err) {
                console.log('Error:', err);
            } else {
                console.log('Benchmark completed successfully!');
            }
        });
    });
}

function printUsage(cmdLineOptionsParser) {
    assert.object(cmdLineOptionsParser, 'cmdLineOptionsParser');

    var help = cmdLineOptionsParser.help({includeEnv: true}).trimRight();

    console.log('usage: node ' + path.basename(__filename) + ' [OPTIONS]\n' +
        'options:\n' + help);
}

function main() {
    var CMDLINE_OPTS = [
        {
            names: ['help', 'h'],
            type: 'bool',
            help: 'Print this help and exit'
        },
        {
            names: ['n'],
            type: 'positiveInteger',
            help: 'Number of test objects to create'
        },
        {
            names: ['c'],
            type: 'positiveInteger',
            help: 'The number of objects added concurrently'
        },
        {
            names: ['r'],
            type: 'positiveInteger',
            help: 'The number of objects to reindex once at a time'
        }
    ];
    var cmdlineOptionsParser = dashdash.createParser({options: CMDLINE_OPTS});
    var concurrencyParam;
    var DEFAULT_CONCURRENCY = 100;
    var DEFAULT_NB_TEST_VMS_TO_CREATE = 100000;
    var DEFAULT_REINDEX_COUNT = 100;
    var nbVmsParam;
    var reindexCountParam;

    try {
        var parsedCmdlineOpts = cmdlineOptionsParser.parse(process.argv);

        if (parsedCmdlineOpts.help) {
            printUsage(cmdlineOptionsParser);
        } else {
            nbVmsParam = Number(parsedCmdlineOpts.n) ||
                DEFAULT_NB_TEST_VMS_TO_CREATE;

            concurrencyParam = Number(parsedCmdlineOpts.c) ||
                DEFAULT_CONCURRENCY;

            reindexCountParam = Number(parsedCmdlineOpts.r) ||
                DEFAULT_REINDEX_COUNT;

            runBenchmark({
                nbVms: nbVmsParam,
                addVmsConcurrency: concurrencyParam,
                reindexCount: reindexCountParam
            });
        }
    } catch (err) {
        console.error('Could not parse command line options, error:', err);
        printUsage(cmdlineOptionsParser);
        process.exit(1);
    }
}

main();