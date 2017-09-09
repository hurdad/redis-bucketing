#!/usr/bin/env python
import logging

import redis
from pyspark import SparkConf, SparkContext

from benchmark import benchmark

logger = logging.getLogger(__name__)

BUCKET_SIZE = 50000
KEYS = 10000000


def main():
    # configure logging
    logging.basicConfig(format="%(asctime)s [%(funcName)s][%(levelname)s] %(message)s")
    logger.setLevel(logging.DEBUG)
    logging.getLogger('benchmark').setLevel(logging.DEBUG)

    # init spar
    sc = SparkContext(conf=SparkConf().setAppName(__name__))
    quiet_logs(sc)

    # create rdd with sequential codes
    rdd = sc.parallelize(range(0, KEYS-1)).repartition(16).persist()

    LUA = """
     local hashname = ARGV[1]
     local keyname = ARGV[2]
     local maxkeyname = ARGV[3]
     local value = redis.call('HGET', hashname, keyname)
     if value == false then
         local maxid = redis.call('INCR', maxkeyname)
         redis.call('HSETNX', hashname, keyname, maxid)
         return maxid
     else
         return tonumber(value)
     end
     """

    r_driver = redis.StrictRedis(host='ziox1.home.lan', port=6379, db=2)

    ## Bucketing
    def bucket_string_lookup(partition):
        r = redis.StrictRedis(host='ziox1.home.lan', port=6379, db=2)
        obj = r.register_script(LUA)
        r.flushdb()
        return [
            obj(args=['hash' + ':' + str(int(hash(str(key).zfill(20)) % BUCKET_SIZE)), str(key).zfill(20), "maxkey"])
            for key in partition]

    def bucket_integer_lookup(partition):
        r = redis.StrictRedis(host='ziox1.home.lan', port=6379, db=2)
        obj = r.register_script(LUA)
        r.flushdb()
        return [obj(args=['hash' + ':' + str(int(key % BUCKET_SIZE)), key, "maxkey"]) for key in partition]

    with benchmark('bucket string'):
        rdd.mapPartitions(bucket_string_lookup).collect()

    logger.debug("used_memory_human : {}".format(r_driver.info()['used_memory_human']))
    logger.debug("used_memory : {}".format(r_driver.info()['used_memory']))

    with benchmark('bucket integer'):
        rdd.mapPartitions(bucket_integer_lookup).collect()

    logger.debug("used_memory_human : {}".format(r_driver.info()['used_memory_human']))
    logger.debug("used_memory : {}".format(r_driver.info()['used_memory']))

    ## Non Bucketing
    def non_bucket_string_lookup(partition):
        r = redis.StrictRedis(host='ziox1.home.lan', port=6379, db=2)
        obj = r.register_script(LUA)
        r.flushdb()
        return [obj(args=['hash', str(key).zfill(20), "maxkey"]) for key in partition]

    def non_bucket_integer_lookup(partition):
        r = redis.StrictRedis(host='ziox1.home.lan', port=6379, db=2)
        obj = r.register_script(LUA)
        r.flushdb()
        return [obj(args=['hash', key, "maxkey"]) for key in partition]

    with benchmark('non bucket string'):
        rdd.mapPartitions(non_bucket_string_lookup).collect()

    logger.debug("used_memory_human : {}".format(r_driver.info()['used_memory_human']))
    logger.debug("used_memory : {}".format(r_driver.info()['used_memory']))

    with benchmark('non bucket integer'):
        rdd.mapPartitions(non_bucket_integer_lookup).collect()

    logger.debug("used_memory_human : {}".format(r_driver.info()['used_memory_human']))
    logger.debug("used_memory : {}".format(r_driver.info()['used_memory']))


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


if __name__ == "__main__":
    main()
