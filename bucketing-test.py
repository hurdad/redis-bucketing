#!/usr/bin/env python
import logging
import sys

import redis

from benchmark import benchmark

logger = logging.getLogger(__name__)


def main():
    # configure logging
    logging.basicConfig(format="%(asctime)s [%(funcName)s][%(levelname)s] %(message)s")
    logger.setLevel(logging.DEBUG)
    logging.getLogger('benchmark').setLevel(logging.DEBUG)

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
    r = redis.StrictRedis(host='ziox1.home.lan', port=6379, db=1)
    obj = r.register_script(LUA)
    logger.debug("flushdb")
    r.flushdb()

    BUCKET_SIZE = 50000
    keys = 1000000

    with benchmark('bucket string'):
        for i in range(0, keys):
            code = str(i).zfill(20)
            maxid = obj(args=['hash' + ':' + str(int(hash(code) % BUCKET_SIZE)), code, 'hashmax'])

    logger.debug("used_memory_human : {}".format(r.info()['used_memory_human']))
    logger.debug("used_memory : {}".format(r.info()['used_memory']))
    logger.debug("flushdb")
    r.flushdb()

    with benchmark('bucket integer'):
        for i in range(0, keys):
            rediskey = i % ((sys.maxsize + 1) * 2)
            maxid = obj(args=['hash' + ':' + str(int(rediskey % BUCKET_SIZE)), i, 'hashmax'])

    logger.debug("used_memory_human : {}".format(r.info()['used_memory_human']))
    logger.debug("used_memory : {}".format(r.info()['used_memory']))
    logger.debug("flushdb")
    r.flushdb()

    with benchmark('non bucket string'):
        for i in range(0, keys):
            code = str(i).zfill(20)
            maxid = obj(args=['hash', code, 'hashmax'])

    logger.debug("used_memory_human : {}".format(r.info()['used_memory_human']))
    logger.debug("used_memory : {}".format(r.info()['used_memory']))
    logger.debug("flushdb")
    r.flushdb()

    with benchmark('non bucket integer'):
        for i in range(0, keys):
            maxid = obj(args=['hash', i, 'hashmax'])

    logger.debug("used_memory_human : {}".format(r.info()['used_memory_human']))
    logger.debug("used_memory : {}".format(r.info()['used_memory']))
    logger.debug("flushdb")
    r.flushdb()


if __name__ == "__main__":
    main()
