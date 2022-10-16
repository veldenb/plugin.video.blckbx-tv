import gzip
import json

import xbmc

# Global vars
cache_pool = {}
cache_pool_updated = False


def read(filename):
    global cache_pool

    # Read cache
    try:
        # Decode gzip and load json
        gzip_file_readonly = gzip.open(filename)
        cache_pool = json.loads(gzip_file_readonly.read())
        gzip_file_readonly.close()

    except (IOError, ValueError):
        cache_pool = {}


def persist(cache_key) -> any:
    def decorator(original_func) -> any:

        def new_func(param) -> str:
            global cache_pool
            global cache_pool_updated

            if cache_key not in cache_pool:
                cache_pool[cache_key] = {}

            if param in cache_pool[cache_key]:
                xbmc.log('Cache hit {}: {}'.format(cache_key, param), xbmc.LOGDEBUG)
            else:
                cache_pool[cache_key][param] = original_func(param)
                cache_pool_updated = True

            return cache_pool[cache_key][param]

        return new_func

    return decorator


def exists(cache_key: str, item: str) -> bool:
    in_cache = False

    if cache_key not in cache_pool:
        cache_pool[cache_key] = {}

    if item in cache_pool[cache_key]:
        in_cache = True

    return in_cache


def write(filename):
    global cache_pool

    # Write cache
    if cache_pool_updated:
        gzip_file_write = gzip.open(filename, 'wb')
        gzip_file_write.write(json.dumps(cache_pool).encode('utf-8'))
        gzip_file_write.close()


def clear(cache_key: str):
    xbmc.log('Clearing user page cache', xbmc.LOGDEBUG)
    cache_pool[cache_key] = {}
