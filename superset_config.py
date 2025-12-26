from cachelib.redis import RedisCache

# Use Redis for Superset caching (improves dashboard performance)
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': 'redis',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
}

# Optional: superset home directory (already mounted in docker-compose)
DATA_DIR = '/app/superset_home'

# Keep logs tidy
SUPERSET_LOG_LEVEL = 'INFO'
