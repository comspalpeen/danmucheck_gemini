# redis_client.py
"""
全局 Redis 客户端模块
在应用启动时调用 init_redis() 初始化，其他模块通过 get_redis() 获取客户端实例
"""
import redis.asyncio as redis
import logging

logger = logging.getLogger("RedisClient")

_redis_client = None


async def init_redis(url: str = "redis://localhost:6379/0"):
    """
    初始化全局 Redis 连接
    :param url: Redis 连接字符串，例如 "redis://localhost:6379/0" 或 "redis://:password@host:port/db"
    """
    global _redis_client
    if _redis_client is not None:
        logger.warning("⚠️ Redis 已经初始化，跳过重复初始化")
        return _redis_client
    
    _redis_client = redis.from_url(url, decode_responses=True)
    logger.info(f"✅ 全局 Redis 连接已初始化: {url.split('@')[-1]}")  # 隐藏密码
    return _redis_client


def get_redis():
    """
    获取全局 Redis 客户端实例
    :raises RuntimeError: 如果 Redis 尚未初始化
    """
    if _redis_client is None:
        raise RuntimeError("❌ Redis 未初始化，请先调用 init_redis()")
    return _redis_client


async def close_redis():
    """
    关闭全局 Redis 连接
    """
    global _redis_client
    if _redis_client:
        await _redis_client.close()
        logger.info("👋 全局 Redis 连接已关闭")
        _redis_client = None
