# db.py
import time
import asyncio
import json
from datetime import datetime
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError, BulkWriteError, CollectionInvalid
from pymongo import IndexModel, ASCENDING, DESCENDING
from redis_client import get_redis
from datetime import datetime,timedelta
logger = logging.getLogger("DB")


def datetime_serializer(obj):
    """JSON 序列化时处理 datetime 对象"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def datetime_deserializer(data: dict) -> dict:
    """将 ISO 格式字符串还原为 datetime 对象"""
    if 'created_at' in data and isinstance(data['created_at'], str):
        try:
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        except ValueError:
            data['created_at'] = datetime.now()
    return data


class AsyncMongoDBHandler:
    def __init__(self, uri="mongodb://gogogo:chufale@localhost:4396/admin", db_name="douyin_live_data"):
        try:
            # Motor 的连接建立是非阻塞的
            self.client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[db_name]
            
            # Redis 缓冲区 Key
            self.REDIS_CHAT_KEY = "buffer:chats"
            self.REDIS_GIFT_KEY = "buffer:gifts"
            
            # 配置
            self.BATCH_SIZE = 500
            self.LAST_WRITE_TIME = time.time()
            self.BUFFER_TIMEOUT = 5  # 缩短写入间隔，适应时序数据
            
            # 定义时序集合名称
            self.COL_GIFT = "live_gifts"
            self.COL_CHAT = "live_chats"
            
            logger.info(f"✅ [Async] MongoDB Client 初始化完成: {db_name}")
        except Exception as e:
            logger.error(f"❌ MongoDB 初始化失败: {e}")
            raise e

    async def init_indexes(self):
        """
        初始化索引及 Time Series 集合
        """
        try:
            existing_cols = await self.db.list_collection_names()

            # --- 1. 创建礼物时序集合 ---
            if self.COL_GIFT not in existing_cols:
                try:
                    await self.db.create_collection(
                        self.COL_GIFT,
                        timeseries={
                            "timeField": "created_at",   # 必须是 Date 类型
                            "metaField": "web_rid",      # 用于索引和分桶的关键字段
                            "granularity": "seconds"     # 直播数据粒度为秒级
                        }
                    )
                    logger.info(f"✅ 创建时序集合: {self.COL_GIFT}")
                except CollectionInvalid:
                    pass # 可能并发创建已存在

            # --- 2. 创建弹幕时序集合 ---
            if self.COL_CHAT not in existing_cols:
                try:
                    await self.db.create_collection(
                        self.COL_CHAT,
                        timeseries={
                            "timeField": "created_at",
                            "metaField": "web_rid",
                            "granularity": "seconds"
                        }
                    )
                    logger.info(f"✅ 创建时序集合: {self.COL_CHAT}")
                except CollectionInvalid:
                    pass

            # --- 3. 创建常规索引 ---
            # Authors 索引
            await self.db['authors'].create_index("sec_uid", unique=True)
            
            # Rooms 索引
            await self.db['rooms'].create_index([("room_id", ASCENDING)])
            await self.db['rooms'].create_index([("live_status", ASCENDING)])
            
            # PK 历史索引
            await self.db['pk_history'].create_index([("battle_id", ASCENDING), ("room_id", ASCENDING)])
            
            # 为时序集合补充二级索引
            await self.db[self.COL_GIFT].create_index([("gift_name", ASCENDING)])
            await self.db[self.COL_CHAT].create_index([("user_id", ASCENDING)])
            
            await self.db[self.COL_GIFT].create_index([("room_id", ASCENDING), ("total_diamond_count", DESCENDING)])
            await self.db[self.COL_GIFT].create_index([("room_id", ASCENDING), ("gift_name", ASCENDING)])
            await self.db[self.COL_GIFT].create_index([("room_id", ASCENDING), ("user_name", ASCENDING)])

            # 弹幕索引
            await self.db[self.COL_CHAT].create_index([("room_id", ASCENDING), ("created_at", DESCENDING)])
            await self.db[self.COL_CHAT].create_index([("room_id", ASCENDING), ("user_name", ASCENDING)])
            await self.db[self.COL_CHAT].create_index([("user_name", ASCENDING)])
            await self.db[self.COL_CHAT].create_index([("sec_uid", ASCENDING)]) # 用于精准搜ID
            
            await self.db['pk_history'].create_index([("room_id", ASCENDING), ("created_at", DESCENDING)])
            
            logger.info("✅ 数据库集合与索引检查完成")
        except Exception as e:
            logger.error(f"❌ 索引/集合初始化失败: {e}")

    async def save_room_info(self, data: dict):
        """保存直播间基础信息 (常规集合)"""
        if not data: return
        try:
            update_fields = data.copy()
            
            if 'created_at' in update_fields:
                update_fields.pop('created_at')

            update_fields['updated_at'] = datetime.now()
            insert_fields = {"created_at": datetime.now()}

            if 'start_follower_count' in update_fields:
                insert_fields['start_follower_count'] = update_fields.pop('start_follower_count')
            else:
                insert_fields['start_follower_count'] = 0

            await self.db['rooms'].update_one(
                {"room_id": data['room_id']}, 
                {
                    "$set": update_fields,
                    "$setOnInsert": insert_fields
                },
                upsert=True
            )
        except PyMongoError as e:
            logger.error(f"❌ [DB] 保存直播间信息失败: {e}")

    async def set_room_ended(self, room_id: str):
        if not room_id: return
        try:
            end_time = datetime.now()
            await self.db['rooms'].update_one(
                {"room_id": room_id},
                {
                    "$set": {
                        "live_status": 4, 
                        "room_status": 4,
                        "end_time": end_time,
                        "updated_at": end_time
                    }
                }
            )
            logger.info(f"🏁 [DB] 直播间 {room_id} 已标记为结束")
        except PyMongoError as e:
            logger.error(f"❌ [DB] 标记结束失败: {e}")

    async def update_room_realtime(self, room_id: str, live_status: int, current_follower_count: int):
        if not room_id: return
        try:
            update_fields = {
                "updated_at": datetime.now(),
                "live_status": live_status, 
                "room_status": live_status, 
            }
            if current_follower_count > 0:
                update_fields["current_follower_count"] = current_follower_count
                room = await self.db['rooms'].find_one({"room_id": room_id}, {"start_follower_count": 1})
                if room:
                    start_count = room.get('start_follower_count', 0)
                    if start_count > 0:
                        update_fields["follower_diff"] = current_follower_count - start_count

            await self.db['rooms'].update_one({"room_id": room_id}, {"$set": update_fields})
        except PyMongoError as e:
            logger.error(f"❌ [DB] 更新实时数据失败: {e}")

    async def save_author_card(self, data: dict):
        if not data or not data.get('sec_uid'): return
        try:
            data['updated_at'] = datetime.now()
            await self.db['authors'].update_one(
                {"sec_uid": data['sec_uid']}, 
                {"$set": data},
                upsert=True
            )
        except PyMongoError as e:
            logger.error(f"❌ [DB] 保存主播资料失败: {e}")

    # --------------------------------------------------------------------------
    # 针对 Time Series 优化的写入逻辑
    # --------------------------------------------------------------------------

    async def insert_gift(self, data: dict):
        """
        异步保存礼物信息 (Redis 缓冲 + 批量写入时序集合)
        """
        if not data: return
        try:
            if isinstance(data.get('created_at'), str):
                try:
                    data['created_at'] = datetime.now() 
                except:
                    data['created_at'] = datetime.now()
            elif not data.get('created_at'):
                data['created_at'] = datetime.now()
            
            redis_client = get_redis()
            json_data = json.dumps(data, default=datetime_serializer)
            await redis_client.rpush(self.REDIS_GIFT_KEY, json_data)
            
            current_time = time.time()
            buffer_size = await redis_client.llen(self.REDIS_GIFT_KEY)
            
            if buffer_size >= self.BATCH_SIZE or (current_time - self.LAST_WRITE_TIME > self.BUFFER_TIMEOUT):
                await self.flush_gift_buffer()

        except Exception as e:
            logger.error(f"❌ [DB] 缓冲礼物失败: {e}")

    async def flush_gift_buffer(self):
        """刷新礼物缓冲区 -> live_gifts (TimeSeries) [安全版]"""
        try:
            redis_client = get_redis()
            BATCH_COUNT = 1000
            
            raw_data_list = await redis_client.lpop(self.REDIS_GIFT_KEY, count=BATCH_COUNT)
            
            if not raw_data_list:
                return

            current_batch = []
            for raw in raw_data_list:
                try:
                    data = json.loads(raw)
                    data = datetime_deserializer(data)
                    current_batch.append(data)
                except: pass
            
            if not current_batch:
                return
            
            try:
                await self.db[self.COL_GIFT].insert_many(current_batch, ordered=False)
                
                room_diamond_sum = {}
                for gift in current_batch:
                    room_id = gift.get('room_id')
                    diamond = gift.get('total_diamond_count', 0)
                    
                    if diamond == 0:
                        d = gift.get('diamond_count', 0)
                        c = gift.get('combo_count', 1)
                        g = gift.get('group_count', 1)
                        diamond = d * c * g
    
                    if room_id and diamond > 0:
                        room_diamond_sum[room_id] = room_diamond_sum.get(room_id, 0) + diamond
                
                for room_id, diamond_inc in room_diamond_sum.items():
                    await self.db['rooms'].update_one(
                        {"room_id": str(room_id)}, 
                        {
                            "$inc": {"total_diamond_count": diamond_inc},
                            "$set": {"updated_at": datetime.now()}
                        },
                        upsert=True
                    )

            except Exception as e:
                logger.error(f"❌ [DB] 批量写入礼物失败: {e}")
                if raw_data_list:
                     await redis_client.rpush(self.REDIS_GIFT_KEY, *raw_data_list)

        except Exception as e:
            logger.error(f"❌ [DB] 刷新礼物异常: {e}")

    async def insert_chat(self, data: dict):
        """
        异步保存弹幕信息 (Redis 缓冲 + 批量写入时序集合)
        """
        if not data: return
        try:
            if isinstance(data.get('created_at'), str) or not data.get('created_at'):
                data['created_at'] = datetime.now()

            redis_client = get_redis()
            json_data = json.dumps(data, default=datetime_serializer)
            await redis_client.rpush(self.REDIS_CHAT_KEY, json_data)
            
            current_time = time.time()
            buffer_size = await redis_client.llen(self.REDIS_CHAT_KEY)
            
            if buffer_size >= self.BATCH_SIZE or (current_time - self.LAST_WRITE_TIME > self.BUFFER_TIMEOUT):
                await self.flush_chat_buffer()
        except Exception as e:
            logger.error(f"❌ [DB] 缓冲弹幕失败: {e}")

    async def flush_chat_buffer(self):
        """刷新弹幕缓冲区 -> live_chats (TimeSeries)"""
        try:
            redis_client = get_redis()
            buffer_size = await redis_client.llen(self.REDIS_CHAT_KEY)
            if buffer_size == 0:
                return
        except RuntimeError as e:
            logger.warning(f"⚠️ [DB] Redis 不可用，跳过弹幕缓冲刷新: {e}")
            return
        except Exception as e:
            logger.error(f"❌ [DB] 检查 Redis 缓冲区失败: {e}")
            return
        
        self.LAST_WRITE_TIME = time.time()

        try:
            pipe = redis_client.pipeline()
            pipe.lrange(self.REDIS_CHAT_KEY, 0, -1)
            pipe.delete(self.REDIS_CHAT_KEY)
            results = await pipe.execute()
            
            raw_data_list = results[0]
            if not raw_data_list:
                return
            
            current_batch = []
            for raw in raw_data_list:
                try:
                    data = json.loads(raw)
                    data = datetime_deserializer(data)
                    current_batch.append(data)
                except json.JSONDecodeError as e:
                    logger.error(f"❌ [DB] JSON 解析失败: {e}")
            
            if not current_batch:
                return
            
            await self.db[self.COL_CHAT].insert_many(current_batch, ordered=False)
            
            room_chat_count = {}
            for chat in current_batch:
                room_id = chat.get('room_id')
                if room_id:
                    room_chat_count[room_id] = room_chat_count.get(room_id, 0) + 1
            
            for room_id, chat_inc in room_chat_count.items():
                await self.db['rooms'].update_one(
                    {"room_id": room_id},
                    {
                        "$inc": {"total_chat_count": chat_inc},
                        "$set": {"updated_at": datetime.now()}
                    },
                    upsert=True
                )
            
            logger.debug(f"📦 [DB] 已写入 {len(current_batch)} 条弹幕记录")
                
        except Exception as e:
            logger.error(f"❌ [DB] 刷新弹幕异常: {e}")

    async def update_room_stats(self, room_id, stats: dict):
        """更新房间状态 (仍保留，因为是更新 rooms 表)"""
        if not room_id or not stats: return
        try:
            update_fields = {"updated_at": datetime.now()}
            
            if 'user_count' in stats: update_fields['user_count'] = stats['user_count']
            if 'total_user' in stats: update_fields['total_user_count'] = stats['total_user']
            if 'like_count' in stats: update_fields['like_count'] = stats['like_count']
            if 'live_status' in stats: 
                update_fields['live_status'] = stats['live_status']
                update_fields['room_status'] = stats['live_status']
            if 'ranks' in stats: 
                update_fields['ranks'] = stats['ranks']

            pipeline = {"$set": update_fields}
            if 'user_count' in stats:
                pipeline["$max"] = {"max_viewers": stats['user_count']}
                
            await self.db['rooms'].update_one({"room_id": room_id}, pipeline, upsert=True)
        except PyMongoError:
            pass

    async def save_pk_result(self, pk_data: dict):
        if not pk_data: return
        try:
            await self.db['pk_history'].update_one(
                {
                    "battle_id": pk_data['battle_id'],
                    "room_id": pk_data['room_id']
                },
                {"$set": pk_data},
                upsert=True
            )
            logger.info(f"⚔️ [DB] PK数据已保存: {pk_data['battle_id']}")
        except PyMongoError as e:
            logger.error(f"❌ [DB] 保存PK数据失败: {e}")

    async def increment_room_stats(self, room_id: str, inc_data: dict):
        if not room_id or not inc_data: return
        try:
            await self.db['rooms'].update_one(
                {"room_id": room_id},
                {
                    "$inc": inc_data,
                    "$set": {"updated_at": datetime.now()}
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"❌ [DB] 递增统计失败: {e}")

    async def close(self):
        logger.info("💾 正在将 Redis 缓冲区数据写入 MongoDB...")
        await self.flush_chat_buffer()
        await self.flush_gift_buffer()
        # 移除了 flush_stat_buffer
        self.client.close()
        logger.info("👋 MongoDB 连接已关闭")

    async def get_room_live_status(self, room_id: str):
        """
        【新增】获取指定房间的当前数据库状态
        用于 main.py 判断是否需要重启录制
        """
        try:
            res = await self.db['rooms'].find_one(
                {"room_id": room_id}, 
                {"live_status": 1}
            )
            if res:
                return res.get('live_status', 0)
        except Exception:
            pass
        return 0

    async def get_all_cookies(self):
        """获取所有 Cookie"""
        cookies = []
        async for doc in self.db['settings_cookies'].find({}, {"_id": 0}):
            if doc.get('cookie'):
                cookies.append(doc['cookie'])
        return cookies

    async def add_cookie(self, cookie_str: str):
        """添加一个 Cookie"""
        if not cookie_str: return
        await self.db['sys_config'].update_one(
            {"key": "douyin_cookies"},
            {"$addToSet": {"cookies": cookie_str}},
            upsert=True
        )

    async def delete_cookie(self, cookie_str: str):
        """删除失效 Cookie"""
        if not cookie_str: return
        await self.db['settings_cookies'].delete_one({"cookie": cookie_str})
        logger.info(f"🗑️ [DB] 已移除失效 Cookie: {cookie_str[:20]}...")
    async def clear_zombie_rooms(self, timeout_seconds=180):
        """
        清理僵尸房间：
        将状态为直播中(1)但超时未更新的房间标记为结束。
        使用 updated_at 作为结束时间，更加精确。
        """
        try:
            # 计算超时阈值
            threshold_time = datetime.now() - timedelta(seconds=timeout_seconds)
            
            # 1. 查找条件：直播中 且 最后更新时间早于阈值
            query = {
                "live_status": 1,
                "updated_at": {"$lt": threshold_time}
            }
            
            # 2. 更新操作 (注意：这里是一个列表 []，这是 MongoDB 4.2+ 的聚合更新语法)
            # $updated_at 引用的是文档自身的字段值
            update_pipeline = [
                {
                    "$set": {
                        "live_status": 4,
                        "room_status": 4,
                        "end_time": "$updated_at",     # <--- 核心修改：使用该文档最后一次更新的时间
                        "end_reason": "zombie_cleanup" # 标记原因
                    }
                }
            ]
            
            result = await self.db['rooms'].update_many(query, update_pipeline)
            
            if result.modified_count > 0:
                logger.warning(f"🧟‍♂️ [DB] 清理了 {result.modified_count} 个僵尸直播间 (判定结束时间为最后活跃时刻)")
                
        except Exception as e:
            logger.error(f"❌ [DB] 清理僵尸房间失败: {e}")  
        
        
        