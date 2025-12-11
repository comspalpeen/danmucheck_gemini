# gift_deduplicator.py
import asyncio
import time
import logging
from collections import OrderedDict
from redis_client import get_redis  # 使用全局 Redis 客户端

logger = logging.getLogger("GiftDeduplicator")

class AsyncGiftDeduplicator:
    # 【修改】增加 max_buffer_size 参数，默认 10000 条
    def __init__(self, db_handler, timeout_seconds=10, max_buffer_size=10000):
        """
        礼物去重处理器
        :param db_handler: 数据库处理器
        :param timeout_seconds: 大礼物缓冲超时时间
        :param max_buffer_size: 【新增】缓冲区最大容量
        """
        self.db = db_handler
        self.timeout = timeout_seconds
        self.max_buffer_size = max_buffer_size  # 【新增】保存上限配置
        
        # --- 核心缓冲区 (Strategy C) ---
        # 【修改】必须显式使用 OrderedDict 以支持 FIFO 淘汰
        self.buffer = OrderedDict()
        
        # --- L1 本地去重缓存 ---
        self.local_history = OrderedDict()
        self.LOCAL_HISTORY_SIZE = 1000 
        
        # 钻石礼物价格修正配置 (保持不变)
        self.DIAMOND_OVERRIDES = { 
            "钻石火箭": 12001, "钻石嘉年华": 36000, "钻石兔兔": 360, "钻石飞艇": 23333,
            "钻石秘境": 16000, "钻石游轮": 7200, "钻石飞机": 3600, "钻石跑车": 1500, "钻石热气球": 620, "钻石邮轮": 7200
        }

        self.lock = asyncio.Lock()
        self.running = False
        self.cleaner_task = None

    def start(self):
        self.running = True
        self.cleaner_task = asyncio.create_task(self._cleanup_loop())
        logger.info(f"✅ [Async] 礼物处理器启动 (BufferSize: {self.max_buffer_size})")

    def _get_unique_key(self, data):
        uid = data.get('user_id', 'unknown')
        gid = data.get('gift_id', 'unknown')
        group_id = data.get('group_id', '0')
        return f"{uid}_{gid}_{group_id}"

    async def _is_duplicate(self, trace_id, combo, repeat_end):
        """
        混合去重逻辑：本地缓存 -> Redis
        """
        fingerprint = f"{trace_id}_{combo}_{repeat_end}"
        
        # 1. L1 本地快速检查 (内存级速度)
        if fingerprint in self.local_history:
            return True
            
        # 2. L2 Redis 权威检查
        # key 格式: gift_dedup:{trace_id}_{combo}_{repeat_end}
        redis_key = f"dedup:gift:{fingerprint}"
        redis_client = get_redis()  # 获取全局 Redis 客户端
        
        try:
            # SET key value NX EX 600
            # NX: 只有键不存在时才设置 (原子操作)
            # EX: 10分钟后过期 (自动释放 Redis 内存)
            is_new = await redis_client.set(redis_key, 1, nx=True, ex=600)
            
            if not is_new:
                # Redis 返回 None/False，说明 Key 已存在 -> 是重复包
                # 顺便写入本地缓存，拦截后续的快速重试
                self.local_history[fingerprint] = True
                if len(self.local_history) > self.LOCAL_HISTORY_SIZE:
                    self.local_history.popitem(last=False)
                return True
                
            # 是新包
            return False
            
        except Exception as e:
            logger.error(f"⚠️ Redis 连接异常，降级通过: {e}")
            return False # 异常时为了不丢数据，默认不过滤

    async def process_gift(self, gift_data):
        trace_id = gift_data.get('trace_id', '')
        repeat_end = gift_data.get('repeat_end', 0)
        combo = gift_data.get('combo_count', 1)
        gift_name = gift_data.get('gift_name', '')
        gift_id = str(gift_data.get('gift_id', ''))
        room_id = gift_data.get('room_id')
        
        diamond_count = gift_data.get('diamond_count', 0)
        group_count = gift_data.get('group_count', 1)

        # --- 1. 特殊礼物：粉丝团灯牌 (不过滤，直接统计) ---
        if gift_id == "685" or "灯牌" in gift_name:
            if self.db:
                # 【修复】构造增量数据，同时增加灯牌数和钻石数
                inc_data = {"fans_ticket_count": 1}
                
                # 如果灯牌有价值（通常是1钻），也加上
                if diamond_count > 0:
                    inc_data["total_diamond_count"] = diamond_count
                
                await self.db.increment_room_stats(room_id, inc_data)
            return  # 继续保持 return，不存入 live_gifts 集合

        # --- 2. Redis 去重检查 ---
        # 如果 trace_id 为空，无法去重，只能放行
        if trace_id and await self._is_duplicate(trace_id, combo, repeat_end):
            return 

        # --- 3. 价格修正逻辑 ---
        if "钻石" in gift_name and gift_name in self.DIAMOND_OVERRIDES:
            corrected_price = self.DIAMOND_OVERRIDES[gift_name]
            diamond_count = corrected_price
            gift_data['diamond_count'] = corrected_price
        elif gift_name == "跑车":
            icon_url = gift_data.get('gift_icon_url', '')
            if "diamond_paoche_icon.png" in icon_url:
                corrected_price = 1500
                diamond_count = corrected_price     # 更新局部变量，确保后续策略B/C生效
                gift_data['diamond_count'] = corrected_price # 更新写入DB的数据
        # --- 策略B: 小礼物直接写入 (<60钻) ---
        if diamond_count < 60:
            if repeat_end == 0:
                return 
            else:
                total = diamond_count * group_count * int(combo)
                gift_data['total_diamond_count'] = total
                if int(combo) > 0 and self.db:
                    await self.db.insert_gift(gift_data)
                return

        # --- 策略C: 大礼物缓冲聚合 (>=60钻) ---
        # 这部分逻辑保持在内存中，因为是高频的 update 操作，
        # 如果把聚合逻辑也放到 Redis，网络 RTT 会成为瓶颈。
        key = self._get_unique_key(gift_data)
        current_time = time.time()

        async with self.lock:
            # Case 1: Key 已存在，直接更新（不增加 buffer 长度）
            if key in self.buffer:
                cached_item = self.buffer[key]
                if int(combo) > cached_item['max_combo']:
                    cached_item['max_combo'] = int(combo)
                    cached_item['combo_count'] = int(combo)
                if group_count > cached_item.get('group_count', 1):
                    cached_item['group_count'] = group_count
                
                cached_item['last_update_time'] = current_time
                # 将更新过的项目移到末尾（表示最近活跃），方便 LRU/FIFO 逻辑
                self.buffer.move_to_end(key)

                if repeat_end == 1:
                    cached_item['repeat_end'] = 1
                    cached_item['_force_flush'] = True 
            else:
                # 【新增】缓冲区溢出保护 (FIFO 淘汰)
                if len(self.buffer) >= self.max_buffer_size:
                    # 弹出最早插入（或最久未更新）的一个元素
                    evicted_key, evicted_item = self.buffer.popitem(last=False)
                    # 立即将该元素写入 DB
                    await self._flush_single_data_direct(evicted_item)
                    # 记录日志（可选，调试用，生产环境可去掉以减少IO）
                    # logger.warning(f"⚠️ Buffer已满({self.max_buffer_size})，强制驱逐: {evicted_key}")

                # 正常插入新元素
                gift_data['last_update_time'] = current_time
                gift_data['max_combo'] = int(combo)
                gift_data['combo_count'] = int(combo)
                gift_data['group_count'] = group_count
                gift_data['diamond_count'] = diamond_count
                self.buffer[key] = gift_data
    async def _flush_item(self, key):
        data_to_write = None
        async with self.lock:
            if key in self.buffer:
                data_to_write = self.buffer.pop(key)

        if data_to_write and self.db:
            # 清理辅助字段
            for field in ['last_update_time', 'max_combo', '_force_flush']:
                data_to_write.pop(field, None)
            
            unit_price = data_to_write.get('diamond_count', 0)
            group_count = data_to_write.get('group_count', 1)
            combo_count = data_to_write.get('combo_count', 1)
            
            data_to_write['total_diamond_count'] = unit_price * group_count * combo_count

            if combo_count > 0:
                await self.db.insert_gift(data_to_write)
    async def _flush_single_data_direct(self, data_to_write):
        if not self.db or not data_to_write: return
        try:
            # 清理辅助字段
            for field in ['last_update_time', 'max_combo', '_force_flush']:
                data_to_write.pop(field, None)
            
            unit_price = data_to_write.get('diamond_count', 0)
            group_count = data_to_write.get('group_count', 1)
            combo_count = data_to_write.get('combo_count', 1)
            
            data_to_write['total_diamond_count'] = unit_price * group_count * combo_count
            
            if combo_count > 0:
                # 调用 DB 的 insert_gift (它会将数据放入 Redis Queue，非常快)
                await self.db.insert_gift(data_to_write)
        except Exception as e:
            logger.error(f"❌ 强制写入失败: {e}")
    async def _cleanup_loop(self):
        while self.running:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            
            current_time = time.time()
            keys_to_flush = []

            async with self.lock:
                for key, item in self.buffer.items():
                    last_update = item.get('last_update_time', 0)
                    is_forced = item.get('_force_flush', False)
                    if is_forced or (current_time - last_update > self.timeout):
                        keys_to_flush.append(key)
            
            for key in keys_to_flush:
                await self._flush_item(key)

    async def stop(self):
        self.running = False
        if self.cleaner_task:
            self.cleaner_task.cancel()
            try:
                await self.cleaner_task
            except asyncio.CancelledError:
                pass
        
        # 强制刷新缓冲区
        logger.info(f"🛑 [Async] 正在保存剩余 {len(self.buffer)} 组大礼物...")
        async with self.lock:
            keys = list(self.buffer.keys())
        if keys and self.db:
            # 并发写入加速退场
            await asyncio.gather(*[self._flush_item(key) for key in keys])