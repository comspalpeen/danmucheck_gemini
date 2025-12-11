# main.py
import asyncio
import os
import logging
import sys
from logging.handlers import RotatingFileHandler
import aiohttp
# 导入异步组件
from db import AsyncMongoDBHandler
from gift_deduplicator import AsyncGiftDeduplicator
from monitor import AsyncDouyinLiveMonitor
from liveMan import AsyncDouyinLiveWebFetcher
from redis_client import init_redis, close_redis

# --- 配置日志 ---
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file_path = os.path.join(log_dir, "monitor.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - [%(name)s]: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(
            filename=log_file_path, 
            mode='a', 
            maxBytes=10 * 1024 * 1024, 
            backupCount=5, 
            encoding='utf-8', 
            delay=False
        )
    ]
)
logger = logging.getLogger("Main")

# --- 全局任务字典 ---
# Key: web_rid, Value: asyncio.Task
recording_tasks = {}

async def settle_room(db, room_id, nickname):
    """【新增】封装结算逻辑"""
    if not room_id: return
    try:
        status = await db.get_room_live_status(room_id)
        if status == 4: return
        logger.info(f"🛑 [智能结算] 判定直播结束，正在结算: {nickname} ({room_id})")
        await db.set_room_ended(room_id)
    except Exception as e:
        logger.error(f"❌ 结算异常: {e}")

async def start_recorder_task(web_rid, nickname, start_follower_count, db, gift_processor, monitor_data=None, session=None):
    """单个直播间录制任务的包装器"""
    fetcher = None
    try:
        logger.info(f"🚀 [任务启动] {nickname} ({web_rid})")
        fetcher = AsyncDouyinLiveWebFetcher(
            live_id=web_rid,
            db=db,
            gift_processor=gift_processor,
            start_follower_count=start_follower_count,
            initial_state=monitor_data,
            session=session
        )
        await fetcher.start()
    except asyncio.CancelledError:
        logger.info(f"🛑 [任务取消] {nickname}")
    except Exception as e:
        logger.error(f"💥 [任务异常] {nickname}: {e}")
    finally:
        if fetcher: await fetcher.stop()
        logger.info(f"🏁 [任务结束] {nickname}")

async def main():
    # 1. 初始化数据库
    db = AsyncMongoDBHandler()
    await db.init_indexes()

    # 2. 初始化全局 Redis 连接
    await init_redis("redis://localhost:6379/0")

    # 3. 初始化礼物去重
    gift_processor = AsyncGiftDeduplicator(db_handler=db)
    gift_processor.start()

    # 设置全局 Session 超时
    timeout = aiohttp.ClientTimeout(total=15, connect=10)
    logger.info("🍪 正在从数据库加载 Cookie...")
    cookies = []
    # 查询 settings_cookies 集合，和 Admin 后台写入的集合保持一致
    async for doc in db.db['settings_cookies'].find({}, {"_id": 0, "cookie": 1}):
        if doc.get('cookie'):
            cookies.append(doc['cookie'])
    
    # 校验 Cookie 是否存在
    if not cookies:
        logger.error("❌ 数据库中没有 Cookie！请先访问 /admin 后台进行添加。")
        # 优雅退出，防止报错
        await db.close()
        await close_redis()
        return

    logger.info(f"✅ 成功加载 {len(cookies)} 个 Cookie，准备启动监控...")    
    # 【Session 上下文管理器】
    async with aiohttp.ClientSession(timeout=timeout) as shared_session:
        
        # 4. 初始化监控器 (传入 session)
        monitor = AsyncDouyinLiveMonitor(cookies, db, session=shared_session)

        logger.info("✅ 系统组件初始化完成，开始智能监控...")

        # 【注意】这里必须有一个 try 对应最后的 except
        try:
            while True:
                try:
                    # 1. 获取最新直播列表
                    live_users = await monitor.get_all_live_users()
                    
                    # 转为字典方便查找: {web_rid: user_info}
                    current_live_map = {u['web_rid']: u for u in live_users}

                    # --- 阶段 A: 清理已结束的任务 (只处理 WS 已经断开的) ---
                    for web_rid in list(recording_tasks.keys()):
                        task_info = recording_tasks[web_rid]
                        task = task_info['task']
                        old_room_id = task_info['room_id']
                        nickname = task_info['nickname']
                        
                        # 【原则】：Monitor 没权杀任务，只有任务自己结束(done)了，我们才处理
                        if not task.done():
                            continue

                        # ====================================================
                        # 代码运行到这里，说明 WS 已经断开了
                        # ====================================================
                        
                        # 1. 检查异常
                        if not task.cancelled() and task.exception():
                            logger.warning(f"💥 [异常断开] {nickname}: {task.exception()}")

                        # 2. 获取 Monitor 的最新情报
                        latest_info = current_live_map.get(web_rid)
                        
                        # 获取数据库里的最终状态
                        db_status = await db.get_room_live_status(old_room_id)

                        # --- 分支 1: 真正下播 ---
                        if db_status == 4 or not latest_info:
                            logger.info(f"👋 [确认下播] 任务自然结束: {nickname}")
                            await settle_room(db, old_room_id, nickname)
                            del recording_tasks[web_rid]
                            continue

                        # --- 分支 2: 换场 (Monitor 显示房间号变了) ---
                        new_room_id = str(latest_info.get('room_id'))
                        if new_room_id and new_room_id != old_room_id:
                            logger.info(f"🔄 [换场] 旧场结束，准备录制新场: {nickname}")
                            await settle_room(db, old_room_id, nickname)
                            del recording_tasks[web_rid]
                            continue

                        # --- 分支 3: 意外断开 (Monitor 显示还在播) ---
                        logger.warning(f"♻️ [闪断恢复] WS断开但Monitor显示在线，立即重启: {nickname}")
                        
                        new_task = asyncio.create_task(
                            start_recorder_task(
                                web_rid, nickname, 
                                latest_info.get('follower_count', 0), 
                                db, gift_processor, 
                                monitor_data=latest_info,
                                session=shared_session
                            )
                        )
                        recording_tasks[web_rid]['task'] = new_task

                    # --- 阶段 B: 检查新增直播 (启动新任务) ---
                    for web_rid, user_info in current_live_map.items():
                        
                        # ✅ 【新增】如果 monitor 没过滤干净，这里坚决不能放行
                        if not web_rid:
                            continue
                        if web_rid not in recording_tasks:
                            if user_info.get('live_status') == 1:
                                nickname = user_info.get('nickname')
                                room_id = str(user_info.get('room_id'))
                                
                                task = asyncio.create_task(
                                    start_recorder_task(
                                        web_rid, nickname, 
                                        user_info.get('follower_count', 0), 
                                        db, gift_processor, 
                                        monitor_data=user_info,
                                        session=shared_session
                                    )
                                )
                                recording_tasks[web_rid] = {
                                    "task": task,
                                    "room_id": room_id,
                                    "nickname": nickname
                                }

                    logger.info(f"💓 扫描完成: 在线{len(current_live_map)} | 录制中{len(recording_tasks)}")

                except Exception as e:
                    logger.error(f"❌ 主循环异常: {e}", exc_info=True)

                # 等待下一次扫描
                await asyncio.sleep(20)

        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("🛑 收到退出信号...")
        finally:
            # 清理工作
            if recording_tasks:
                logger.info("正在取消所有录制任务...")
                for t in recording_tasks.values():
                    if isinstance(t, dict): t['task'].cancel()
                    else: t.cancel()
                
                # 等待任务取消
                tasks = [t['task'] for t in recording_tasks.values() if isinstance(t, dict)]
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            await gift_processor.stop()
            await db.close()
            await close_redis()
            logger.info("👋 系统已完全退出")

if __name__ == "__main__":
    # Windows 下 Python 3.8+ 需要设置事件循环策略
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass # main() 内部已经处理了，这里防止外部报错