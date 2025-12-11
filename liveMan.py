# liveMan.py
import gzip
import logging
import asyncio
import aiohttp
import urllib.parse
import json
import time
from datetime import datetime, timedelta
from http.cookies import SimpleCookie

from protobuf.douyin import *
from liveMan_utils import (
    generateSignature, 
    generateMsToken, 
    get_safe_url, 
    get_ac_signature, 
    execute_js
)

from db import AsyncMongoDBHandler
from gift_deduplicator import AsyncGiftDeduplicator
from message_handler import MessageHandler  # 【新增导入】

logger = logging.getLogger("LiveMan")

class AsyncDouyinLiveWebFetcher:
    
    def __init__(self, live_id, db, gift_processor, start_follower_count=0, abogus_file='a_bogus.js', initial_state=None, session=None):

        self.live_id = live_id
        self.start_follower_count = start_follower_count
        self.abogus_file = abogus_file
        self.db = db
        self.gift_processor = gift_processor
        self.handler = None # 【新增】消息处理器实例
        self.initial_state = initial_state       
        
        self.session = None 
        self.ws = None      
        
        self.__ttwid = None
        self.current_room_id = None
        
        self.live_url = "https://live.douyin.com/"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        self.headers = {'User-Agent': self.user_agent}
        
        self.running = False
        self.session = session # 保存外部传入的 session
        self._own_session = False # 标记是否拥有 session 所有权
        
        if self.session is None:
            # 如果没传（兼容旧代码），就自己建一个
            self.session = aiohttp.ClientSession(headers=self.headers)
            self._own_session = True
    async def get_ttwid(self):
        """获取 ttwid"""
        if self.__ttwid: return self.__ttwid
        
        if self.session:
            for cookie in self.session.cookie_jar:
                if cookie.key == 'ttwid':
                    self.__ttwid = cookie.value
                    return self.__ttwid
        
        try:
            async with self.session.get(self.live_url, headers=self.headers) as resp:
                pass
            for cookie in self.session.cookie_jar:
                if cookie.key == 'ttwid':
                    self.__ttwid = cookie.value
                    return self.__ttwid
        except Exception as err:
            logger.error(f"【X】获取游客 ttwid 失败: {err}")
        return None

    def get_ac_nonce(self):
        import random
        chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return ''.join(random.choice(chars) for _ in range(21))
    
    def get_a_bogus(self, url_params: dict):
        url = urllib.parse.urlencode(url_params)
        ctx = execute_js(self.abogus_file)
        return ctx.call("get_ab", url, self.user_agent)

    async def get_room_status(self):
        try:
            ttwid = await self.get_ttwid()
            if not ttwid: pass # 尝试无ttwid继续

            msToken = generateMsToken()
            nonce = self.get_ac_nonce()
            signature = get_ac_signature(self.live_url[8:], nonce, self.user_agent)

            base_url = "https://live.douyin.com/webcast/room/web/enter/"
            params = {
                'aid': '6383',
                'app_name': 'douyin_web',
                'live_id': '1',
                'device_platform': 'web',
                'language': 'zh-CN',
                'enter_from': 'page_refresh',
                'cookie_enabled': 'true',
                'screen_width': '1920',
                'screen_height': '1080',
                'browser_language': 'zh-CN',
                'browser_platform': 'Win32',
                'browser_name': 'Edge',
                'browser_version': '120.0.0.0',
                'web_rid': self.live_id,
                'room_id_str': "",
                'enter_source': '',
                'is_need_double_stream': 'false',
                'insert_task_id': '',
                'live_reason': '',
                'msToken': msToken,
            }

            try:
                params['a_bogus'] = self.get_a_bogus(params)
            except Exception as e:
                logger.warning(f"⚠️ a_bogus 计算失败: {e}")

            headers = self.headers.copy()
            headers.update({'Referer': f'https://live.douyin.com/{self.live_id}'})
            
            req_cookies = {
                '__ac_nonce': nonce,
                '__ac_signature': signature,
                'msToken': msToken
            }

            async with self.session.get(base_url, params=params, headers=headers, cookies=req_cookies, timeout=10) as resp:
                text = await resp.text() 
                try:
                    json_data = json.loads(text)
                except json.JSONDecodeError:
                    return None
            
            data_core = json_data.get('data')
            if not data_core: return None

            room_data = None
            if isinstance(data_core.get('data'), list) and len(data_core.get('data')) > 0:
                room_data = data_core.get('data')[0]
            elif isinstance(data_core.get('user'), dict):
                room_data = data_core
            
            if not room_data: return None
            
            status = room_data.get('status')
            user = room_data.get('owner') or room_data.get("user")
            if not user: return None

            self.current_room_id = room_data.get('id_str')
            
            info = {
                'web_rid': self.live_id,
                'room_id': self.current_room_id,
                'title': room_data.get('title', ''),
                'user_id': user.get('id_str', ''),
                'sec_uid': user.get('sec_uid', ''),
                'nickname': user.get('nickname', '未知用户'),
                'avatar_url': get_safe_url(user.get('avatar_thumb')),
                'cover_url': get_safe_url(room_data.get('cover')),
                'user_count': room_data.get('user_count', 0), 
                'like_count': room_data.get('like_count', 0),
                'room_status': status,
                'live_status': 1,
                'start_follower_count': self.start_follower_count
            }
            logger.info(f"🟢 [LiveMan] 直播中 | 🏠 {info['nickname']}: {info['title']}")
            if self.db: await self.db.save_room_info(info)
            return info

        except Exception as e:
            logger.error(f"❌ 获取直播间状态异常: {e}")
            return None

    async def start(self):
        logger.info(f"🚀 启动抓取: {self.live_id}")
        self.running = True
        
        try:
            # --- 核心分支逻辑 ---
            
            # 分支 A: 极速模式（Monitor 已经给了 room_id）
            if self.initial_state and self.initial_state.get('room_id'):
                logger.info(f"⚡ [极速模式] 使用 Monitor 数据直接启动: {self.live_id}")
                self.current_room_id = self.initial_state['room_id']
                
                # 1. 构造临时数据并入库，确保存储有据可依
                temp_info = {
                    'web_rid': self.live_id,
                    'room_id': self.current_room_id,
                    # 优先用 Monitor 抓到的标题，没有则用昵称拼凑
                    'title': self.initial_state.get('title') or f"{self.initial_state.get('nickname', '主播')}正在直播",
                    'user_id': self.initial_state.get('uid', ''),
                    'sec_uid': self.initial_state.get('sec_uid', ''),
                    'nickname': self.initial_state.get('nickname', '未知用户'),
                    # 优先用 Monitor 抓到的封面，没有则用头像
                    'cover_url': self.initial_state.get('cover_url') or self.initial_state.get('avatar_url', ''),
                    'avatar_url': self.initial_state.get('avatar_url', ''),
                    'live_status': 1, # 强制标记为直播中
                    'start_follower_count': self.start_follower_count,
                    'created_at': datetime.now()
                }
                
                if self.db:
                    await self.db.save_room_info(temp_info)
                    
                # 2. 启动后台任务去慢慢获取高清详情
                asyncio.create_task(self._lazy_update_room_info())

            # 分支 B: 传统模式（没有 room_id，必须阻塞请求）
            else:
                room_info = await self.get_room_status()
                if not room_info:
                    logger.warning("⚠️ 等待 3秒 后重试...")
                    await asyncio.sleep(3)
                    room_info = await self.get_room_status()
                    
                if not room_info:
                    logger.error("❌ 无法获取房间信息，放弃录制")
                    return
                # get_room_status 内部已经设置了 self.current_room_id

            # --- 公共逻辑: 连接 WebSocket ---
            
            # 初始化消息处理器
            self.handler = MessageHandler(
                live_id=self.live_id,
                room_id=self.current_room_id,
                db=self.db,
                gift_processor=self.gift_processor
            )

            await self._connectWebSocket()
            
        except Exception as e:
            logger.error(f"❌ 录制任务异常退出: {e}")
        finally:
            await self.stop()

    async def stop(self):
        self.running = False
        if self.ws: await self.ws.close()
        
        # 【修改】只有自己创建的 session 才需要关闭
        # 共享的 session 由 main.py 负责关闭
        if self._own_session and self.session:
            await self.session.close()

    async def _sendHeartbeat(self, ws):
        while self.running and not ws.closed:
            try:
                heartbeat = PushFrame(payload_type='hb').SerializeToString()
                await ws.send_bytes(heartbeat) 
                await asyncio.sleep(10)
            except asyncio.CancelledError:  # <--- 新增：收到停止信号时直接退出循环
                break
            except Exception: 
                break

    async def _connectWebSocket(self):
        ttwid = await self.get_ttwid() or ""
        
        wss = ("wss://webcast100-ws-web-lq.douyin.com/webcast/im/push/v2/?app_name=douyin_web"
               "&version_code=180800&webcast_sdk_version=1.0.14-beta.0"
               "&update_version_code=1.0.14-beta.0&compress=gzip&device_platform=web&cookie_enabled=true"
               "&screen_width=1536&screen_height=864&browser_language=zh-CN&browser_platform=Win32"
               "&browser_name=Mozilla"
               "&browser_version=5.0%20(Windows%20NT%2010.0;%20Win64;%20x64)%20AppleWebKit/537.36%20(KHTML,"
               "%20like%20Gecko)%20Chrome/126.0.0.0%20Safari/537.36"
               "&browser_online=true&tz_name=Asia/Shanghai"
               f"&cursor=d-1_u-1_fh-7392091211001140287_t-1721106114633_r-1"
               f"&internal_ext=internal_src:dim|wss_push_room_id:{self.current_room_id}|wss_push_did:7319483754668557238"
               f"|first_req_ms:1721106114541|fetch_time:1721106114633|seq:1|wss_info:0-1721106114633-0-0|"
               f"wrds_v:7392094459690748497"
               f"&host=https://live.douyin.com&aid=6383&live_id=1&did_rule=3&endpoint=live_pc&support_wrds=1"
               f"&user_unique_id=7319483754668557238&im_path=/webcast/im/fetch/&identity=audience"
               f"&need_persist_msg_count=15&insert_task_id=&live_reason=&room_id={self.current_room_id}&heartbeatDuration=0")
        
        signature = generateSignature(wss)
        wss += f"&signature={signature}"
        
        headers = {
            "Cookie": f"ttwid={ttwid}",
            'User-Agent': self.user_agent,
        }

        try:
            # 【重点 1】捕获连接建立阶段的异常（如超时、DNS错误）
            async with self.session.ws_connect(wss, headers=headers, timeout=15) as ws:
                self.ws = ws
                logger.info("✅ WebSocket 连接成功")
                
                # 启动心跳任务
                hb_task = asyncio.create_task(self._sendHeartbeat(ws))
                
                try:
                    # 【重点 2】消息循环
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.BINARY:
                            await self._handle_binary_message(msg.data, ws)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning("⚠️ WebSocket 连接被动关闭")
                            break
                except Exception as e:
                    # 这里捕获的是 读取消息过程中的异常
                    logger.error(f"❌ 消息读取循环异常: {e}")
                    # 不需要 break，异常发生后会自动跳出 async for
                    
        except Exception as e:
            # 这里捕获的是 连接建立 或 整体流程 的异常
            logger.error(f"❌ WebSocket 连接/运行异常: {e}")
            
        finally:
            # 【重点 3】兜底清理：无论是因为 return、break 还是 Exception 退出，这里都会执行
            self.running = False # 确保 flag 关闭
            
            if hb_task:
                hb_task.cancel() # 停止心跳
                try:
                    await hb_task # 等待心跳协程真正结束
                except asyncio.CancelledError:
                    pass # 忽略取消异常

            if self.ws and not self.ws.closed:
                await self.ws.close() # 确保连接关闭
                
            logger.info(f"👋 [LiveMan] 录制任务结束/退出: {self.live_id}")

    async def _handle_binary_message(self, data, ws):
        try:
            package = PushFrame().parse(data)
            response = Response().parse(gzip.decompress(package.payload))
            
            if response.need_ack:
                ack = PushFrame(log_id=package.log_id, payload_type='ack',
                                payload=response.internal_ext.encode('utf-8')).SerializeToString()
                await ws.send_bytes(ack)
            
            for msg in response.messages_list:
                # 【修改】委托给 Handler 处理
                if self.handler:
                    is_ended = await self.handler.handle(msg.method, msg.payload)
                    if is_ended:
                        self.running = False
                        await ws.close()
                        break
        except Exception: 
            pass
    async def _lazy_update_room_info(self):
        """后台任务：尝试获取更详细的直播间信息（高清封面、准确标题等）"""
        logger.info(f"⏳ [LiveMan] 启动后台详情同步: {self.live_id}")
        try:
            # 尝试 3 次，每次间隔递增
            for i in range(5):
                if not self.running: break
                
                wait_time = 10 + (i * 5)
                await asyncio.sleep(wait_time)
                
                # 调用原有的获取逻辑，它内部会自动调用 db.save_room_info 更新数据库
                room_info = await self.get_room_status()
                
                if room_info:
                    logger.info(f"✨ [LiveMan] 详情页信息已同步: {room_info['title']}")
                    break # 获取成功，退出重试
        except Exception as e:
            logger.warning(f"⚠️ 后台同步详情失败: {e}")