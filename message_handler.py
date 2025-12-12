# message_handler.py
import time
import logging
from datetime import datetime, timedelta
from protobuf.douyin import *
from liveMan_utils import get_safe_url

logger = logging.getLogger("MsgHandler")

class MessageHandler:
    def __init__(self, live_id, room_id, db, gift_processor):
        self.live_id = live_id
        self.room_id = room_id
        self.db = db
        self.gift_processor = gift_processor
        self.last_seq_state = None       
        # --- 频率控制状态 ---
        self.last_like_time = 0
        self.last_seq_time = 0
        self.THROTTLE_INTERVAL = 2 # 120秒限制

    async def handle(self, method, payload):
        """
        统一消息分发入口
        Returns:
            bool: True 表示收到下播信号(ControlMessage=3)，建议断开连接；否则 False
        """
        try:
            if method == 'WebcastChatMessage':
                await self._parse_chat(payload)
            elif method == 'WebcastGiftMessage':
                await self._parse_gift(payload)
            elif method == 'WebcastRoomUserSeqMessage':
                await self._parse_user_seq(payload)
            elif method == 'WebcastLikeMessage':
                await self._parse_like(payload)
            elif method == 'WebcastControlMessage':
                return await self._parse_control(payload)
            elif method == 'WebcastLinkMicBattleFinishMethod':
                await self._parse_pk_finish(payload)
        except Exception as e:
            # 单个消息解析失败不应影响整体
            logger.debug(f"⚠️ 消息解析异常 [{method}]: {e}")
        
        return False

    async def _parse_control(self, payload):
        try:
            message = ControlMessage().parse(payload)
            if message.status == 3:
                logger.info(f"🛑 [ControlMsg] 收到下播信号 (Room: {self.room_id})")
                if self.db and self.room_id:
                    await self.db.set_room_ended(self.room_id)
                return True # Signal to stop
        except Exception: pass
        return False

    async def _parse_chat(self, payload):
        try:
            message = ChatMessage().parse(payload)
            user = message.user
            
            # --- 新增：消费等级 ---
            pay_grade = 0
            pay_grade_icon = ""
            try:
                if hasattr(user, 'pay_grade'):
                    pay_grade = user.pay_grade.level
                    pay_grade_icon = get_safe_url(user.pay_grade.new_im_icon_with_level)
            except: pass
            
            # --- 粉丝团信息 ---
            fans_club_icon = ""
            fans_club_level = 0
            try:
                fans_club_icon = user.fans_club.data.badge.icons[4].url_list_list[0]
                fans_club_level = user.fans_club.data.level
            except: pass
            
            event_ts = getattr(message, 'event_time', 0)
            
            if event_ts == 0:
                event_time_obj = datetime.now()
            else:
                # 加上8小时转为北京时间
                event_time_obj = datetime.utcfromtimestamp(event_ts) + timedelta(hours=8)
            
            event_time_str = event_time_obj.strftime('%Y-%m-%d %H:%M:%S')
            
            chat_data = {
                'web_rid': self.live_id,
                'room_id': self.room_id,
                'user_id': str(user.id),
                'user_name': user.nick_name,
                'gender': getattr(user, 'gender', 0),
                'content': message.content,
                'sec_uid': getattr(user, 'sec_uid', ''),
                'avatar_url': get_safe_url(user.avatar_thumb),
                'pay_grade': pay_grade,          # ✅ 新增
                'pay_grade_icon': pay_grade_icon,
                'fans_club_icon': fans_club_icon,
                'fans_club_level': fans_club_level,
                'event_time': event_time_str,
                'created_at': datetime.now()
            }
            if self.db: 
                await self.db.insert_chat(chat_data)
        except Exception: pass

    async def _parse_gift(self, payload):
        try:
            message = GiftMessage().parse(payload)
            user = message.user
            gift = message.gift
            group_id = getattr(message, 'group_id', '')          
            group_count = message.group_count
            
            # --- 新增：消费等级 ---
            pay_grade = 0
            pay_grade_icon = ""
            try:
                if hasattr(user, 'pay_grade'):
                    pay_grade = user.pay_grade.level
                    pay_grade_icon = get_safe_url(user.pay_grade.new_im_icon_with_level)
            except: pass
            
            # --- 新增：粉丝团等级 ---
            fans_club_icon = ""
            fans_club_level = 0
            try:
                fans_club_icon = user.fans_club.data.badge.icons[4].url_list_list[0]
                fans_club_level = user.fans_club.data.level
            except: pass
            
            gift_icon_url = ""
            try:
                gift_icon_url = message.gift.icon.url_list_list[0]
            except: pass
            
            send_time_ms = getattr(message, 'send_time', int(time.time() * 1000))
            
            if send_time_ms == 0:
                send_time_obj = datetime.now()
            else:
                send_time_obj = datetime.utcfromtimestamp(send_time_ms / 1000) + timedelta(hours=8)
            
            # 格式化时间字符串
            send_time_str = send_time_obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            gift_data = {
                'web_rid': self.live_id,
                'room_id': self.room_id,
                'user_id': str(user.id),
                'user_name': user.nick_name,
                'gender': getattr(user, 'gender', 0),
                'sec_uid': getattr(user, 'sec_uid', ''),
                'avatar_url': get_safe_url(user.avatar_thumb),
                'pay_grade': pay_grade,          # ✅ 新增
                'pay_grade_icon': pay_grade_icon,
                'fans_club_level': fans_club_level, # ✅ 新增
                'fans_club_icon': fans_club_icon,
                'gift_icon_url' : gift_icon_url,
                'gift_id': str(gift.id),
                'gift_name': gift.name,
                'diamond_count': gift.diamond_count,
                'combo_count': message.combo_count,
                'group_count': group_count,
                'group_id': str(group_id),
                'repeat_end': getattr(message, 'repeat_end', 0),
                'trace_id': getattr(message, 'trace_id', ''),
                'send_time': send_time_str,
                'created_at': datetime.now()
            }
            
            if self.gift_processor: 
                await self.gift_processor.process_gift(gift_data)
        except Exception: pass

    async def _parse_user_seq(self, payload):
        """
        直播间统计信息（在线人数、榜单）
        """
        now = time.time()
        # 频率控制
        if now - self.last_seq_time < self.THROTTLE_INTERVAL:
            return
        
        # 计算实际的时间间隔 (可能不是精确的5.0秒，用实际差值更准)
        time_diff = now - self.last_seq_time if self.last_seq_time > 0 else 0
        self.last_seq_time = now

        try:
            message = RoomUserSeqMessage().parse(payload)
            
            # 当前值
            current_online = message.total       # 当前在线
            current_total = message.total_user   # 累计观看
            
            stats = {
                'user_count': current_online,
                'total_user': current_total
            }

            # --- 【新增】核心计算逻辑 ---
            inc_data = {}
            
            if self.last_seq_state:
                prev_online = self.last_seq_state['online']
                prev_total = self.last_seq_state['total']
                
                # 1. 计算进场 (Entries)
                new_entries = current_total - prev_total
                if new_entries < 0: new_entries = 0 # 异常数据防护

                # 2. 计算离场 (Exits)
                # 离场 = 进场 - 在线净增量
                net_growth = current_online - prev_online
                new_exits = new_entries - net_growth
                if new_exits < 0: new_exits = 0 # 异常数据防护
                
                # 3. 计算产生的总观看时长 (Total Watch Time Increment)
                # 近似计算：当前在线人数 * 过去了多少秒
                # 更精确的积分算法是：(上刻人数 + 这刻人数) / 2 * 时间间隔
                # 这里采用简单的高频近似：
                duration_inc = current_online * time_diff

                # 准备更新到数据库的增量数据
                inc_data = {
                    'real_time_entries': new_entries, # 这一瞬间进场
                    'real_time_exits': new_exits,     # 这一瞬间离场
                    'total_watch_time_sec': duration_inc # 累计总时长(秒)
                }
                
                # 记录一下调试日志
                # logger.info(f"📊 5s流转: 进+{new_entries} 离-{new_exits} | 累计时长+{duration_inc:.1f}s")

            # 更新状态供下一次使用
            self.last_seq_state = {
                'online': current_online,
                'total': current_total,
                'time': now
            }

            # 榜单解析 (保持原有逻辑)
            ranks_source = getattr(message, 'ranks_list', getattr(message, 'ranksList', []))
            if ranks_source:
                rank_data = []
                for item in ranks_source:
                    user = item.user
                    if not user: continue
                    rank_data.append({
                        "uid": str(user.id),
                        "nickname": user.nick_name,
                        "avatar": get_safe_url(user.avatar_thumb),
                        "rank": item.rank,
                    })
                stats['ranks'] = rank_data

            if self.db and self.room_id:
                # 1. 更新覆盖型数据 (在线人数、榜单) -> rooms 表
                await self.db.update_room_stats(self.room_id, stats)
                
                # 2. 更新增量数据 (累计时长等) -> rooms 表
                if inc_data:
                    await self.db.increment_room_stats(self.room_id, inc_data)

                # ❌ 已移除：写入 live_stats 时序集合的操作

        except Exception as e:
            logger.error(f"⚠️ 解析UserSeq异常: {e}")

    async def _parse_like(self, payload):
        """
        点赞信息
        【频率控制】120s 一次
        """
        now = time.time()
        if now - self.last_like_time < self.THROTTLE_INTERVAL:
            return
        self.last_like_time = now

        try:
            message = LikeMessage().parse(payload)
            if self.db and self.room_id:
                # logger.info(f"❤️ [Like] 更新点赞数: {message.total}")
                await self.db.update_room_stats(self.room_id, {
                    'like_count': message.total
                })
        except Exception: pass

    async def _parse_pk_finish(self, payload):
        try:
            message = LinkMicBattleFinishMethod().parse(payload)
            if message.info.status != 2: return

            battle_id = str(message.info.battle_id)
            start_time = message.info.start_time_ms
            
            scores_map = {}
            has_valid_win_status = False
            
            for s in message.scores:
                uid = str(s.user_id)
                win_status = s.win_status
                if win_status in [1, 2]: has_valid_win_status = True
                scores_map[uid] = {"score": s.score, "win_status": win_status, "rank": s.rank}

            contrib_map = {}
            for c_group in message.contributors:
                anchor_id = str(c_group.anchor_id)
                top_list = []
                for item in c_group.list[:3]: 
                    top_list.append({
                        "user_id": str(item.id),
                        "nickname": item.nickname,
                        "avatar": get_safe_url(item.avatar),
                        "score": item.score,
                        "rank": item.rank if item.rank else 0
                    })
                contrib_map[anchor_id] = top_list

            total_anchors = 0
            for army in message.anchors: total_anchors += len(army.list)

            mode_type = "free_for_all"
            if has_valid_win_status: mode_type = "team_battle"
            elif total_anchors == 2: mode_type = "team_battle"

            teams_map = {} 
            for army in message.anchors:
                for anchor_item in army.list:
                    if not anchor_item.user: continue
                    uid = str(anchor_item.user.id)
                    score_info = scores_map.get(uid, {})
                    contributors = contrib_map.get(uid, [])
                    
                    anchor_data = {
                        "user_id": uid,
                        "nickname": anchor_item.user.nickname,
                        "avatar": get_safe_url(anchor_item.user.avatar_thumb),
                        "score": score_info.get("score", 0),
                        "rank": score_info.get("rank", 0),
                        "contributors": contributors
                    }

                    if has_valid_win_status:
                        team_id = str(score_info.get("win_status"))
                    else:
                        team_id = uid

                    if team_id not in teams_map:
                        teams_map[team_id] = {"team_id": team_id, "win_status": score_info.get("win_status", 0), "anchors": []}
                    teams_map[team_id]["anchors"].append(anchor_data)

            final_teams = list(teams_map.values())
            if mode_type == "free_for_all":
                final_teams.sort(key=lambda t: t["anchors"][0]["rank"] if t["anchors"] else 999)

            pk_result = {
                "battle_id": battle_id,
                "room_id": self.room_id,
                "start_time": datetime.fromtimestamp(start_time / 1000) if start_time else datetime.now(),
                "mode": mode_type, 
                "created_at": datetime.now(),
                "teams": final_teams
            }

            logger.info(f"🏁 [PK结算] ID:{battle_id} | 模式:{mode_type}")
            
            if self.db:
                await self.db.save_pk_result(pk_result)

        except Exception as e:
            logger.error(f"❌ 解析PK结算消息异常: {e}")