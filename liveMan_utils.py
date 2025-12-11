# liveMan_utils.py
import codecs
import hashlib
import random
import string
import subprocess
import urllib.parse
from unittest.mock import patch
from contextlib import contextmanager
import execjs
from py_mini_racer import MiniRacer
# 假设 ac_signature 在项目目录下，保持原样导入
from ac_signature import get__ac_signature
import logging

logger = logging.getLogger("Utils")

def execute_js(js_file: str):
    with open(js_file, 'r', encoding='utf-8') as file:
        js_code = file.read()
    ctx = execjs.compile(js_code)
    return ctx

@contextmanager
def patched_popen_encoding(encoding='utf-8'):
    original_popen_init = subprocess.Popen.__init__
    def new_popen_init(self, *args, **kwargs):
        kwargs['encoding'] = encoding
        original_popen_init(self, *args, **kwargs)
    with patch.object(subprocess.Popen, '__init__', new_popen_init):
        yield

def generateSignature(wss, script_file='sign.js'):
    params = ("live_id,aid,version_code,webcast_sdk_version,"
              "room_id,sub_room_id,sub_channel_id,did_rule,"
              "user_unique_id,device_platform,device_type,ac,"
              "identity").split(',')
    wss_params = urllib.parse.urlparse(wss).query.split('&')
    wss_maps = {i.split('=')[0]: i.split("=")[-1] for i in wss_params}
    tpl_params = [f"{i}={wss_maps.get(i, '')}" for i in params]
    param = ','.join(tpl_params)
    
    md5 = hashlib.md5()
    md5.update(param.encode())
    md5_param = md5.hexdigest()
    
    with codecs.open(script_file, 'r', encoding='utf8') as f:
        script = f.read()
    
    # 注意: MiniRacer 是同步的，如果并发极高可能会轻微阻塞 EventLoop
    # 但签名生成频率低，通常可以接受。
    ctx = MiniRacer()
    ctx.eval(script)
    try:
        signature = ctx.call("get_sign", md5_param)
        return signature
    except Exception as e:
        logger.error(f"签名生成失败: {e}")
        return ""

def generateMsToken(length=182):
    random_str = ''
    base_str = string.ascii_letters + string.digits + '-_'
    _len = len(base_str) - 1
    for _ in range(length):
        random_str += base_str[random.randint(0, _len)]
    return random_str

def get_safe_url(icon_obj):
    try:
        # 兼容字典访问和对象属性访问
        if isinstance(icon_obj, dict):
            url_list = icon_obj.get('url_list_list') or icon_obj.get('url_list')
            if url_list: return url_list[0]
        else:
            if hasattr(icon_obj, 'url_list_list') and icon_obj.url_list_list:
                return icon_obj.url_list_list[0]
            elif hasattr(icon_obj, 'url_list') and icon_obj.url_list:
                 return icon_obj.url_list[0]
    except:
        pass
    return ""

def get_ac_signature(host_part, nonce, user_agent):
    """
    包装原始的 ac_signature 调用
    """
    try:
        return get__ac_signature(host_part, nonce, user_agent)
    except Exception as e:
        logger.error(f"ac_signature 计算错误: {e}")
        return ""