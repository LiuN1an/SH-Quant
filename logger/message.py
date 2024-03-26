from typing import Dict, Callable
import requests
import json
import multiprocessing
import time
from config import FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_APP_GROUP_ID


def getTemp():
    try:
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = json.dumps(
            {
                "app_id": FEISHU_APP_ID,
                "app_secret": FEISHU_APP_SECRET,
            }
        )
        headers = {"Content-Type": "application/json"}
        response = requests.request("POST", url, headers=headers, data=payload)
        dic = json.loads(response.text)
        return dic["tenant_access_token"]
    except Exception as e:
        pass


group_id = FEISHU_APP_GROUP_ID


class Throttle:
    def __init__(self, delay: float, func: Callable):
        self.delay = delay
        self.func = func
        self.last_call_time = 0

    def __call__(self, *args, **kwargs):
        current_time = time.time()
        if current_time - self.last_call_time >= self.delay:
            self.last_call_time = current_time
            self.func(*args, **kwargs)


def sendTextToFeishu(msg: str, token: str):
    url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
    payload = json.dumps(
        {
            "receive_id": group_id,
            "msg_type": "text",
            "content": msg,
            "uuid": "",
        }
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    rsp = requests.request("POST", url, headers=headers, data=payload)
    rsp_content = json.loads(rsp.text)
    if rsp_content.get("code", None) == "99991663":
        return getTemp()


throttle_send = Throttle(0.5, sendTextToFeishu)


class Messager:
    def __init__(self, is_print=True) -> None:
        self.is_print = is_print
        if self.is_print:
            self.queue = multiprocessing.Queue()
            self.process = multiprocessing.Process(target=self._send)
            self.token = getTemp()

    def start(self):
        if self.is_print:
            self.process.start()

    def send(self, data: Dict):
        if self.is_print:
            self.queue.put(json.dumps(data))

    def _send(self):
        while True:
            msg = self.queue.get()
            token = sendTextToFeishu(msg, self.token)
            if token is not None:
                self.token = token


messager = Messager()


def start():
    messager.start()
