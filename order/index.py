import inspect
import queue
from datetime import datetime, timedelta
from typing import Callable, Dict
from openctp_ctp import tdapi, mdapi
from utils.local import LocalStore
from utils.base import plus, subtract
from logger.op import OpLog
from logger.running import RunningLog
from utils.ref import reset, increaseCondition, g_id, resetId, resetCondition
from collections import namedtuple


class CTP(tdapi.CThostFtdcTraderSpi):
    def __init__(self, local: LocalStore, unit: float):
        super().__init__()
        self.init()
        self.is_real = True
        self.today = datetime.now().strftime("%Y%m%d")
        self.OpLogger: OpLog = local["op_logger"]
        self.RunLogger: RunningLog = local["run_logger"]
        self.local = local
        self.last_price = None
        self.unit = unit

    def setToday(self, today):
        self.today = today

    def init(self):
        self.callbacks = {}  # 每一笔仓位的回调函数
        self.condition = []  # 本地监听盘口的价格队列
        self.position_info = {}  # 所有的仓位信息管理
        self.position_id_list = []  # 仓位id的时序排序
        resetId()
        resetCondition()

    def unreal(self):
        self.is_real = False
        self.match = []

    def setReal(self):
        print(len(self.condition), "位于观察队列中")
        print(len(self.match), "位于成交队列中")
        if len(self.match) != 0:
            print(self.match)
            old_cb = self.match[-1]["callback"]

            def newCB(data):
                old_cb(data)
                print("成交已完成, 启动实盘模式")
                self.is_real = True

            self.match[-1]["callback"] = newCB
            for conditon in self.condition:
                old_conditon = conditon["triggerCallback"]

                def newConditon(data):
                    old_conditon(data)
                    print("条件已达成, 启动实盘模式")
                    self.is_real = True

                conditon["triggerCallback"] = newConditon
        else:
            self.is_real = True

    def recover(self):
        # TODO: 本地存储恢复
        pass

    def real(
        self,
        front: str,
        user: str,
        passwd: str,
        authcode: str,
        appid: str,
        broker_id: str,
        exchange_id: str,
        instrument_id: str,
    ):
        self._front = front
        self._user = user
        self.exchange_id = exchange_id
        self.instrument_id = instrument_id
        self._password = passwd
        self._authcode = authcode
        self._appid = appid
        self._broker_id = broker_id
        self._is_authenticate = False
        self._is_login = False
        self._is_last = True
        self._print_max = 5
        self._print_count = 0
        self._total = 0
        self._wait_queue = queue.Queue(1)
        self._api: tdapi.CThostFtdcTraderApi = (
            tdapi.CThostFtdcTraderApi.CreateFtdcTraderApi(self._user)
        )
        print("CTP交易API版本号:", self._api.GetApiVersion())
        print("交易前置:" + self._front)
        # 注册交易前置
        self._api.RegisterFront(self._front)
        # 注册交易回调实例
        self._api.RegisterSpi(self)
        # 订阅私有流
        self._api.SubscribePrivateTopic(tdapi.THOST_TERT_QUICK)
        # 订阅公有流
        self._api.SubscribePublicTopic(tdapi.THOST_TERT_QUICK)
        # 初始化交易实例
        self._api.Init()
        print("初始化成功")

    @property
    def is_login(self):
        return self._is_login

    def release(self):
        # 释放实例
        self._api.Release()

    def _check_req(self, req, ret: int):
        """检查请求"""
        # 打印请求
        params = []
        for name, value in inspect.getmembers(req):
            if name[0].isupper():
                params.append(f"{name}={value}")
        self.print("发送请求:", ",".join(params))
        # 检查请求结果
        error = {
            0: "",
            -1: "网络连接失败",
            -2: "未处理请求超过许可数",
            -3: "每秒发送请求数超过许可数",
        }.get(ret, "未知错误")
        if ret != 0:
            self.OpLogger.print(f"请求失败: {ret}={error}")
            self.print(f"请求失败: {ret}={error}")

    def _check_rsp(
        self, pRspInfo: tdapi.CThostFtdcRspInfoField, rsp=None, is_last: bool = True
    ) -> bool:
        if self._is_last:
            if pRspInfo and pRspInfo.ErrorID != 0:
                print(pRspInfo.ErrorMsg)
                self.OpLogger.print(
                    f"响应失败, ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}"
                )
                self.print(
                    f"响应失败, ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}"
                )
                return False
            self.print(f"响应成功")
            if rsp:
                params = []
                for name, value in inspect.getmembers(rsp):
                    if name[0].isupper():
                        params.append(f"{name}={value}")
                self.print("响应内容:", ",".join(params))
            if not is_last:
                self._print_count += 1
                self._total = +1
            else:
                if self._is_login:
                    self._wait_queue.put_nowait(None)
        else:
            if self._print_count < self._print_max:
                if rsp:
                    params = []
                    for name, value in inspect.getmembers(rsp):
                        if name[0].isupper():
                            params.append(f"{name}={value}")
                    self.print("     ", ",".join(params))
                self._print_count += 1
            self._total += 1
            if is_last:
                self.print("总计数量:", self._total, "打印数量:", self._print_count)
                self._print_count = 0
                self._total = 0
                if self._is_login:
                    self._wait_queue.put_nowait(None)
        self._is_last = is_last
        return True

    @staticmethod
    def print_rsp_rtn(prefix, rsp_rtn):
        if rsp_rtn:
            params = []
            for name, value in inspect.getmembers(rsp_rtn):
                if name[0].isupper():
                    params.append(f"{name}={value}")
            print(">", prefix, ",".join(params))

    @staticmethod
    def print(*args, **kwargs):
        print("    ", *args, **kwargs)

    def OnFrontConnected(self):
        """交易前置连接成功"""
        print("交易前置连接成功")
        self.authenticate()

    def OnFrontDisconnected(self, nReason: int):
        """交易前置连接断开"""
        print("交易前置连接断开: nReason=", nReason)

    def authenticate(self):
        """认证 demo"""
        print("> 认证")
        _req = tdapi.CThostFtdcReqAuthenticateField()
        _req.BrokerID = self._broker_id
        _req.UserID = self._user
        _req.AppID = self._appid
        _req.AuthCode = self._authcode
        self._check_req(_req, self._api.ReqAuthenticate(_req, 0))

    def OnRspAuthenticate(
        self,
        pRspAuthenticateField: tdapi.CThostFtdcRspAuthenticateField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        """客户端认证响应"""
        if not self._check_rsp(pRspInfo, pRspAuthenticateField):
            return
        self._is_authenticate = True
        # 登录
        self.login()

    def login(self):
        """登录 demo"""
        print("> 登录")
        _req = tdapi.CThostFtdcReqUserLoginField()
        _req.BrokerID = self._broker_id
        _req.UserID = self._user
        _req.Password = self._password
        self._check_req(_req, self._api.ReqUserLogin(_req, 0))

    def OnRspUserLogin(
        self,
        pRspUserLogin: tdapi.CThostFtdcRspUserLoginField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        """登录响应"""
        if not self._check_rsp(pRspInfo, pRspUserLogin):
            return
        reset(pRspUserLogin.MaxOrderRef)
        self._is_login = True
        self._front_id = pRspUserLogin.FrontID
        self._session_id = pRspUserLogin.SessionID

    def settlement_info_confirm(self):
        """投资者结算结果确认"""
        print("> 投资者结算结果确认")
        _req = tdapi.CThostFtdcSettlementInfoConfirmField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        self._check_req(_req, self._api.ReqSettlementInfoConfirm(_req, 0))

    def OnRspSettlementInfoConfirm(
        self,
        pSettlementInfoConfirm: tdapi.CThostFtdcSettlementInfoConfirmField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        """投资者结算结果确认响应"""
        if not self._check_rsp(pRspInfo, pSettlementInfoConfirm):
            return

    def OnRspOrderAction(
        self,
        orderAction: tdapi.CThostFtdcOrderActionField,
        info: tdapi.CThostFtdcRspInfoField,
    ):
        """报单录入请求响应"""
        print("报单操作: ", orderAction.StatusMsg, info.ErrorMsg)

    def OnRspOrderInsert(
        self,
        pInputOrder: tdapi.CThostFtdcInputOrderField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        """报单录入请求响应"""
        if not self._check_rsp(pRspInfo, pInputOrder, bIsLast):
            return

    def OnErrRtnOrderAction(self, resp: tdapi.CThostFtdcErrOrderActionField):
        print("撤单： ", resp.StatusMsg)

    def wait(self):
        # 阻塞 等待
        self._wait_queue.get()
        input("--------------- 等待按键退出 --------------- ")
        print("\n")
        self.release()

    def OnRtnOrder(self, pOrder: tdapi.CThostFtdcOrderField):
        """报单通知，当执行ReqOrderInsert后并且报出后，收到返回则调用此接口，私有流回报。"""
        # self.OpLogger.print(
        #     f"OnRtnOrder 状态信息:{pOrder.StatusMsg}, 状态码:{pOrder.OrderStatus}, 时间: {pOrder.InsertTime}"
        # )
        # d = {
        #     "price": pOrder.LimitPrice,
        #     "volume": pOrder.VolumeTraded,
        #     "direction": pOrder.Direction,
        #     "id": pOrder.OrderRef,
        #     "time": pOrder.InsertTime,
        #     "status": pOrder.OrderStatus,
        # }
        if pOrder.OrderRef in self.callbacks:
            if pOrder.OrderRef in self.position_info:
                if (
                    self.position_info[pOrder.OrderRef]
                    .get("status", "")
                    .startswith("with_draw")
                ):
                    self.position_info[pOrder.OrderRef]["status"] = "with_draw_complete"
                else:
                    self.position_info[pOrder.OrderRef][
                        "status"
                    ] = f"RtnOrder: {pOrder.StatusMsg}"
                if self.is_real:
                    self.RunLogger.print(self.position_info)
            else:
                self.OpLogger.print(
                    f"RtnOrder仓位信息丢失: ref是{pOrder.OrderRef} 时间: {pOrder.InsertTime}"
                )
            time: str = pOrder.InsertTime
            if not time.startswith(self.today[0:4]):
                time = f"{self.today} {time}"
            self.callbacks[pOrder.OrderRef](
                "OnRtnOrder",
                {
                    "price": pOrder.LimitPrice,
                    "volume": pOrder.VolumeTraded,
                    "direction": pOrder.Direction,
                    "id": pOrder.OrderRef,
                    "time": time,
                    "status": pOrder.OrderStatus,
                },
            )
        else:
            self.OpLogger.print(
                f"RtnOrder回调函数丢失: ref是{pOrder.OrderRef} 价格是{pOrder.LimitPrice}, 成交量是{pOrder.VolumeTotal}, TradeTime是{pOrder.InsertTime}"
            )

    def OnRtnTrade(self, pTrade: tdapi.CThostFtdcTradeField):
        """成交通知，报单发出后有成交则通过此接口返回。私有流"""
        # self.OpLogger.print(f"OnRtnTrade id是{pTrade.OrderRef}, 已成交, 时间: {pTrade.TradeTime}")
        if pTrade.OrderRef in self.callbacks:
            if pTrade.OrderRef in self.position_info:
                self.position_info[pTrade.OrderRef]["status"] = "RtnTrade"
                self.position_info[pTrade.OrderRef]["real_price"] = pTrade.Price
                self.position_info[pTrade.OrderRef]["real_volume"] = pTrade.Volume
                self.position_info[pTrade.OrderRef]["real_time"] = pTrade.TradeTime
                if self.is_real:
                    self.RunLogger.print(self.position_info)
            else:
                self.OpLogger.print(
                    f"RtnTrade仓位信息丢失: ref是{pTrade.OrderRef}  价格是{pTrade.Price}, 成交量是{pTrade.Volume}, TradeTime是{pTrade.TradeTime}"
                )
                return  # 这里是为了防止callback函数中进行后续一系列副作用操作
            time: str = pTrade.TradeTime
            if not time.startswith(self.today[0:4]):
                time = f"{self.today} {time}"
            self.callbacks[pTrade.OrderRef](
                "OnRtnTrade",
                {
                    "price": pTrade.Price,
                    "volume": pTrade.Volume,
                    "direction": pTrade.Direction,
                    "id": pTrade.OrderRef,
                    "time": time,
                    "relative_open_ref": self.position_info[pTrade.OrderRef].get(
                        "relative_open_ref", None
                    ),
                },
            )
        else:
            self.OpLogger.print(
                f"RtnTrade回调函数丢失: ref是{pTrade.OrderRef}  价格是{pTrade.Price}, 成交量是{pTrade.Volume}, TradeTime是{pTrade.TradeTime}"
            )

    def watch(
        self,
        price: float,
        compare: str,
        callback: Callable,
        time: str = "",
        extra: Dict = {},
    ):
        id = increaseCondition()

        def cb(data):
            for condition in self.condition:
                if condition["id"] == id:
                    condition["locked"] = True
            callback(data)
            self.condition = list(filter(lambda x: x["id"] != id, self.condition))

        self.condition.append(
            {
                "id": id,
                "trigger_price": price,
                "trigger_compare": compare,
                "triggerCallback": cb,
                "time": time,
                **extra,
            }
        )
        return id

    def open(
        self,
        price: float,
        direction: str,
        volume: int,
        callback: Callable,
        time: str = "",
        behavior: str = "",
        pos_extra: Dict = {},
    ):
        trade_id = g_id()
        self.callbacks[trade_id] = callback
        self.position_info[trade_id] = {
            "id": trade_id,
            "price": price,
            "trigger_price": price,
            "direction": direction,
            "volume": volume,
            "behavior": "open",
            "status": "init",
            "time": time,
            **pos_extra,
        }
        self.position_id_list.append(trade_id)
        if self.is_real:
            self.OpLogger.print(
                f"下开仓单(立即)请求中: {price} {direction}... 时间: {time} 仓位: {trade_id}"
            )
            _req = tdapi.CThostFtdcInputOrderField()
            _req.BrokerID = self._broker_id
            _req.InvestorID = self._user
            _req.ExchangeID = self.exchange_id
            _req.InstrumentID = self.instrument_id
            _req.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice
            _req.LimitPrice = price
            _req.VolumeTotalOriginal = volume
            _req.MinVolume = volume
            _req.IsAutoSuspend = 0
            _req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Open
            _req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
            _req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
            if direction == "long":
                _req.Direction = tdapi.THOST_FTDC_D_Buy
            else:
                _req.Direction = tdapi.THOST_FTDC_D_Sell
            _req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
            _req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
            _req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
            _req.OrderRef = trade_id
            self._check_req(_req, self._api.ReqOrderInsert(_req, 0))
            self.RunLogger.print(self.position_info)
        else:
            self.OpLogger.print(
                f"下开仓单(立即)请求中: {price} {direction}... 时间: {time} 仓位: {trade_id}"
            )

            def unrealCallback(data):
                nonlocal trade_id
                self.match = [
                    item for item in self.match if item.get("id", None) != trade_id
                ]
                Data = namedtuple(
                    "Data",
                    ["OrderRef", "Price", "Volume", "Direction", "TradeTime"],
                )
                self.OpLogger.print(f"{data['price'] }已成交 时间: {data['time']}")
                self.OnRtnTrade(
                    Data(
                        OrderRef=trade_id,
                        Price=data["price"],
                        Volume=volume,
                        Direction=direction,
                        TradeTime=data["time"],
                    )
                )

            Data = namedtuple(
                "Data",
                [
                    "StatusMsg",
                    "OrderStatus",
                    "LimitPrice",
                    "VolumeTraded",
                    "Direction",
                    "OrderRef",
                    "InsertTime",
                ],
            )
            self.OnRtnOrder(
                Data(
                    StatusMsg="未成交",
                    OrderStatus=3,
                    LimitPrice=price,
                    VolumeTraded=volume,
                    Direction=direction,
                    OrderRef=trade_id,
                    InsertTime=time,
                )
            )

            self.match.append(
                {
                    "id": trade_id,
                    "direction": direction,
                    "price": price,
                    "volume": volume,
                    "type": "open",
                    "callback": unrealCallback,
                    "time": time,
                    "behavior": behavior,
                    "rest": self.local["open_orders_rank"],
                }
            )
        return trade_id

    def anyClose(
        self,
        direction: str,
        volume: int,
        callback: Callable,
        relative_open_ref: str,
        time: str = "",
        behavior: str = "",
        pos_extra: Dict = {},
    ):
        trade_id = g_id()
        self.callbacks[trade_id] = callback
        self.position_info[trade_id] = {
            "id": trade_id,
            "price": "any",
            "direction": direction,
            "volume": volume,
            "behavior": "close",
            "status": "init",
            "relative_open_ref": relative_open_ref,
            "time": time,
            **pos_extra,
        }
        self.position_id_list.append(trade_id)
        if self.is_real:
            _req = tdapi.CThostFtdcInputOrderField()
            _req.BrokerID = self._broker_id
            _req.InvestorID = self._user
            _req.ExchangeID = self.exchange_id
            _req.InstrumentID = self.instrument_id
            _req.OrderPriceType = tdapi.THOST_FTDC_OPT_AnyPrice
            _req.LimitPrice = 0
            _req.VolumeTotalOriginal = volume
            _req.MinVolume = volume
            _req.IsAutoSuspend = 0
            _req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Close
            _req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
            _req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
            if direction == "long":
                _req.Direction = tdapi.THOST_FTDC_D_Sell
            else:
                _req.Direction = tdapi.THOST_FTDC_D_Buy
            _req.TimeCondition = tdapi.THOST_FTDC_TC_IOC
            _req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
            _req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
            _req.OrderRef = trade_id
            self._check_req(_req, self._api.ReqOrderInsert(_req, 0))
            self.RunLogger.print(self.position_info)
        else:

            def unrealCallback(data):
                nonlocal trade_id, direction
                self.match = [
                    item for item in self.match if item.get("id", None) != trade_id
                ]
                Data = namedtuple(
                    "Data",
                    ["OrderRef", "Price", "Volume", "Direction", "TradeTime"],
                )
                self.OnRtnTrade(
                    Data(
                        OrderRef=trade_id,
                        Price=data["price"],
                        Volume=volume,
                        Direction=direction,
                        TradeTime=data["time"],
                    )
                )

            Data = namedtuple(
                "Data",
                [
                    "StatusMsg",
                    "OrderStatus",
                    "LimitPrice",
                    "VolumeTraded",
                    "Direction",
                    "OrderRef",
                    "InsertTime",
                ],
            )
            self.OnRtnOrder(
                Data(
                    StatusMsg="未成交",
                    OrderStatus=3,
                    LimitPrice="any",
                    VolumeTraded=volume,
                    Direction=direction,
                    OrderRef=trade_id,
                    InsertTime=time,
                )
            )
            self.match.append(
                {
                    "id": trade_id,
                    "direction": direction,
                    "price": "any",
                    "volume": volume,
                    "type": "close",
                    "relative_open_ref": relative_open_ref,
                    "callback": unrealCallback,
                    "time": time,
                    "behavior": behavior,
                    "rest": self.local["close_orders_rank"],
                }
            )

    def close(
        self,
        price: float,
        direction: str,
        volume: int,
        callback: Callable,
        relative_open_ref: str,
        time: str = "",
        behavior: str = "",
        pos_extra: Dict = {},
    ):
        trade_id = g_id()
        self.callbacks[trade_id] = callback
        self.position_info[trade_id] = {
            "id": trade_id,
            "trigger_price": price,
            "price": price,
            "direction": direction,
            "volume": volume,
            "behavior": "close",
            "status": "init",
            "relative_open_ref": relative_open_ref,
            "time": time,
            **pos_extra,
        }
        self.position_id_list.append(trade_id)
        if self.is_real:
            # 这个没有盘口观察
            _req = tdapi.CThostFtdcInputOrderField()
            _req.BrokerID = self._broker_id
            _req.InvestorID = self._user
            _req.ExchangeID = self.exchange_id
            _req.InstrumentID = self.instrument_id
            _req.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice
            _req.LimitPrice = price
            # _req.StopPrice = price
            _req.VolumeTotalOriginal = volume
            _req.MinVolume = volume
            _req.IsAutoSuspend = 0
            _req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Close
            _req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
            _req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
            if direction == "long":
                _req.Direction = tdapi.THOST_FTDC_D_Sell
                # _req.ContingentCondition = (
                #     tdapi.THOST_FTDC_CC_LastPriceLesserEqualStopPrice
                # )
            else:
                _req.Direction = tdapi.THOST_FTDC_D_Buy
                # _req.ContingentCondition = (
                #     tdapi.THOST_FTDC_CC_LastPriceGreaterEqualStopPrice
                # )
            _req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
            _req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
            _req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
            _req.OrderRef = trade_id
            self._check_req(_req, self._api.ReqOrderInsert(_req, 0))
            self.RunLogger.print(self.position_info)
        else:
            # self.OpLogger.print(f"直接平仓单请求中, 主要是考虑止盈提早下入, 等待成交")
            def unrealCallback(data):
                nonlocal trade_id, direction
                self.match = [
                    item for item in self.match if item.get("id", None) != trade_id
                ]
                Data = namedtuple(
                    "Data",
                    ["OrderRef", "Price", "Volume", "Direction", "TradeTime"],
                )
                self.OnRtnTrade(
                    Data(
                        OrderRef=trade_id,
                        Price=data["price"],
                        Volume=volume,
                        Direction=direction,
                        TradeTime=data["time"],
                    )
                )

            Data = namedtuple(
                "Data",
                [
                    "StatusMsg",
                    "OrderStatus",
                    "LimitPrice",
                    "VolumeTraded",
                    "Direction",
                    "OrderRef",
                    "InsertTime",
                ],
            )
            self.OnRtnOrder(
                Data(
                    StatusMsg="未成交",
                    OrderStatus=3,
                    LimitPrice=price,
                    VolumeTraded=volume,
                    Direction=direction,
                    OrderRef=trade_id,
                    InsertTime=time,
                )
            )
            self.match.append(
                {
                    "id": trade_id,
                    "direction": direction,
                    "price": price,
                    "volume": volume,
                    "type": "close",
                    "relative_open_ref": relative_open_ref,
                    "callback": unrealCallback,
                    "time": time,
                    "behavior": behavior,
                    "rest": self.local["close_orders_rank"],
                }
            )

        return trade_id

    def watchWithDraw(
        self,
        id: str,
        callback: Callable,
        trigger_price: float = None,
        trigger_compare: str = ">",
        beforeRequest: Callable = None,
        time: str = "",
    ):
        position = self.position_info.get(id, None)
        if position:
            if (
                position.get("status", None) == "with_draw_loading"
                or position.get("status", None) == "with_draw_complete"
            ):
                # 防止重复撤单, 因为这个观察撤单可能会调用多次
                return

            condition_id = increaseCondition()

            def triggerCallback(data):
                nonlocal beforeRequest, callback
                if beforeRequest is not None:
                    beforeRequest({**data})
                for i in range(len(self.condition)):
                    if (
                        self.condition[i].get("relative_open_ref", None) == id
                        and self.condition[i].get("type", None) == "withdraw"
                    ):
                        self.condition[i]["locked"] = True
                self.position_info[id]["status"] = "with_draw_loading"
                self.callbacks[id] = callback
                price = position["price"]
                behavior = position["behavior"]
                direction = position["direction"]
                volume = position["volume"]
                if self.is_real:
                    self.OpLogger.print(
                        f"下撤单请求中: 观察撤单价格是{trigger_price} 撤的那个单的价格是{price} 行为是{behavior} 方向是{direction} 成交量是{volume} 时间: {data['time']} 仓位: {id}"
                    )
                    _req = tdapi.CThostFtdcInputOrderActionField()
                    _req.BrokerID = self._broker_id
                    _req.InvestorID = self._user
                    _req.ExchangeID = self.exchange_id
                    _req.InstrumentID = self.instrument_id
                    _req.ActionFlag = tdapi.THOST_FTDC_AF_Delete
                    _req.FrontID = self._front_id
                    _req.SessionID = self._session_id
                    # _req.OrderActionRef = id
                    _req.OrderRef = id
                    try:
                        self._check_req(_req, self._api.ReqOrderAction(_req, 0))
                        self.RunLogger.print(self.position_info)
                    except Exception as e:
                        self.OpLogger.print(f"撤单失败 时间:{data['time']}")
                else:
                    self.OpLogger.print(
                        f"下撤单请求中: 观察撤单价格是{trigger_price} 撤的那个单的价格是{price} 行为是{behavior} 方向是{direction} 成交量是{volume} 时间: {data['time']} 仓位: {id}"
                    )
                    self.position_info[id]["status"] = "with_draw_complete"
                    # 假设撤单立马成交
                    self.match = [
                        item for item in self.match if item.get("id", None) != id
                    ]
                    callback("withdraw", data)

            if trigger_price is None:
                # 立即撤单
                triggerCallback({"time": time})
            else:
                self.OpLogger.print(
                    f"撤单观察 突破{trigger_price}价格是撤单id是{id}的单 时间: {time}"
                )
                old_count = len(self.condition)
                self.condition.append(
                    {
                        "id": condition_id,
                        "trigger_price": trigger_price,
                        "triggerCallback": triggerCallback,
                        "relative_open_ref": id,
                        "type": "withdraw",
                        "trigger_compare": trigger_compare,
                        "time": time,
                    }
                )
                # self.OpLogger.print(
                #     f"condition数量 {old_count} -> {len(self.condition)} 时间: {time}"
                # )
                return condition_id
        else:
            self.OpLogger.print(f"撤单时，原仓位丢失 id是{id} 时间: {time}")

    def withDraw(self, id: str, callback: Callable, time: str = ""):
        position = self.position_info.get(id, None)
        if position:
            if (
                position.get("status", None) == "with_draw_loading"
                or position.get("status", None) == "with_draw_complete"
            ):
                # 防止重复撤单, 因为这个观察撤单可能会调用多次
                return

            self.position_info[id]["status"] = "with_draw_loading"
            self.callbacks[id] = callback
            price = position["price"]
            behavior = position["behavior"]
            direction = position["direction"]
            volume = position["volume"]
            if self.is_real:
                self.OpLogger.print(
                    f"下撤单(立即)请求中: 撤的那个单的价格是{price} 行为是{behavior} 方向是{direction} 成交量是{volume} 时间: {time} 仓位: {id}"
                )
                _req = tdapi.CThostFtdcInputOrderActionField()
                _req.BrokerID = self._broker_id
                _req.InvestorID = self._user
                _req.ExchangeID = self.exchange_id
                _req.InstrumentID = self.instrument_id
                _req.ActionFlag = tdapi.THOST_FTDC_AF_Delete
                _req.FrontID = self._front_id
                _req.SessionID = self._session_id
                # _req.OrderActionRef = id
                _req.OrderRef = id
                try:
                    self._check_req(_req, self._api.ReqOrderAction(_req, 0))
                    self.RunLogger.print(self.position_info)
                except Exception as e:
                    self.OpLogger.print(f"撤单失败 时间:{time}")
            else:
                # 撤单立马成交
                self.OpLogger.print(
                    f"下撤单(立即)请求中: 撤的那个单的价格是{price} 行为是{behavior} 方向是{direction} 成交量是{volume} 时间: {time} 仓位: {id}"
                )
                self.match = [item for item in self.match if item.get("id", None) != id]
                self.position_info[id]["status"] = "with_draw_complete"
                callback("withdraw", {"time": time, "status": 5})

    # 该取消监控行为发生在开启了价格观察但是尚未开仓
    def removeTriggerById(self, id: str, time: str = ""):
        # for i in range(len(self.condition)):
        #     if self.condition[i].get("id", None) == id:
        #         condition = self.condition[i]
        #         price = condition.get("price", None)
        #         direction = condition.get("direction", None)
        #         volume = condition.get("volume", None)
        #         trigger_price = condition.get("trigger_price", None)
        #         self.OpLogger.print(
        #             f"取消监听 突破价格是{trigger_price} 下单价格是{price} 方向是{direction} 成交量是{volume} 时间:{time}"
        #         )
        old_count = len(self.condition)
        self.condition = [
            item
            for item in self.condition
            if str(item.get("id", None)) != str(id)
            or item.get("type", None) == "virtual"
        ]
        # self.OpLogger.print(f"condition数量 {old_count} -> {len(self.condition)} 时间:{time}")

    # 该取消监控行为发生在任意价格成交后
    def removeTriggerByRef(self, ref: str, time: str = ""):
        # for i in range(len(self.condition)):
        #     if self.condition[i].get("ref", None) == ref:
        #         condition = self.condition[i]
        #         price = condition.get("price", None)
        #         direction = condition.get("direction", None)
        #         volume = condition.get("volume", None)
        #         trigger_price = condition.get("trigger_price", None)
        #         self.OpLogger.print(
        #             f"取消监听 监听价格是{trigger_price} 下单价格是{price} 方向是{direction} 成交量是{volume} 时间:{time}"
        #         )
        old_count = len(self.condition)
        self.condition = [
            item
            for item in self.condition
            if str(item.get("ref", None)) != str(ref)
            or item.get("type", None) == "virtual"
        ]
        # self.OpLogger.print(
        #     f"ByRef condition数量 {old_count} -> {len(self.condition)} 时间:{time}"
        # )

    # 该取消监控行为发生在止损/止盈达到后需要把另一笔也杀掉
    def removeTriggerByRelativeOpenRef(self, relative_open_ref: str, time: str = ""):
        # for i in range(len(self.condition)):
        #     if str(self.condition[i].get("relative_open_ref", None)) == str(
        #         relative_open_ref
        #     ):
        #         condition = self.condition[i]
        #         price = condition.get("price", None)
        #         direction = condition.get("direction", None)
        #         volume = condition.get("volume", None)
        #         trigger_price = condition.get("trigger_price", None)
        #         self.OpLogger.print(
        #             f"取消监听 监听价格是{trigger_price} 下单价格是{price} 方向是{direction} 成交量是{volume} 时间:{time}"
        #         )
        old_count = len(self.condition)
        self.condition = [
            item
            for item in self.condition
            if str(item.get("relative_open_ref", None)) != str(relative_open_ref)
            or item.get("type", None) == "virtual"
        ]
        # self.OpLogger.print(
        #     f"ByRelativeOpenRef condition数量 {old_count} -> {len(self.condition)} {list(map(lambda x:[x.get('relative_open_ref', None),x['trigger_price']], self.condition))} 时间:{time}"
        # )

    def removeTriggerByWithDraw(self, relative_open_ref: str, time: str = ""):
        # for i in range(len(self.condition)):
        #     if (
        #         str(self.condition[i].get("relative_open_ref", None))
        #         == str(relative_open_ref)
        #         and self.condition[i].get("type", None) == "withdraw"
        #     ):
        #         condition = self.condition[i]
        #         trigger_price = condition.get("trigger_price", None)
        #         self.OpLogger.print(f"取消撤单监听, 撤单监听价格是{trigger_price} 时间:{time}")
        old_count = len(self.condition)
        self.condition = [
            item
            for item in self.condition
            if str(item.get("relative_open_ref", None)) != str(relative_open_ref)
            or item.get("type", None) != "withdraw"
            or item.get("type", None) == "virtual"
        ]
        # self.OpLogger.print(
        #     f"ByWithDraw condition数量 {old_count} -> {len(self.condition)} 时间:{time}"
        # )

    def removeTriggerByVirtual(self):
        self.condition = [
            item for item in self.condition if item.get("type", None) != "virtual"
        ]

    def removeAllTrigger(self):
        self.condition = []

    def prob(self, data):
        if len(self.condition) > 0:
            condition_copy = self.condition.copy()
            for condition in condition_copy:
                if condition.get("locked", False):
                    # 被锁住时不参与盘口监听
                    continue
                price = condition["trigger_price"]
                cb = condition["triggerCallback"]
                compare = condition.get("trigger_compare", None)
                # debug = condition.get("debug", False)
                # if debug:
                #     print(data["price"], price)
                if compare == ">":
                    if data["price"] >= price:
                        # if debug:
                        #     print(">= finish")
                        cb(data)
                if compare == "<":
                    if data["price"] <= price:
                        # if debug:
                        #     print("<= finish")
                        cb(data)
                if compare == "=":
                    if data["price"] == price:
                        # if debug:
                        #     print("== finish")
                        cb(data)

        if self.is_real is False:
            # 在这里模拟交易所的撮合并调用OnRtnOrder和OnRtnTrade响应
            if len(self.match) > 0:
                match_copy = self.match.copy()
                for match in match_copy:
                    price = match["price"]
                    cb = match["callback"]
                    behavior = match.get("behavior", "")
                    if price == "any":
                        if behavior == "buy":
                            cb(
                                {
                                    "price": plus(data["price"] + self.unit),
                                    "time": data["time"],
                                }
                            )
                        elif behavior == "sell":
                            cb(
                                {
                                    "price": subtract(data["price"] - self.unit),
                                    "time": data["time"],
                                }
                            )
                    else:
                        current_price = data["price"]
                        is_scan = False
                        if self.last_price is not None:
                            if behavior == "buy":
                                if (
                                    self.last_price["price"] >= price
                                    and current_price < price
                                ):
                                    cb(
                                        {
                                            "price": price,
                                            "time": self.last_price["time"],
                                        }
                                    )
                                    is_scan = True

                            if behavior == "sell":
                                if (
                                    self.last_price["price"] <= price
                                    and current_price > price
                                ):
                                    cb(
                                        {
                                            "price": price,
                                            "time": self.last_price["time"],
                                        }
                                    )
                                    is_scan = True

                        if not is_scan:
                            if data["price"] == price:
                                match["rest"] -= data["volume"]
                                if match["rest"] <= 0:
                                    cb(data)
                                # else:
                                #     match_time = datetime.strptime(
                                #         match["time"], "%Y%m%d %H:%M:%S"
                                #     )
                                #     current_time = datetime.strptime(
                                #         data["time"], "%Y%m%d %H:%M:%S"
                                #     )
                                #     assume_with_draw_count = (
                                #         math.floor(
                                #             (current_time - match_time).seconds / 60
                                #         )
                                #         * 10
                                #     )
                                #     if match["rest"] - assume_with_draw_count <= 0:
                                #         cb(data)
            self.last_price = data

    # bar级别的监听
    def probBar(self, data):
        # 暂时不考虑同一根k内开仓,止盈或止损同时存在的情况
        if data["k"] is not None:
            [open, _, high, low] = data["k"]
            if len(self.condition) > 0:
                condition_copy = self.condition.copy()

                for condition in condition_copy:
                    if condition.get("locked", False):
                        # 被锁住时不参与盘口监听
                        continue
                    price = condition["trigger_price"]
                    cb = condition["triggerCallback"]
                    compare = condition.get("trigger_compare", None)
                    if price <= high and price >= low:
                        cb({"price": price, "time": data["time"]})
                    elif compare == "<":
                        if price > high:
                            cb({"price": price, "time": data["time"]})
                    elif compare == ">":
                        if price < low:
                            cb({"price": price, "time": data["time"]})

            if len(self.match) > 0:
                match_copy = self.match.copy()
                for match in match_copy:
                    price = match["price"]
                    # time = match["time"]
                    cb = match["callback"]
                    behavior = match.get("behavior", "")
                    type = match.get("type", "")
                    if price <= high and price >= low:
                        cb({"price": price, "time": data["time"]})
                    else:
                        cb({"price": open, "time": data["time"]})

    def stats(self):
        return self.position_info

    def lastPos(self):
        keys = list(self.position_info.keys())
        if len(keys) > 0:
            return self.position_info[keys[-1]]

    def getLastPosIsUnComplete(self):
        # 获取上一个没有闭合的仓, 场景发生在下一笔信号已经启动, 但是上一笔仓位因为止盈/止损未成交导致没有关闭
        if len(self.position_id_list) == 0:
            return [False, None]
        pos = self.position_info[self.position_id_list[-1]].copy()
        if pos.get("behavior", None) == "open":
            if pos.get("status").startswith("with_draw"):
                return [False, None]
            else:
                # self.OpLogger.print("出大问题, 在下一笔信号出现的时候, 上一笔开仓居然都没成交, 证明虚拟仓信号出问题了")
                return [True, pos]

        if pos.get("behavior", None) == "close":
            # 其实只要执行到这里了,就证明价格一定打到了止盈/止损了,只是没成交而已
            status = pos.get("status", None)
            if status != "RtnTrade":
                return [True, pos]
            else:
                return [False, None]

    def getProfitByRef(self, ref: str):
        if ref in self.position_info:
            pos = self.position_info[ref]
            price = pos["price"]
            direction = pos["direction"]
            for t_id in list(self.position_info.keys()):
                item = self.position_info[t_id]
                item_price = item.get("price", None)
                if item.get("relative_open_ref", None) == ref:
                    if direction == "long" and item_price > price:
                        return item
                    if direction == "short" and item_price < price:
                        return item
        return None
