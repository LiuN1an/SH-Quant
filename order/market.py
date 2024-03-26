import inspect
from openctp_ctp import mdapi
from datetime import datetime
from config import REAL_instruments

instruments = REAL_instruments

today_str = datetime.today().strftime("%Y%m%d")


class CTP(mdapi.CThostFtdcMdSpi):
    def __init__(self, front: str, cb):
        print("-------------------------------- 启动 mduser api demo ")
        super().__init__()
        self._front = front
        self.send = cb
        self._api = mdapi.CThostFtdcMdApi.CreateFtdcMdApi(
            "market"
        )  # type: mdapi.CThostFtdcMdApi

        print("CTP行情API版本号:", self._api.GetApiVersion())
        print("行情前置:" + self._front)

        # 注册行情前置
        self._api.RegisterFront(self._front)
        # 注册行情回调实例
        self._api.RegisterSpi(self)
        # 初始化行情实例
        self._api.Init()
        print("初始化成功")

    def OnFrontConnected(self):
        """行情前置连接成功"""
        print("行情前置连接成功")

        # 登录请求, 行情登录不进行信息校验
        print("登录请求")
        req = mdapi.CThostFtdcReqUserLoginField()
        self._api.ReqUserLogin(req, 0)

    def OnRspUserLogin(
        self,
        pRspUserLogin: mdapi.CThostFtdcRspUserLoginField,
        pRspInfo: mdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        """登录响应"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            print(f"登录失败: ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}")
            return

        print("登录成功")

        if len(instruments) == 0:
            return

        # 订阅行情
        print("订阅行情请求：", instruments)
        self._api.SubscribeMarketData(
            [i.encode("utf-8") for i in instruments], len(instruments)
        )

    def OnRtnDepthMarketData(
        self, pDepthMarketData: mdapi.CThostFtdcDepthMarketDataField
    ):
        data = {}
        for name, value in inspect.getmembers(pDepthMarketData):
            if name[0].isupper():
                data[name] = value
        if self.send is not None:
            # ymd = data["ActionDay"]
            hms = data["UpdateTime"]
            price = data["LastPrice"]
            self.send(f"{today_str} {hms}", price, 0)

    def OnRspSubMarketData(
        self,
        pSpecificInstrument: mdapi.CThostFtdcSpecificInstrumentField,
        pRspInfo: mdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        """订阅行情响应"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            print(
                f"订阅行情失败:ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}",
            )
            return

        print("订阅行情成功:", pSpecificInstrument.InstrumentID)

    def wait(self):
        # 阻塞 等待
        input("-------------------------------- 按任意键退出 trader api demo ")

        self._api.Release()
