# -*- coding: utf-8 -*-
"""
TODO
    Update Date: 2026-05-14
    Description:
    Notice:
        FIXME Loki 動態定義標籤待研究如何傳遞: extra, metadata
"""
import logging, logstash, logging_loki
from colorlog import ColoredFormatter
from logging.handlers import RotatingFileHandler
from shared.configs import os, json, pathlib
from shared.configs.constant import LONG_FORMAT
from shared.configs.settings import LOKI_HOST, LOKI_PORT, LOGSTASH_HOST, LOGSTASH_PORT


COLORS_CONFIG = {
    "INFO": "white",
    "NOTICE": "yellow",
    "WARNING": "red",
    "ERROR": "red",
    "DEBUG": "green",
    "CRITICAL": "bold_red",
}
FILE_FMT = "[%(asctime)s] %(levelname)s: %(message)s"
CONSOLE_FMT = (
    "%(log_color)s[%(asctime)s] [%(pathname)s:%(lineno)d] %(levelname)s: %(message)s"
)

TITLE_SYMBOL_NUMBER = 20
NOTICE_LEVEL_NUM = 25


def _init_logging_level_name():
    """TODO 僅註冊名稱，不注入方法至全域類別"""
    if hasattr(logging.Logger, "notice"):
        return  # 避免重複註冊

    if logging.getLevelName(NOTICE_LEVEL_NUM) != "NOTICE":
        logging.addLevelName(NOTICE_LEVEL_NUM, "NOTICE")


_init_logging_level_name()


class RelativePathFilter(logging.Filter):
    """TODO 自動將絕對路徑轉換為相對路徑的過濾器"""

    def filter(self, record):
        # 取得專案根目錄 (假設你已經定義好 GET_PATH_ROOT)
        # 這裡動態修改 record.pathname，這樣格式化工具就會用到縮短後的路徑
        project_root = os.getcwd()
        record.pathname = "".join(
            os.path.relpath(record.pathname, project_root).upper().split(".")[:-1]
        )
        return True


class Logger:
    def __init__(
        self,
        console_name: str = None,
        file_name: str = None,
        file_path: str = None,
        is_logstash: bool = True,
        is_loki: bool = True,
        max_bytes: int = (15 * 1024 * 1024),
        backup_count: int = 100,
        logging_level: str = "INFO",
        symbol_tag: dict = None,
        **kwargs,
    ):
        """
        TODO 日誌等級說明：
            DEBUG    10  追蹤細節  開發除錯、檢視 TCP 連線過程、模組內部運作
            INFO     20  一般確認  程式正常運行的關鍵節點（如：API 啟動）
            NOTICE   25  重要通知  [自定義] 用於比 INFO 重要但非錯誤的事件（如：模擬開始）
            WARNING  30  警告     潛在問題但不影響運行（如：硬碟空間即將不足）
            ERROR    40  錯誤     發生 Exception，特定功能失效但主程式未崩潰
            CRITICAL 50  嚴重     系統災難、無法繼續運行（如：資料庫連不上）
        """
        _LOGSTASH_HOST = kwargs.get("LOGSTASH_HOST", LOGSTASH_HOST)
        _LOGSTASH_PORT = kwargs.get("LOGSTASH_PORT", LOGSTASH_PORT)
        _LOKI_HOST = kwargs.get("LOKI_HOST", LOKI_HOST)
        _LOKI_PORT = kwargs.get("LOKI_PORT", LOKI_PORT)
        _IS_KUBERNETES = kwargs.get("IS_KUBERNETES", "false")
        _IS_KUBERNETES = True if _IS_KUBERNETES.lower() == "true" else False

        if _IS_KUBERNETES:
            is_logstash = False
            is_loki = False

        self.logging_level = logging_level
        self.symbol_tag = {} if symbol_tag is None else symbol_tag

        # 1. 建立唯一 Logger 實體
        self.raw_logger = logging.getLogger(console_name.upper())
        self.raw_logger.setLevel(getattr(logging, self.logging_level))

        # 2. 清除舊的 Handler 避免重複
        if self.raw_logger.hasHandlers():
            for handler in self.raw_logger.handlers:
                handler.close()
            self.raw_logger.handlers.clear()

        # 3. 設定 console 輸出設定
        if console_name:
            c_handler = logging.StreamHandler()
            c_handler.setFormatter(
                ColoredFormatter(
                    fmt=CONSOLE_FMT, datefmt=LONG_FORMAT, log_colors=COLORS_CONFIG
                )
            )
            self.raw_logger.addHandler(c_handler)

        # 4. 設定實體 log 輸出設定
        if file_name:
            if file_path is None:
                file_path = "logs" + file_name.replace(".", "/") + ".txt"

            os.makedirs(str(getattr(pathlib.Path(file_path), "parent")), exist_ok=True)

            f_handler = RotatingFileHandler(
                file_path,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
            f_handler.setFormatter(logging.Formatter(fmt=FILE_FMT, datefmt=LONG_FORMAT))
            self.raw_logger.addHandler(f_handler)

        # TODO 5. 設定 Logstash 輸出設定
        if is_logstash:
            ls_handler = logstash.TCPLogstashHandler(
                _LOGSTASH_HOST, _LOGSTASH_PORT, version=1
            )
            self.raw_logger.addHandler(ls_handler)

        # TODO 6. 設定 Loki 輸出設定
        if is_loki:
            loki_handler = logging_loki.LokiHandler(
                url=f"http://{_LOKI_HOST}:{_LOKI_PORT}/loki/api/v1/push",
                tags=self.symbol_tag,
                version="1",
            )
            self.raw_logger.addHandler(loki_handler)

        # TODO 7. 建立一個 Filter 辨識路徑
        path_filter = RelativePathFilter()
        self.raw_logger.addFilter(path_filter)

        # 8. 封裝通用記錄器 Adapter ( 自定義標籤 )
        self.logging = logging.LoggerAdapter(self.raw_logger, extra=self.symbol_tag)

    def debug(self, msg: str = "", stack_level: int = 2, **kwargs):
        self.log_custom("debug".lower(), msg, stack_level, **kwargs)

    def info(self, msg: str = "", stack_level: int = 2, **kwargs):
        self.log_custom("info".lower(), msg, stack_level, **kwargs)

    def warning(self, msg: str = "", stack_level: int = 2, **kwargs):
        self.log_custom("warning".lower(), msg, stack_level, **kwargs)

    def error(
        self, msg: str = "", exc_info: bool = True, stack_level: int = 2, **kwargs
    ):
        kwargs = {**{"exc_info": exc_info}, **kwargs}
        self.log_custom("error".lower(), msg, stack_level, **kwargs)

    def critical(
        self, msg: str = "", exc_info: bool = True, stack_level: int = 2, **kwargs
    ):
        kwargs = {**{"exc_info": exc_info}, **kwargs}
        self.log_custom("critical".lower(), msg, stack_level, **kwargs)

    def log_custom(self, level_name: str, msg: str, stack_level: int = 2, **kwargs):
        """
        通用日誌記錄器
        """
        _exc_info = kwargs.get("exc_info", False)
        _extra = kwargs.pop("extra_tags", {})  # 客製化標籤
        _full_extra = {**self.symbol_tag, **_extra}  # 合併標籤供 Handler 使用

        # metadata = ''
        # if _extra:
        #     _trans_json = json.dumps(_extra, ensure_ascii=False).strip()
        #     metadata = f' | META:{_trans_json}'
        # msg = f'{msg}{metadata}'

        if self.logging:
            method = getattr(self.logging, level_name, None)
            if method:
                method(
                    msg,
                    exc_info=_exc_info,
                    stacklevel=stack_level + 1,
                    extra=_full_extra,
                )

    def notice(self, msg: str = "", stack_level: int = 2, **kwargs):
        """使用底層的 log(level_num, msg) 避開全域方法的依賴"""
        _extra = kwargs.pop("extra_tags", {})  # 客製化標籤
        _full_extra = {**self.symbol_tag, **_extra}  # 合併標籤供 Handler 使用

        # metadata = ''
        # if _extra:
        #     _trans_json = json.dumps(_extra, ensure_ascii=False).strip()
        #     metadata = f' | META:{_trans_json}'
        # msg = f'{msg}{metadata}'

        if self.logging:
            self.logging.log(
                NOTICE_LEVEL_NUM, msg, stacklevel=stack_level + 2, extra=_full_extra
            )

    def title_log(
        self,
        level_name: str,
        msg: str,
        exc_info: bool = False,
        stack_level: int = 2,
        **kwargs,
    ) -> str:
        _level_name = level_name.lower()
        msg = f"\n\n\n{'=' * TITLE_SYMBOL_NUMBER} {msg} {'=' * TITLE_SYMBOL_NUMBER}\n"

        if _level_name == "notice":
            self.logging.log(NOTICE_LEVEL_NUM, msg, stacklevel=stack_level + 2)
        else:
            self.log_custom(
                _level_name,
                msg,
                **{
                    "stack_level": stack_level,
                    "exc_info": exc_info,
                },
            )
