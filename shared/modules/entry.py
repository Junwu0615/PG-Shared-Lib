# -*- coding: utf-8 -*-
"""
TODO
    Update Date: 2026-05-07
    Description: 所有 APP 通用底層入口
        - [統一初始化 + 分發給子模塊] logging
        - [統一註冊 + 關閉] 採用 threading 而非 asyncio
        - [優雅關閉服務] 偵測 docker rm : SIGTERM
        - [處理例外] exception handling
        - [影響併行可能性] 唯一實例註冊 : __new__
        - [上下文管理器] __enter__ + __exit__
        - [依賴注入代碼設計]
        - [硬編碼拉到外部 .env 設置]
    Notice:
"""
import signal
from shared.configs import (
    os, sys, time, load_dotenv, threading,
    Callable, Iterator, Tuple, Any, Dict, List, Optional,
)


class EntryPoint:
    _instance: Optional['EntryPoint'] = None
    _initialized: Optional['EntryPoint'] = None
    _lock: threading.Lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """
        實作線程安全的唯一實例註冊 (Singleton)
        只會產生一個實例，避免配置重複加載或訊號監聽衝突
        """
        # [第一層檢查] 若實例已存在，直接回傳，不進入鎖區域
        if cls._instance is None:
            with cls._lock:
                # [第二層檢查] 確保在等待鎖的期間，沒有其他線程已經建立了實例
                if cls._instance is None:
                    cls._instance = super(EntryPoint, cls).__new__(cls)
                    cls._instance._initialized = False
                    cls._initialized = False
        return cls._instance


    def __enter__(self, **kwargs):
        """上下文管理器 : 開始"""
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器 : 結束"""
        return False


    def __init__(self, dotenv_path: Optional[str] = None, **kwargs):
        if self._initialized:
            return

        # 加載環境變數
        load_dotenv(dotenv_path=dotenv_path)
        self.env = {
            'APP_ENV': os.getenv('APP_ENV', 'UNDEFINED')
        }

        # 宣告變數
        self._stop_event = None
        self._threads = None
        self.logging = None


    def configure_setting(self, logging, **kwargs):
        """必要工具初始化"""

        # 1. 註冊 threading 物件，供子模塊使用
        # 工具支援 : mqtt / kafka producer
        self._stop_event = threading.Event()
        self._threads = []

        # 2. 註冊系統訊號 (Docker rm 發送的是 SIGTERM)
        self._setup_signals()

        # 3. 取得 main.py 傳遞的日誌設定
        self.logging = logging

        # 4. 初始化完成標記
        self._initialized = True
        self.logging.notice(f'[{self.env['APP_ENV']} MODE] EntryPoint Initialized ...', stack_level=0)


    def start_service(self, func: callable, **kwargs):
        """TODO 多執行緒啟動統一途徑"""
        _title = kwargs.get('title', 'UNKNOWN SERVICE')
        service_thread = threading.Thread(
            target=func,
            daemon=False,
            kwargs=kwargs,
        )
        service_thread.start()
        self._threads.append(service_thread)
        self.logging.info(f'{_title} ... 已啟動')


    def stop_all_services(self, **kwargs):
        """TODO 安全地關閉多執行緒"""
        if self._threads:
            self.logging.notice('正在向所有執行緒發出停止訊號...', stack_level=0)
            self._stop_event.set() # 發出停止訊號

            # 等待所有執行緒結束
            for thread in self._threads:
                if thread.is_alive():
                    self.logging.info(f'[{thread.name}] 等待執行緒結束...')
                    thread.join(timeout=10.0)
                    if thread.is_alive():
                        self.logging.error(f'[{thread.name}] 執行緒超時未結束，強制繼續程序 ...')

            self.logging.title_log('notice', '所有執行緒服務已確實關閉', stack_level=0)
            time.sleep(0.1)


    def _setup_signals(self, **kwargs):
        """TODO 偵測系統關閉訊號"""
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._handle_exit)


    def _handle_exit(self, signum, frame, **kwargs):
        """安全關閉程序 1"""
        self.stop_all_services()
        time.sleep(1)
        self.logging.warning(f'[Graceful Shutdown # 1] Received signal [{signum}] ...', stack_level=2)


    def _finalize(self, **kwargs):
        """安全關閉程序 2"""
        self.logging.warning(f'[Graceful Shutdown # 2] sys.exit(0)', stack_level=2)
        sys.exit(0)


    def main(self, **kwargs):
        """
        TODO 所有事物在此進行完整生態週期
            - 使用 with 觸發 __enter__ 與 __exit__
            - 供 main.py 以 EntryPoint.main 使用，並觸發 run
        """
        try:
            with self:
                self.run()

        except Exception as e:
            self.logging.critical(f'Unhandled Exception', exc_info=True, stack_level=0)

        finally:
            self._finalize()


    def run(self, **kwargs):
        """
        TODO 實施業務邏輯區塊，供 main.py 的 class 以 run 繼承覆蓋使用
        """
        pass