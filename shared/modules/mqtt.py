# -*- coding: utf-8 -*-
"""
TODO
    Update Date: 2026-05-04
    Description:
        1.1 啟動物件
            - ms = MqttServer(client_id=DEFAULT_CLIENT + MQTT_CLIENT_VERSION,
                              broker_host=BROKER,
                              broker_port=BROKER_PORT,
                              # middle_host=DEFAULT_MIDDLE_BROKER,
                              # middle_port=DEFAULT_MIDDLE_PORT,
                              logger=logger)
        1.2 若程序停止時，都要確實關閉服務
            - ms.stop_all_services()
        \
        2.1.1 [單純] 訂閱 Topic 使用
            - on_message 處理後續作業
        \
        2.2.2 [單純] 啟動一個 MQTT 服務
            -     ms.start_service(ms.publisher_server, **{
                        'title': f'推送訊息至 {BROKER}:{BROKER_PORT} 服務',
                        'use_middle': False,
                    })
        2.2.3 [單純] 推送至 Broker 使用
            - self.ms.add_content(topic=f'MachState/{self.machine_no}', payload=data, qos=1)
        \
        2.3.1 [複雜] 啟動一個 "TCP 中間層監聽佇列" 服務
            -     ms.start_service(ms.middle_server_thread, **{
                        'title': 'TCP 中間層監聽佇列服務',
                    })
        2.3.2 [複雜] 啟動一個 "TCP 中間層推送 MQTT" 服務
            -     ms.start_service(ms.publisher_server, **{
                        'title': 'TCP 中間層推送 MQTT 服務',
                        'use_middle': True,
                    })
        2.3.3 [複雜] 推送訊息至中間層，該層會基於給定參數將內容推送至 Broker
            - self.ms.send_to_middle(topic=f'MachState/{self.machine_no}', payload=data, qos=1)
        \
        3. [其他] 自定義函式
            - on_message
            - on_publish
            - on_connect_publisher
            - on_connect_subscriber ...etc.
        4. qos 協定機制自行網路查詢 [0, 1, 2]
        5. daemon=True，當主執行緒結束時，子執行緒會被強制終止
        6. 清除 topic 的 payload: self.ms.clear_retained_message(topic=f'MachState/{self.machine_no}', fun=self.ms.add_content)
    \
    Notice:
        FIXME : 明文傳送應加密 + 安全性須提升 ( 認證 ...etc. ) + socket 傳輸穩健性問題 (line:293)
"""
import socket, queue
import paho.mqtt.client as mqtt_client
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from shared.configs import (
    json,
    time,
    queue,
    threading,
)


KEEPALIVE_INTERVAL = 300 # timeout (300s = 5min)
DEFAULT_CLIENT = 'Developer'
DEFAULT_BROKER = '127.0.0.1'
DEFAULT_BROKER_PORT = 1883
DEFAULT_MIDDLE_BROKER = '127.0.0.1'
DEFAULT_MIDDLE_PORT = 9999
LOG_DEFAULT_NAME = 'MQTT Service'
MAX_WORKERS = 10
PUBLISHER_BATCH_SIZE = 10 # 設定批次處理的訊息數量
PUBLISHER_BATCH_TIMEOUT = 0.5 # 設定批次發布的逾時時間 (例如 0.5 秒)

# 設定最大訊息長度
    # 4096 (4 KB)
    # 8192 (8 KB)
    # 16384 (16 KB)
MAX_MSG_SIZE = 8192


class MqttServer:
    def __init__(self,
                 logging,
                 log_main_name: str,
                 stop_event=threading.Event(),
                 client_id: str = DEFAULT_CLIENT,
                 broker_host: str = DEFAULT_BROKER,
                 broker_port: int = DEFAULT_BROKER_PORT,
                 middle_host: str = DEFAULT_MIDDLE_BROKER,
                 middle_port: int = DEFAULT_MIDDLE_PORT,
                 max_workers: int = MAX_WORKERS,
                 username: str = None,
                 password: str = None,
                 ):

        self.logging = logging
        self.main_name = log_main_name
        self.stop_event = stop_event

        # ---------------------
        # 1.1) MQTT BROKER 設定
        # ---------------------
        # 連線設定
        self.client_id = client_id
        self.broker_host = broker_host
        self.broker_port = int(broker_port)
        self.username = username
        self.password = password

        # 提供任意程式提快速啟用中間層監聽佇列服務 # 通常是地端
        self.middle_host = middle_host
        self.middle_port = int(middle_port)
        self.middle_queue = queue.Queue()  # 建立中間層訊息佇列

        # 直接傳送訊息至 MQTT
        self.to_broker_queue = queue.Queue()

        # 訂閱清單
        self.subscriber_list = None

        # -------------
        # 1.2) 其他設定
        # -------------
        self._msg_count = 0
        self._last_report_time = 0
        self.max_workers = int(max_workers)
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers) # middle_server 專用


    def clear_retained_message(self, topic: str, fun: callable = None, **kwargs):
        # ------------------------------------
        # 2.3) 發布一個空的保留訊息，以清除 Broker 上的舊保留訊息。
        # ------------------------------------
        if fun is None:
            raise Exception('fun is None')
        try:
            self.add_content(topic=topic, payload=None, qos=0, retain=True)
            self.logging.info(f"成功向 Topic '{topic}' 發布空的保留訊息，以清除舊資料", stack_level=2)

        except Exception as e:
            self.logging.error(f"清除 Topic '{topic}' 的保留訊息時發生錯誤", stack_level=2)


    def subscriber_topic(self, subscriber_list: list = None,
                         on_connect_subscriber: callable = None,
                         on_message: callable = None, **kwargs):
        # -----------------------------------------------
        # 3.1) MQTT 執行緒函式
        # * 此執行緒負責持續從佇列中取出訊息並發布到 MQTT Broker
        # -----------------------------------------------
        if subscriber_list is None:
            raise Exception(f'缺少關鍵變數 subscriber_list is None')

        self.subscriber_list = subscriber_list

        mc = mqtt_client.Client(client_id=f'{self.client_id}_SubscriberTopic')

        if self.username is not None and self.password is not None:
            mc.username_pw_set(username=self.username, password=self.password)

        mc.on_connect = self.on_connect_subscriber if on_connect_subscriber is None else on_connect_subscriber
        mc.on_message = self.on_message if on_message is None else on_message
        try:
            mc.connect(self.broker_host, self.broker_port, KEEPALIVE_INTERVAL)
            mc.loop_start()

            self.logging.notice(f'啟動 MQTT 訂閱執行緒 ...', stack_level=0)
            # -------------------------------
            # 使用 Event 作為停止旗標，並定期檢查
            # -------------------------------
            while not self.stop_event.is_set():
                try:
                    self.stop_event.wait(timeout=0.1)  # 讓迴圈等待，同時可以處理來自 Event 的停止訊號
                    time.sleep(0.1)

                except queue.Empty:
                    # 佇列為空時，繼續循環以維持 loop_start() 的運行
                    pass

                except Exception as e:
                    self.logging.error('發生錯誤', stack_level=2)

        except Exception as e:
            self.logging.error('執行緒發生錯誤', stack_level=2)

        finally:
            mc.loop_stop()
            mc.disconnect()
            self.logging.notice('執行緒已停止 ...', stack_level=0)


    def add_content(self, topic: str = 'beta/add_content/', payload: dict = None,
                    qos: int = 1, retain: bool = False, **kwargs):
        # ----------------------------------------------------
        # 3.2.2) 直接傳送訊息至 MQTT Broker 服務 : 增加佇列內容函式
        # ----------------------------------------------------
        try:
            if type(payload) == dict:
                payload = json.dumps(payload)
            self.to_broker_queue.put((topic, payload, qos, retain))

            self._msg_count += 1
            current_qsize = self.to_broker_queue.qsize()

            if current_qsize > 100:
                if self._msg_count % 100 == 0:
                    self.logging.warning(f'高負載警報 => Queue length: {current_qsize}', stack_level=2)


            elif current_qsize <= 1:
                current_time = time.time()
                if current_time - self._last_report_time > 5:
                    self.logging.info(f'✅ 隊列已清空/穩定 => Queue length: {current_qsize}', stack_level=2)
                    self._last_report_time = current_time


        except Exception as e:
            self.logging.error('發生錯誤', stack_level=2)


    def middle_server_thread(self, **kwargs):
        # -------------------------------------
        # 3.3.1) TCP 伺服器: 被動接收其他程式的訊息
        # * 此執行緒負責監聽 TCP 連接 + 接受新連線
        # * 透過 send_to_middle 函式傳送數據
        # -------------------------------------
        self.logging.notice(f'啟動 TCP 服務 [{self.max_workers}]] '
                            f'監聽於 {self.middle_host}:{self.middle_port}', stack_level=0)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind((self.middle_host, self.middle_port))
                s.listen()

                # ------------------------------------------------
                # * 設定 socket 的逾時時間 (設定為 1 秒)
                # * 讓 s.accept() 在沒有新連線時會跳出，以便檢查停止訊號
                # ------------------------------------------------
                s.settimeout(1.0)

                # ------------------------
                # * 使用 Event 物件控制迴圈
                # ------------------------
                while not self.stop_event.is_set():
                    try:
                        conn, addr = s.accept()

                        self.executor.submit(self._handle_client_thread, conn, addr)

                        current_qsize = self.middle_queue.qsize()

                        if current_qsize > 100:
                            # ---------------------------------------------
                            # * 當佇列長度超過 10 時，記錄警告訊息 ( 避免頻繁洗版 )
                            # ---------------------------------------------
                            self.logging.info(f'Queue length：{current_qsize}', stack_level=2)

                        elif current_qsize == 1:
                            # --------------------------------------------------------
                            # * 當佇列長度等於 1 時，回報檢視消耗狀態，不採用 0 是因為很容易洗版
                            # --------------------------------------------------------
                            self.logging.info(f'Queue length：{current_qsize}', stack_level=2)

                    # -----------------------------------------------------
                    # * 處理 socket 逾時例外
                    # * 屬於預期情況，迴圈會繼續執行，並再次檢查 self.stop_event
                    # -----------------------------------------------------
                    except socket.timeout:
                        continue

                    except Exception as e:
                        self.logging.error('內部發生錯誤', stack_level=2)

            except Exception as e:
                self.logging.error('啟動時發生錯誤', stack_level=2)

            finally:
                self.logging.notice('TCP 服務準備停止，正在關閉 Socket ...', stack_level=0)

                # * 確保多執行緒關閉
                self.executor.shutdown(wait=True)

                # * 確保 socket 關閉，並釋放監聽埠
                s.close()


    def _handle_client_thread(self, conn, addr):
        # -------------------------------------------------------------------
        # 3.3.2) TCP 伺服器: 負責從單一客戶端接收數據，並將其放入佇列
        # * 背景情境下再開此執行緒: middle_server_thread -> handle_client_thread
        # -------------------------------------------------------------------
        # self.logging.info(f'TCP 連接來自 {addr}，啟動新執行緒開始處理 ...', stack_level=2)
        try:
            buffer = b''
            while not self.stop_event.is_set():
                # recv 依然會阻塞，但只阻塞此子執行緒 # 不影響其他執行緒或主執行緒
                part = conn.recv(1024)  # 讀取最多 1024 個位元組的資料

                # 連線中斷
                if not part:
                    break

                buffer += part
                # 檢查是否超出最大長度 # 若超出則強制傳送
                if len(buffer) > MAX_MSG_SIZE:
                    self.logging.error('接收到的數據超過最大限制，關閉連線 ...', stack_level=2)
                    conn.close()
                    break

                if buffer.endswith(b'\n'):
                    break

            if buffer:
                try:
                    data_string = buffer.decode('utf-8').strip()
                    message_data = json.loads(data_string)

                    topic = message_data.get('topic')
                    payload = message_data.get('payload')
                    qos = message_data.get('qos')
                    retain = message_data.get('retain')

                    if type(payload) == dict:
                        payload = json.dumps(payload)

                    if topic and payload is not None and qos is not None:
                        # self.logging.info(f'TCP 連接來自 {addr} | 接收到數據，放入佇列', stack_level=2)
                        self.middle_queue.put((topic, payload, qos, retain))

                    else:
                        self.logging.error(f'無效的 JSON 格式或缺少 topic/payload/qos 鍵', exc_info=False, stack_level=2)

                except json.JSONDecodeError:
                    self.logging.error(f'接收到無效的 JSON 數據：{buffer}', stack_level=2)

        except Exception as e:
            self.logging.error('內部發生錯誤', stack_level=2)

        finally:
            # self.logging.info(f'TCP 連接來自 {addr} 執行緒進行關閉 ...', stack_level=2)
            conn.close()


    def publisher_server(self, on_connect_publisher: callable = None,
                         on_publish: callable = None, **kwargs):
        # ----------------------
        # 3.3.3) MQTT 執行緒函式
        # * 管理多個發布執行緒
        # ----------------------
        if kwargs.get('use_middle', False):
            queue_type = self.middle_queue
            thread_name = 'middle_publisher'.upper()
        else:
            queue_type = self.to_broker_queue
            thread_name = 'publisher'.upper()

        self.logging.info(f'[{thread_name}] 啟動 MQTT 發布執行緒池 [{self.max_workers}] ...', stack_level=2)
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交多個任務到執行緒池，每個任務都是一個獨立的發布者
                futures = [executor.submit(self._publisher_worker,
                                           queue_type, f'{thread_name}_worker'
                                           ) for _ in range(self.max_workers)]

                for future in as_completed(futures):
                    try:
                        future.result()  # 檢查是否有任何發布者執行緒報錯

                    except Exception as e:
                        self.logging.error(f'[{thread_name}] 發布者執行緒發生錯誤', stack_level=2)

        except Exception as e:
            self.logging.error(f'[{thread_name}，發生錯誤]', stack_level=2)

        finally:
            self.logging.notice(f'[{thread_name}] 執行緒已停止 ...', stack_level=0)


    def _publisher_worker(self, todo_queue: Queue = None,
                          worker_title: str = 'publisher_worker'):
        # ------------------------------------
        # 3.3.4) MQTT 執行緒函式
        # * 此私有方法負責從佇列中取出一則訊息並發布
        # ------------------------------------
        mc = mqtt_client.Client(client_id=f'{self.client_id}_PublisherWorker_{threading.get_ident()}')

        if self.username is not None and self.password is not None:
            mc.username_pw_set(username=self.username, password=self.password)

        mc.on_connect = self.on_connect_publisher
        mc.on_publish = self.on_publish

        try:
            mc.connect(self.broker_host, self.broker_port, KEEPALIVE_INTERVAL)
            mc.loop_start()

            while not self.stop_event.is_set():
                # 讓迴圈等待，同時可以處理來自 Event 的停止訊號
                self.stop_event.wait(timeout=0.1)
                try:
                    # 使用非阻塞方式從佇列中取值，並設置較短的逾時
                    topic, payload, qos, retain = todo_queue.get(block=False)
                    mc.publish(topic, payload, qos=qos, retain=retain)
                    todo_queue.task_done()

                    self._msg_count += 1
                    current_qsize = todo_queue.qsize()

                    # 情況 A：高水位監控 (每 100 筆才記錄一次，避免洗版，但能反映高峰)
                    if current_qsize > 100:
                        if self._msg_count % 100 == 0:
                            # ---------------------------------------------
                            # * 當佇列長度超過 100 時，記錄警告訊息 ( 避免頻繁洗版 )
                            # ---------------------------------------------
                            self.logging.warning(f'[🔥 {worker_title}] 高負載警報 => '
                                                 f'Queue length: {current_qsize}', stack_level=2)

                    # 情況 B：確認清空狀況 (使用「歸零瞬間」+「冷卻時間」)
                    # 使用 time.time() 確保即使一直維持在 0~1，也不會每秒噴幾十條 Log
                    elif current_qsize <= 1:
                        current_time = time.time()
                        if current_time - self._last_report_time > 5: # 至少間隔 5 秒才報一次消耗完畢
                            self.logging.info(f'[✅ {worker_title}] 隊列已清空/穩定 => '
                                              f'Queue length: {current_qsize}', stack_level=2)
                            self._last_report_time = current_time

                except queue.Empty:
                    # 佇列為空，繼續等待
                    pass

        except Exception as e:
            self.logging.error(f'[{worker_title}] 內部錯誤', stack_level=2)

        finally:
            mc.loop_stop()
            mc.disconnect()


    def send_to_middle(self, topic: str = 'beta/add_content/', payload: dict = None,
                       qos: int = 1, retain: bool = False, **kwargs):
        # --------------------------------------
        # 3.3.5) 傳送訊息至 'TCP 中間層監聽佇列服務'
        # --------------------------------------
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.middle_host, self.middle_port))
                s.sendall(json.dumps({
                    'topic': topic,
                    'payload': payload,
                    'qos': qos,
                    'retain': retain,
                }).encode('utf-8'))

            # self.logging.info(f'已成功將數據發送給 TCP 中間層監聽佇列服務 ({self.middle_host}:{self.middle_port})', stack_level=2)

        except ConnectionRefusedError:
            self.logging.error(f'連接失敗...請確認 MQTT 服務程式已啟動，'
                              f'並運行在 {self.middle_host}:{self.middle_port}', exc_info=False, stack_level=2)

        except Exception as e:
            self.logging.error('發生錯誤', stack_level=2)


    def on_connect_publisher(self, client, userdata, flags, rc, **kwargs):
        # -------------------------------------
        # 4.1.1) MQTT 回呼函式
        # * 當客戶端成功連接到 MQTT Broker 時會呼叫
        # -------------------------------------
        if rc == 0:
            self.logging.notice(f'已成功連接到 MQTT Broker ({self.broker_host}:{self.broker_port})', stack_level=0)
        else:
            self.logging.error(f'連接失敗...錯誤碼: {rc}', exc_info=False, stack_level=2)


    def on_connect_subscriber(self, client, userdata, flags, rc, **kwargs):
        # --------------------------------------
        # 4.1.2) MQTT 回呼函式
        # * 當客戶端成功連接到 MQTT Broker 時會呼叫
        # --------------------------------------
        if rc == 0:
            self.logging.notice(f'已成功連接到 MQTT Broker ({self.broker_host}:{self.broker_port})', stack_level=0)
            for sub in self.subscriber_list:
                client.subscribe(sub)
                self.logging.notice(f'已訂閱 Topic: {sub}', stack_level=0)
        else:
            self.logging.error(f'連接失敗...錯誤碼: {rc}', exc_info=False, stack_level=2)


    def on_publish(self, client, userdata, mid, **kwargs):
        # ----------------------
        # 4.2) MQTT 回呼函式
        # * 當訊息成功發布時會呼叫
        # ----------------------
        # self.logging.info(f'訊息 [ID:{mid}] 已成功發布到 Broker', stack_level=2)
        pass


    def on_message(self, client, userdata, msg, **kwargs):
        # -----------------------------------------------
        # 4.3) MQTT 回呼函式
        # * 當收到來自 Broker 的訊息時會呼叫
        # * 根據收到的訊息執行其他動作 ex: 寫入資料庫或觸發事件
        # -----------------------------------------------
        try:
            client = client._client_id.decode('utf-8')
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            self.logging.notice(f'client: {client}, topic: {topic}, payload: {payload}', stack_level=0)
            # pass

        except Exception as e:
            self.logging.error('發生錯誤', stack_level=2)