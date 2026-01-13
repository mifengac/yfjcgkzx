"""
全局会话管理器
处理统一的登录和会话管理功能
"""
import requests
import threading
from datetime import datetime, timedelta
import logging
from typing import Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def _decode_response_text(content: bytes, preferred: Optional[str] = None) -> Tuple[str, str]:
    """
    兼容部分内网系统返回 GBK/GB18030 但声明为 UTF-8 的情况，避免 response.text 直接抛 UnicodeDecodeError。
    返回 (text, encoding_used)。
    """
    candidates = []
    if preferred:
        candidates.append(preferred)
    candidates.extend(["utf-8", "gb18030", "gbk"])

    last_error: Optional[Exception] = None
    for enc in candidates:
        try:
            return content.decode(enc), enc
        except Exception as exc:
            last_error = exc
            continue

    logging.warning("响应内容解码失败，将使用 utf-8 replace 兜底: %s", last_error)
    return content.decode("utf-8", errors="replace"), "utf-8(replace)"

class SessionManager:
    """
    全局会话管理器类
    """
    def __init__(self):
        self.session = None
        self.last_login_time = None
        self.lock = threading.Lock()  # 确保线程安全
        self.login_url = "http://68.253.2.107/zhksh/login"
        self.username = "270378"
        self.password = "jpx8hLPMyV7EDVX1p9d89Q=="
        self.target_host = "68.253.2.107"
        # 添加登录失败计数器和冷却时间
        self.login_failure_count = 0
        self.last_login_failure_time = None
        self.login_failure_cooldown = timedelta(minutes=1)  # 1分钟冷却时间
    
    def get_session(self):
        """
        获取当前会话，如果会话不存在或已过期则重新登录
        """
        with self.lock:
            if self.session is None or self._is_session_expired():
                logging.info("会话不存在或已过期，正在重新登录...")
                self._login()
            return self.session
    
    def _is_session_expired(self):
        """
        判断会话是否过期
        优化验证逻辑，减少网络请求频率
        """
        if self.last_login_time is None:
            return True

        # 会话有效期延长至4小时，减少验证频率
        if datetime.now() - self.last_login_time > timedelta(hours=4):
            return True

        # 只在会话接近过期时才进行网络验证（剩余30分钟时）
        time_until_expiry = timedelta(hours=4) - (datetime.now() - self.last_login_time)
        if time_until_expiry > timedelta(minutes=30):
            # 会话仍然有效，不需要网络验证
            return False

        # 只有在会话即将过期时才进行网络验证
        try:
            test_session = self._create_session_with_cookie()
            # 尝试访问一个接口来验证会话
            test_response = test_session.get(f"http://{self.target_host}/zhksh/system/user/getInfo",
                                          timeout=(3.05, 10))  # 连接超时3.05秒，读取超时10秒
            if test_response.status_code != 200:
                logging.info("会话已失效，需要重新登录")
                return True
            else:
                # 会话仍然有效，更新登录时间
                self.last_login_time = datetime.now()
                return False
        except requests.exceptions.Timeout:
            logging.warning("验证会话有效性时超时，认为会话失效")
            return True
        except Exception as e:
            logging.warning(f"验证会话有效性时发生错误: {e}")
            return True

    def _create_session_with_cookie(self):
        """
        创建一个新的会话并设置Cookie
        """ 
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
        })
        return session

    def _login(self):
        """
        执行登录操作，包含失败处理和重试限制
        """
        # 检查是否在冷却期内
        if (self.last_login_failure_time and
            datetime.now() - self.last_login_failure_time < self.login_failure_cooldown):
            time_remaining = self.login_failure_cooldown - (datetime.now() - self.last_login_failure_time)
            logging.warning(f"登录失败后冷却期，还需等待 {time_remaining.total_seconds():.1f} 秒")
            raise Exception(f"登录失败后冷却期，请稍后再试")

        try:
            # 创建新的会话
            session = self._create_session_with_cookie()

            # 准备登录数据
            login_data = {
                'username': self.username,
                'password': self.password,
                'rememberMe': True,
                'isPkiLogin': False,
                'isAccLogin': True,
                'isSmsLogin': False
            }

            # 执行登录请求
            response = session.post(self.login_url, data=login_data, timeout=180)
            # 移除 response.raise_for_status()，因为我们将在下面手动处理状态码

            # 验证登录是否成功（检查响应内容）
            logging.info(f"登录响应状态码: {response.status_code}")
            logging.info(f"登录响应头: {dict(response.headers)}")

            # 尝试获取响应内容用于调试
            preferred = None
            try:
                preferred = response.apparent_encoding
            except Exception:
                preferred = None
            response_text, used_encoding = _decode_response_text(response.content, preferred=preferred)
            # 让后续 response.json()/response.text 使用同一编码（如对方声明错误，这里会覆盖）
            if used_encoding and not used_encoding.endswith("(replace)"):
                response.encoding = used_encoding
            logging.info(f"登录响应内容前200字符: {response_text[:200]}")

            login_success = False
            error_msg = "登录失败"

            # 方法1：检查响应内容是否包含成功标识
            if response_text and ("操作成功" in response_text or "登录成功" in response_text):
                login_success = True
                logging.info("登录成功（响应内容包含成功标识）")

            # 方法2：尝试解析JSON响应
            try:
                response_data = response.json()
                logging.info(f"解析JSON响应成功: {response_data}")

                # 检查多种成功标识
                if (response_data.get('code') == 200 or
                    response_data.get('code') == 0 or
                    response_data.get('success') is True or
                    response_data.get('msg') == "操作成功" or
                    response_data.get('message') == "操作成功"):
                    login_success = True
                    logging.info("登录成功（JSON响应验证通过）")
                else:
                    error_msg = response_data.get('msg') or response_data.get('message') or "登录失败"
                    logging.warning(f"JSON响应显示登录失败: {error_msg}")

            except (ValueError, KeyError) as e:
                logging.warning(f"JSON解析失败: {e}")
                # 如果JSON解析失败，检查状态码
                if response.status_code == 200:
                    login_success = True
                    logging.info("登录成功（基于状态码200）")
                else:
                    error_msg = f"响应解析失败，状态码: {response.status_code}"

            # 方法3：检查状态码
            if not login_success and response.status_code == 200:
                login_success = True
                logging.info("登录成功（最终基于状态码200）")

            # 根据验证结果处理
            if login_success:
                self.session = session
                self.last_login_time = datetime.now()
                # 重置失败计数器
                self.login_failure_count = 0
                self.last_login_failure_time = None
                logging.info("登录会话已建立")
            else:
                raise Exception(error_msg)

        except requests.exceptions.Timeout:
            error_msg = f"登录请求超时"
            logging.error(error_msg)
            self._record_login_failure()
            raise Exception(error_msg)
        except Exception as e:
            error_msg = f"登录过程中发生错误: {e}"
            logging.error(error_msg)
            self._record_login_failure()
            raise Exception(error_msg)

    def _record_login_failure(self):
        """
        记录登录失败信息
        """
        self.login_failure_count += 1
        self.last_login_failure_time = datetime.now()

        # 如果失败次数过多，延长冷却时间
        if self.login_failure_count > 3:
            self.login_failure_cooldown = timedelta(minutes=5)  # 5次失败后冷却5分钟
        elif self.login_failure_count > 5:
            self.login_failure_cooldown = timedelta(minutes=15)  # 6次失败后冷却15分钟

        logging.warning(f"登录失败次数: {self.login_failure_count}，冷却时间: {self.login_failure_cooldown.total_seconds():.1f}秒")

    def test_login(self):
        """
        测试登录功能，用于调试
        """
        try:
            logging.info("开始测试登录功能...")
            # 强制清除当前会话
            self.session = None
            # 尝试登录
            self._login()
            # 验证会话
            if self.session:
                logging.info("登录测试成功！")
                return True
            else:
                logging.error("登录测试失败：没有建立会话")
                return False
        except Exception as e:
            logging.error(f"登录测试失败: {e}")
            return False

    def make_request(self, method, url, **kwargs):
        """
        使用全局会话发起请求
        如果请求失败，会自动重新登录并重试
        """
        session = self.get_session()
        
        try:
            # 检查URL是否是目标主机
            if self.target_host in url:
                # 为所有请求添加超时设置，优先使用kwargs中的超时，否则使用180秒
                if 'timeout' not in kwargs:
                    kwargs['timeout'] = 180

                response = session.request(method, url, **kwargs)

                # 检查响应是否表明会话已失效（比如返回登录页面或401状态码）
                if response.status_code == 401 or "login" in response.url.lower():
                    logging.info("请求失败，会话可能已失效，正在重新登录...")
                    # 强制清除当前会话
                    self.session = None
                    self._login()  # 重新登录
                    session = self.get_session()  # 获取新的会话
                    # 重试时也添加超时设置
                    if 'timeout' not in kwargs:
                        kwargs['timeout'] = 180
                    # 只重试一次，避免无限重试
                    try:
                        response = session.request(method, url, **kwargs)  # 重新请求
                    except Exception as retry_error:
                        logging.error(f"重新登录后重试失败: {retry_error}")
                        raise Exception(f"会话失效且重试失败: {retry_error}")

                return response
            else:
                # 如果不是目标主机的请求，直接发起原生请求
                if 'timeout' not in kwargs:
                    kwargs['timeout'] = 180
                return requests.request(method, url, **kwargs)

        except requests.exceptions.Timeout:
            logging.error(f"请求超时: {url}")
            raise
        except Exception as e:
            logging.error(f"请求失败: {e}")

            # 如果请求失败，尝试重新登录后重试（仅限一次）
            logging.warning(f"请求失败，尝试重新登录后重试: {url}")
            try:
                # 强制清除当前会话
                self.session = None
                self._login()
                session = self.get_session()
                # 重试时也添加超时设置
                if 'timeout' not in kwargs:
                    kwargs['timeout'] = 180
                response = session.request(method, url, **kwargs)
                return response
            except requests.exceptions.Timeout:
                logging.error(f"重新登录后请求仍然超时: {url}")
                raise Exception(f"请求超时且重试失败: {url}")
            except Exception as retry_error:
                logging.error(f"重新登录后请求仍然失败: {retry_error}")
                raise Exception(f"请求失败且重试失败: {retry_error}")


# 全局会话管理器实例
session_manager = SessionManager()
