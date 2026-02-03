import os
import threading
from functools import lru_cache
from langchain_openai import ChatOpenAI
from langchain_core.callbacks import BaseCallbackHandler

# ==================== LLM 配置 ====================
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://172.31.2.45/v1")
LLM_API_KEY = os.getenv("LLM_API_KEY", "EMPTY")
MODEL_NAME = os.getenv("LLM_MODEL_NAME", "qwen3_coder:30b")

# ==================== Package 路径配置 ====================
# Java 项目基础包路径，可通过环境变量自定义
# 示例：com.company.project, com.mycompany.myapp, org.example.demo
BASE_PACKAGE = os.getenv("JAVA_BASE_PACKAGE", "com.company.project")

# 将 package 转换为文件路径格式
# 如: com.company.project -> com/company/project
BASE_PACKAGE_PATH = BASE_PACKAGE.replace(".", "/")

# ==================== RAGFlow 配置 ====================
RAGFLOW_HOST = os.getenv("RAGFLOW_HOST", "http://ragflow-server:9380")
RAGFLOW_API_KEY = os.getenv("RAGFLOW_API_KEY")
FSD_DATASET_NAME = os.getenv("FSD_DATASET_NAME", "FSD_DS")
ARCH_DATASET_NAME = os.getenv("ARCH_DATASET_NAME", "JAVA_ARCHITECTURE_DS")

# ==================== 工作目录配置 ====================
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
INPUT_DIR = os.getenv("FSD_INPUT_DIR", f"{AIRFLOW_HOME}/data/input_fsd")
OUTPUT_DIR = os.getenv("FSD_OUTPUT_DIR", f"{AIRFLOW_HOME}/data/output_project")
FEEDBACK_DIR = os.getenv("FSD_FEEDBACK_DIR", f"{AIRFLOW_HOME}/data/feedback")
SESSION_DIR = os.getenv("FSD_SESSION_DIR", f"{AIRFLOW_HOME}/data/sessions")


class TokenBudgetTracker(BaseCallbackHandler):
    """Tracks cumulative token usage across all LLM invocations."""

    def __init__(self):
        self._lock = threading.Lock()
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self.total_calls = 0

    def on_llm_end(self, response, **kwargs):
        token_usage = getattr(response, 'llm_output', {})
        if isinstance(token_usage, dict):
            usage = token_usage.get('token_usage', {})
            with self._lock:
                self.total_prompt_tokens += usage.get('prompt_tokens', 0)
                self.total_completion_tokens += usage.get('completion_tokens', 0)
                self.total_calls += 1

    @property
    def total_tokens(self) -> int:
        return self.total_prompt_tokens + self.total_completion_tokens

    def summary(self) -> dict:
        return {
            'total_calls': self.total_calls,
            'total_prompt_tokens': self.total_prompt_tokens,
            'total_completion_tokens': self.total_completion_tokens,
            'total_tokens': self.total_tokens,
        }


# Global token tracker instance
token_tracker = TokenBudgetTracker()


@lru_cache(maxsize=8)
def get_llm(temperature=0.1):
    """
    获取 LLM 实例（按 temperature 缓存，同一 temperature 复用同一实例）

    Args:
        temperature: 生成温度，默认 0.1

    Returns:
        ChatOpenAI 实例
    """
    return ChatOpenAI(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=MODEL_NAME,
        temperature=temperature,
        max_tokens=8192,
        request_timeout=300,  # 5分钟超时，大模型生成代码较慢
        callbacks=[token_tracker],
    )


def get_package_path(package: str) -> str:
    """
    将 Java 包名转换为文件路径

    Args:
        package: Java 包名，如 com.company.project.core.entity

    Returns:
        文件路径格式，如 com/company/project/core/entity
    """
    return package.replace(".", "/")


