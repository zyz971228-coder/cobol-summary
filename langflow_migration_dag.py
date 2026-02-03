"""
Airflow DAG: COBOL Summary Generator
====================================
This DAG migrates the Langflow workflow (Cobol_Summary_11Nov.json) to native
LangChain Python code running on Apache Airflow.

Original Langflow workflow functionality:
- Reads COBOL source files from a directory
- Uses RAGFlow Chat with knowledge base (Cobol-Source-Code, Cobol_Manuals) for
  retrieval-augmented generation (RAG)
- Uses LLM to analyze code and generate documentation:
  1. Program Overview
  2. Flowchart (Mermaid syntax)
  3. Input/Output descriptions
- Combines results into a comprehensive markdown document
- Saves output to specified directory

RAGFlow Chat Configuration (from original Langflow):
====================================================
- Datasets: Cobol-Source-Code, Cobol_Manuals
- Similarity threshold: 0.2
- Vector similarity weight: 0.30 (full-text: 0.70)
- Top N: 8
- System prompt includes {knowledge} placeholder for RAG injection

Requirements (requirements.txt):
================================
apache-airflow>=2.7.0
langchain>=0.1.0
langchain-openai>=0.0.5
langchain-community>=0.0.20
openai>=1.10.0
pandas>=2.0.0
aiohttp>=3.9.0
requests>=2.31.0
python-dotenv>=1.0.0

Environment Variables:
======================
VLLM_API_BASE: Base URL for VLLM API (e.g., http://localhost:8000/v1)
VLLM_API_KEY: API key for VLLM (if required)
VLLM_MODEL_NAME: Model name for VLLM (default: Qwen/Qwen2.5-72B-Instruct)
RAGFLOW_API_BASE: Base URL for RAGFlow API (e.g., http://localhost:9380)
RAGFLOW_API_KEY: API key for RAGFlow
RAGFLOW_CHAT_ID: Chat Assistant ID for RAGFlow (required for RAG functionality)
COBOL_INPUT_DIR: Input directory containing COBOL source files
COBOL_OUTPUT_DIR: Output directory for generated summaries
USE_RAGFLOW: Set to "true" to use RAGFlow Chat with knowledge base (default: false)
"""

from datetime import datetime, timedelta
from typing import Any, Optional
import os
import logging
import json
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import pandas as pd
import requests

# ============================================================================
# Configuration
# ============================================================================

# Default configuration - can be overridden by Airflow Variables
DEFAULT_CONFIG = {
    # VLLM Configuration
    "vllm_api_base": "http://localhost:8000/v1",
    "vllm_model_name": "Qwen/Qwen2.5-72B-Instruct",
    # RAGFlow Configuration
    "ragflow_api_base": "http://localhost:9380",  # Note: without /v1
    "ragflow_api_key": "",
    "ragflow_chat_id": "",  # Chat Assistant ID from RAGFlow
    # RAGFlow Chat Settings (matching your screenshot)
    "ragflow_similarity_threshold": 0.2,
    "ragflow_vector_weight": 0.3,
    "ragflow_top_n": 8,
    # General Settings
    "cobol_input_dir": "/data/projects/cobol-test/Input",
    "cobol_output_dir": "/data/projects/cobol-test/Output",
    "max_tokens": 4096,
    "temperature": 0.7,
    "use_ragflow": False,  # Set to True to use RAGFlow Chat with knowledge base
}

# ============================================================================
# RAGFlow Chat Client
# ============================================================================

class RAGFlowChatClient:
    """
    Client for RAGFlow Chat API with knowledge base retrieval.

    RAGFlow Chat automatically retrieves relevant content from configured
    datasets (Cobol-Source-Code, Cobol_Manuals) and injects it into the
    system prompt via the {knowledge} placeholder.

    API Reference: https://ragflow.io/docs/dev/http_api_reference
    """

    def __init__(
        self,
        api_base: str,
        api_key: str,
        chat_id: str,
        similarity_threshold: float = 0.2,
        vector_weight: float = 0.3,
        top_n: int = 8,
    ):
        """
        Initialize RAGFlow Chat Client.

        Args:
            api_base: RAGFlow API base URL (e.g., http://localhost:9380)
            api_key: RAGFlow API key
            chat_id: Chat Assistant ID from RAGFlow
            similarity_threshold: Minimum similarity score for retrieval (default: 0.2)
            vector_weight: Weight for vector similarity (default: 0.3, full-text: 0.7)
            top_n: Number of top results to retrieve (default: 8)
        """
        self.api_base = api_base.rstrip('/')
        self.api_key = api_key
        self.chat_id = chat_id
        self.similarity_threshold = similarity_threshold
        self.vector_weight = vector_weight
        self.top_n = top_n
        self.session_id = None

        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

    def create_session(self) -> str:
        """
        Create a new chat session.

        Returns:
            Session ID for the new conversation
        """
        url = f"{self.api_base}/api/v1/chats/{self.chat_id}/sessions"

        payload = {
            "name": f"COBOL Analysis Session {datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()

            if result.get("code") == 0 and result.get("data"):
                self.session_id = result["data"].get("id")
                logging.info(f"Created RAGFlow session: {self.session_id}")
                return self.session_id
            else:
                error_msg = result.get("message", "Unknown error")
                logging.error(f"Failed to create session: {error_msg}")
                raise Exception(f"RAGFlow session creation failed: {error_msg}")

        except requests.exceptions.RequestException as e:
            logging.error(f"RAGFlow API request failed: {e}")
            raise

    def send_message(
        self,
        message: str,
        session_id: Optional[str] = None,
        stream: bool = False
    ) -> str:
        """
        Send a message to RAGFlow Chat and get response with RAG.

        The RAGFlow Chat will automatically:
        1. Retrieve relevant content from knowledge base (Cobol-Source-Code, Cobol_Manuals)
        2. Inject retrieved content into the {knowledge} placeholder in system prompt
        3. Generate response using the LLM

        Args:
            message: User message to send
            session_id: Session ID (uses current session if not provided)
            stream: Whether to use streaming response

        Returns:
            LLM response text
        """
        sid = session_id or self.session_id
        if not sid:
            sid = self.create_session()

        url = f"{self.api_base}/api/v1/chats/{self.chat_id}/completions"

        payload = {
            "question": message,
            "session_id": sid,
            "stream": stream,
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=300)
            response.raise_for_status()
            result = response.json()

            if result.get("code") == 0 and result.get("data"):
                answer = result["data"].get("answer", "")
                # Log retrieval info if available
                if "reference" in result["data"]:
                    refs = result["data"]["reference"]
                    logging.info(f"RAGFlow retrieved {len(refs)} reference chunks")
                return answer
            else:
                error_msg = result.get("message", "Unknown error")
                logging.error(f"RAGFlow completion failed: {error_msg}")
                return f"Error: {error_msg}"

        except requests.exceptions.RequestException as e:
            logging.error(f"RAGFlow API request failed: {e}")
            return f"Error: {str(e)}"

    def chat_with_retry(
        self,
        message: str,
        max_retries: int = 3,
        retry_delay: float = 2.0
    ) -> str:
        """
        Send message with automatic retry on failure.

        Args:
            message: User message to send
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds

        Returns:
            LLM response text
        """
        last_error = None

        for attempt in range(max_retries):
            try:
                response = self.send_message(message)
                if not response.startswith("Error:"):
                    return response
                last_error = response
            except Exception as e:
                last_error = str(e)
                logging.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")

            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))

        return f"Failed after {max_retries} attempts. Last error: {last_error}"

    def close_session(self):
        """Close the current session."""
        if self.session_id:
            try:
                url = f"{self.api_base}/api/v1/chats/{self.chat_id}/sessions"
                requests.delete(
                    url,
                    headers=self.headers,
                    json={"ids": [self.session_id]},
                    timeout=30
                )
                logging.info(f"Closed RAGFlow session: {self.session_id}")
            except Exception as e:
                logging.warning(f"Failed to close session: {e}")
            finally:
                self.session_id = None


# ============================================================================
# Prompt Templates
# ============================================================================

# RAGFlow System Prompt (matching your screenshot configuration)
# The {knowledge} placeholder will be replaced by RAGFlow with retrieved content
RAGFLOW_SYSTEM_PROMPT = """You are an intelligent assistant. Please summarize the content of the knowledge base to answer the question. Please list the data in the knowledge base and answer in detail. When all knowledge base content is irrelevant to the question, your answer must include the sentence "The answer you are looking for is not found in the knowledge base!" Answers need to consider chat history.
      Here is the knowledge base:
      {knowledge}
      The above is the knowledge base."""

# Standard System Prompt for non-RAGFlow usage
SYSTEM_PROMPT = """You are an expert COBOL programmer and technical documentation specialist.
Analyze the provided COBOL source code and generate clear, accurate documentation."""

PROGRAM_OVERVIEW_PROMPT = """By analyzing above cobol source code, provide the Program Overview, write no less than 300 words in this session. Generate the Program Overview one time only, do not repeat.

## 1. Program Overview

* **Program ID**: `[Extracted from IDENTIFICATION DIVISION]`
* **Function Description**: A concise summary of the program's main business purpose.
* **Main Processes **: List out all the processes in the cobol program, by looking into the PROCEDURE DIVISION."""

FLOWCHART_PROMPT = """By analyzing the cobol source code above, provide the Flowchart. Use Mermaid syntax to visualize the main execution flow of `PROCEDURE DIVISION`.
Please strictly generate the document in the following Markdown structure:

## 2. Flowchart
```mermaid
graph TD
    A[Start] --> B[Read Input File];
    B --> C[End of File?];
    C -- Yes --> D[Close Files];
    C -- No --> E[Process Record];
    E --> F[Write to Output File];
    F --> B;
    D --> G[End];
```
"""

INPUT_OUTPUT_PROMPT = """By analyzing the cobol source code above, provide the Input/Output of the code in below md format

## 3. Input/Output
* **Input**:
    * `[Input File Name 1]`: [Briefly describe the purpose and key fields of this file].
    * `[Input File Name 2]`: ...
	...
* **Output**:
    * `[Output File Name 1]`: [Briefly describe the purpose and generation method of this file].
    * `[Output File Name 2]`: ...
	...
"""

# ============================================================================
# Utility Functions
# ============================================================================

def get_config() -> dict:
    """Get configuration from Airflow Variables or use defaults."""
    config = DEFAULT_CONFIG.copy()

    # Try to get from Airflow Variables
    try:
        # VLLM Configuration
        config["vllm_api_base"] = Variable.get("VLLM_API_BASE", default_var=config["vllm_api_base"])
        config["vllm_api_key"] = Variable.get("VLLM_API_KEY", default_var="")
        config["vllm_model_name"] = Variable.get("VLLM_MODEL_NAME", default_var=config["vllm_model_name"])

        # RAGFlow Configuration
        config["ragflow_api_base"] = Variable.get("RAGFLOW_API_BASE", default_var=config["ragflow_api_base"])
        config["ragflow_api_key"] = Variable.get("RAGFLOW_API_KEY", default_var="")
        config["ragflow_chat_id"] = Variable.get("RAGFLOW_CHAT_ID", default_var="")

        # RAGFlow Chat Settings
        config["ragflow_similarity_threshold"] = float(Variable.get(
            "RAGFLOW_SIMILARITY_THRESHOLD",
            default_var=config["ragflow_similarity_threshold"]
        ))
        config["ragflow_vector_weight"] = float(Variable.get(
            "RAGFLOW_VECTOR_WEIGHT",
            default_var=config["ragflow_vector_weight"]
        ))
        config["ragflow_top_n"] = int(Variable.get(
            "RAGFLOW_TOP_N",
            default_var=config["ragflow_top_n"]
        ))

        # General Settings
        config["cobol_input_dir"] = Variable.get("COBOL_INPUT_DIR", default_var=config["cobol_input_dir"])
        config["cobol_output_dir"] = Variable.get("COBOL_OUTPUT_DIR", default_var=config["cobol_output_dir"])
        config["use_ragflow"] = Variable.get("USE_RAGFLOW", default_var="false").lower() == "true"

    except Exception:
        # Fall back to environment variables if Airflow Variables not available
        config["vllm_api_base"] = os.getenv("VLLM_API_BASE", config["vllm_api_base"])
        config["vllm_api_key"] = os.getenv("VLLM_API_KEY", "")
        config["vllm_model_name"] = os.getenv("VLLM_MODEL_NAME", config["vllm_model_name"])

        config["ragflow_api_base"] = os.getenv("RAGFLOW_API_BASE", config["ragflow_api_base"])
        config["ragflow_api_key"] = os.getenv("RAGFLOW_API_KEY", "")
        config["ragflow_chat_id"] = os.getenv("RAGFLOW_CHAT_ID", "")

        config["ragflow_similarity_threshold"] = float(os.getenv(
            "RAGFLOW_SIMILARITY_THRESHOLD",
            str(config["ragflow_similarity_threshold"])
        ))
        config["ragflow_vector_weight"] = float(os.getenv(
            "RAGFLOW_VECTOR_WEIGHT",
            str(config["ragflow_vector_weight"])
        ))
        config["ragflow_top_n"] = int(os.getenv(
            "RAGFLOW_TOP_N",
            str(config["ragflow_top_n"])
        ))

        config["cobol_input_dir"] = os.getenv("COBOL_INPUT_DIR", config["cobol_input_dir"])
        config["cobol_output_dir"] = os.getenv("COBOL_OUTPUT_DIR", config["cobol_output_dir"])
        config["use_ragflow"] = os.getenv("USE_RAGFLOW", "false").lower() == "true"

    return config


def create_ragflow_client(config: dict) -> RAGFlowChatClient:
    """
    Create RAGFlow Chat Client for RAG-based generation.

    This client uses RAGFlow's Chat API which automatically retrieves
    relevant content from the configured knowledge bases (Cobol-Source-Code,
    Cobol_Manuals) and injects it into the prompt.

    Args:
        config: Configuration dictionary

    Returns:
        RAGFlowChatClient instance
    """
    if not config.get("ragflow_api_key") or not config.get("ragflow_chat_id"):
        raise ValueError(
            "RAGFlow requires both RAGFLOW_API_KEY and RAGFLOW_CHAT_ID to be set. "
            "Please configure these in Airflow Variables or environment variables."
        )

    return RAGFlowChatClient(
        api_base=config["ragflow_api_base"],
        api_key=config["ragflow_api_key"],
        chat_id=config["ragflow_chat_id"],
        similarity_threshold=config.get("ragflow_similarity_threshold", 0.2),
        vector_weight=config.get("ragflow_vector_weight", 0.3),
        top_n=config.get("ragflow_top_n", 8),
    )


def create_llm(config: dict):
    """
    Create LLM instance using LangChain (for non-RAGFlow usage).

    This function creates a VLLM-compatible ChatOpenAI instance.
    For RAGFlow with knowledge base, use create_ragflow_client() instead.
    """
    from langchain_openai import ChatOpenAI

    # Use VLLM API (OpenAI-compatible)
    return ChatOpenAI(
        model=config["vllm_model_name"],
        openai_api_base=config["vllm_api_base"],
        openai_api_key=config.get("vllm_api_key", "EMPTY"),
        max_tokens=config.get("max_tokens", 4096),
        temperature=config.get("temperature", 0.7),
    )


def read_cobol_files(input_dir: str) -> pd.DataFrame:
    """
    Read all COBOL source files from the input directory.

    Returns a DataFrame with columns: file_name, file_path, content
    """
    files_data = []

    # Supported file extensions
    extensions = ['.cob', '.cbl', '.txt', '.cobol', '.COB', '.CBL']

    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if any(file.endswith(ext) for ext in extensions):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    files_data.append({
                        'file_name': file,
                        'file_path': file_path,
                        'content': content
                    })
                except Exception as e:
                    logging.warning(f"Failed to read file {file_path}: {e}")

    return pd.DataFrame(files_data)


def invoke_llm_with_prompt(llm, cobol_code: str, prompt: str, system_prompt: str = SYSTEM_PROMPT) -> str:
    """
    Invoke LLM with the given COBOL code and prompt.

    Uses LangChain's message format for the conversation.
    """
    from langchain_core.messages import HumanMessage, SystemMessage

    # Combine COBOL code with the analysis prompt
    full_prompt = f"""Here is the COBOL source code to analyze:

```cobol
{cobol_code}
```

{prompt}"""

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=full_prompt)
    ]

    try:
        response = llm.invoke(messages)
        return response.content
    except Exception as e:
        logging.error(f"LLM invocation failed: {e}")
        return f"Error generating content: {str(e)}"


def invoke_ragflow_with_prompt(
    ragflow_client: RAGFlowChatClient,
    cobol_code: str,
    prompt: str
) -> str:
    """
    Invoke RAGFlow Chat with COBOL code and prompt.

    RAGFlow will automatically:
    1. Retrieve relevant content from knowledge bases (Cobol-Source-Code, Cobol_Manuals)
    2. Inject retrieved content into the {knowledge} placeholder
    3. Generate response using the LLM

    Args:
        ragflow_client: RAGFlow Chat client instance
        cobol_code: COBOL source code to analyze
        prompt: Analysis prompt

    Returns:
        LLM response with RAG-enhanced generation
    """
    # Combine COBOL code with the analysis prompt
    full_message = f"""Here is the COBOL source code to analyze:

```cobol
{cobol_code}
```

{prompt}"""

    try:
        response = ragflow_client.chat_with_retry(full_message)
        return response
    except Exception as e:
        logging.error(f"RAGFlow invocation failed: {e}")
        return f"Error generating content: {str(e)}"


def batch_process_with_llm(
    df: pd.DataFrame,
    llm,
    prompt: str,
    input_column: str = 'content',
    output_column: str = 'model_response'
) -> pd.DataFrame:
    """
    Process a DataFrame of COBOL files with LLM in batch (non-RAGFlow mode).

    This replaces the BatchRunComponent from Langflow.
    """
    results = []

    for idx, row in df.iterrows():
        cobol_code = row[input_column]
        logging.info(f"Processing batch item {idx + 1}/{len(df)}")

        response = invoke_llm_with_prompt(llm, cobol_code, prompt)
        results.append(response)

    df_result = df.copy()
    df_result[output_column] = results
    return df_result


def batch_process_with_ragflow(
    df: pd.DataFrame,
    ragflow_client: RAGFlowChatClient,
    prompt: str,
    input_column: str = 'content',
    output_column: str = 'model_response'
) -> pd.DataFrame:
    """
    Process a DataFrame of COBOL files with RAGFlow Chat in batch.

    This uses RAGFlow's RAG functionality to retrieve relevant content
    from the knowledge bases (Cobol-Source-Code, Cobol_Manuals) and
    generate enhanced responses.

    Args:
        df: DataFrame with COBOL files
        ragflow_client: RAGFlow Chat client
        prompt: Analysis prompt
        input_column: Column name containing COBOL code
        output_column: Column name for storing results

    Returns:
        DataFrame with added results column
    """
    results = []

    for idx, row in df.iterrows():
        cobol_code = row[input_column]
        logging.info(f"Processing batch item {idx + 1}/{len(df)} with RAGFlow")

        response = invoke_ragflow_with_prompt(ragflow_client, cobol_code, prompt)
        results.append(response)

    df_result = df.copy()
    df_result[output_column] = results
    return df_result


def combine_analysis_results(
    overview: str,
    flowchart: str,
    input_output: str,
    file_name: str
) -> str:
    """
    Combine all analysis results into a single markdown document.

    This replaces the CombineText components from Langflow.
    """
    delimiter = "\n\n"

    header = f"""# COBOL Program Analysis: {file_name}
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---
"""

    combined = delimiter.join([
        header,
        overview,
        flowchart,
        input_output
    ])

    return combined


def save_results(df: pd.DataFrame, output_dir: str):
    """
    Save analysis results to the output directory.

    Creates one markdown file per COBOL source file analyzed.
    """
    os.makedirs(output_dir, exist_ok=True)

    for idx, row in df.iterrows():
        file_name = row['file_name']
        output_file = os.path.join(output_dir, f"{os.path.splitext(file_name)[0]}_summary.md")

        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(row['combined_result'])
            logging.info(f"Saved summary to {output_file}")
        except Exception as e:
            logging.error(f"Failed to save {output_file}: {e}")


# ============================================================================
# Airflow Task Functions
# ============================================================================

def task_load_cobol_files(**context) -> str:
    """
    Task 1: Load COBOL source files from the input directory.

    Returns the DataFrame as JSON for XCom serialization.
    """
    logging.info("Starting to load COBOL files...")
    config = get_config()
    input_dir = config["cobol_input_dir"]

    df = read_cobol_files(input_dir)
    logging.info(f"Loaded {len(df)} COBOL files from {input_dir}")

    # Store DataFrame as JSON for XCom
    return df.to_json(orient='records')


def task_generate_program_overview(**context) -> str:
    """
    Task 2: Generate Program Overview for all COBOL files.

    Corresponds to the first BatchRunComponent chain in Langflow.
    Uses RAGFlow Chat with knowledge base if USE_RAGFLOW is enabled.
    """
    logging.info("Generating Program Overview...")

    # Get DataFrame from previous task
    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='load_cobol_files')
    df = pd.read_json(df_json, orient='records')

    config = get_config()

    if config.get("use_ragflow"):
        # Use RAGFlow Chat with knowledge base retrieval
        logging.info("Using RAGFlow Chat with knowledge base (Cobol-Source-Code, Cobol_Manuals)")
        ragflow_client = create_ragflow_client(config)
        df_result = batch_process_with_ragflow(
            df=df,
            ragflow_client=ragflow_client,
            prompt=PROGRAM_OVERVIEW_PROMPT,
            input_column='content',
            output_column='program_overview'
        )
    else:
        # Use standard VLLM
        logging.info("Using VLLM (non-RAG mode)")
        llm = create_llm(config)
        df_result = batch_process_with_llm(
            df=df,
            llm=llm,
            prompt=PROGRAM_OVERVIEW_PROMPT,
            input_column='content',
            output_column='program_overview'
        )

    logging.info("Program Overview generation completed")
    return df_result.to_json(orient='records')


def task_generate_flowchart(**context) -> str:
    """
    Task 3: Generate Flowchart (Mermaid) for all COBOL files.

    Corresponds to the second BatchRunComponent chain in Langflow.
    Uses RAGFlow Chat with knowledge base if USE_RAGFLOW is enabled.
    """
    logging.info("Generating Flowcharts...")

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='generate_program_overview')
    df = pd.read_json(df_json, orient='records')

    config = get_config()

    if config.get("use_ragflow"):
        logging.info("Using RAGFlow Chat with knowledge base")
        ragflow_client = create_ragflow_client(config)
        df_result = batch_process_with_ragflow(
            df=df,
            ragflow_client=ragflow_client,
            prompt=FLOWCHART_PROMPT,
            input_column='content',
            output_column='flowchart'
        )
    else:
        logging.info("Using VLLM (non-RAG mode)")
        llm = create_llm(config)
        df_result = batch_process_with_llm(
            df=df,
            llm=llm,
            prompt=FLOWCHART_PROMPT,
            input_column='content',
            output_column='flowchart'
        )

    logging.info("Flowchart generation completed")
    return df_result.to_json(orient='records')


def task_generate_input_output(**context) -> str:
    """
    Task 4: Generate Input/Output description for all COBOL files.

    Corresponds to the third BatchRunComponent chain in Langflow.
    Uses RAGFlow Chat with knowledge base if USE_RAGFLOW is enabled.
    """
    logging.info("Generating Input/Output descriptions...")

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='generate_flowchart')
    df = pd.read_json(df_json, orient='records')

    config = get_config()

    if config.get("use_ragflow"):
        logging.info("Using RAGFlow Chat with knowledge base")
        ragflow_client = create_ragflow_client(config)
        df_result = batch_process_with_ragflow(
            df=df,
            ragflow_client=ragflow_client,
            prompt=INPUT_OUTPUT_PROMPT,
            input_column='content',
            output_column='input_output'
        )
    else:
        logging.info("Using VLLM (non-RAG mode)")
        llm = create_llm(config)
        df_result = batch_process_with_llm(
            df=df,
            llm=llm,
            prompt=INPUT_OUTPUT_PROMPT,
            input_column='content',
            output_column='input_output'
        )

    logging.info("Input/Output generation completed")
    return df_result.to_json(orient='records')


def task_combine_and_save_results(**context) -> dict:
    """
    Task 5: Combine all analysis results and save to output directory.

    Corresponds to the CombineText and output components in Langflow.
    """
    logging.info("Combining and saving results...")

    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='generate_input_output')
    df = pd.read_json(df_json, orient='records')

    config = get_config()
    output_dir = config["cobol_output_dir"]

    # Combine results for each file
    combined_results = []
    for idx, row in df.iterrows():
        combined = combine_analysis_results(
            overview=row.get('program_overview', ''),
            flowchart=row.get('flowchart', ''),
            input_output=row.get('input_output', ''),
            file_name=row['file_name']
        )
        combined_results.append(combined)

    df['combined_result'] = combined_results

    # Save to output directory
    save_results(df, output_dir)

    result_summary = {
        'files_processed': len(df),
        'output_directory': output_dir,
        'files': df['file_name'].tolist()
    }

    logging.info(f"Processing completed. {len(df)} files processed.")
    return result_summary


# ============================================================================
# DAG Definition
# ============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='langflow_migration_dag',
    default_args=default_args,
    description='COBOL Summary Generator - Migrated from Langflow',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['cobol', 'langflow', 'llm', 'documentation'],
) as dag:

    # Task 1: Load COBOL files
    load_files = PythonOperator(
        task_id='load_cobol_files',
        python_callable=task_load_cobol_files,
        provide_context=True,
    )

    # Task 2: Generate Program Overview
    gen_overview = PythonOperator(
        task_id='generate_program_overview',
        python_callable=task_generate_program_overview,
        provide_context=True,
    )

    # Task 3: Generate Flowchart
    gen_flowchart = PythonOperator(
        task_id='generate_flowchart',
        python_callable=task_generate_flowchart,
        provide_context=True,
    )

    # Task 4: Generate Input/Output
    gen_io = PythonOperator(
        task_id='generate_input_output',
        python_callable=task_generate_input_output,
        provide_context=True,
    )

    # Task 5: Combine and Save
    combine_save = PythonOperator(
        task_id='combine_and_save_results',
        python_callable=task_combine_and_save_results,
        provide_context=True,
    )

    # Define task dependencies (sequential pipeline)
    load_files >> gen_overview >> gen_flowchart >> gen_io >> combine_save


# ============================================================================
# Standalone Execution Support
# ============================================================================

def run_standalone():
    """
    Run the pipeline standalone (outside of Airflow).

    Useful for testing and debugging.
    """
    import argparse

    parser = argparse.ArgumentParser(description='COBOL Summary Generator')
    parser.add_argument('--input-dir', type=str, help='Input directory containing COBOL files')
    parser.add_argument('--output-dir', type=str, help='Output directory for summaries')
    parser.add_argument('--vllm-api-base', type=str, help='VLLM API base URL')
    parser.add_argument('--vllm-model', type=str, help='VLLM model name')
    parser.add_argument('--use-ragflow', action='store_true',
                        help='Use RAGFlow Chat with knowledge base (requires RAGFLOW_API_KEY and RAGFLOW_CHAT_ID)')
    parser.add_argument('--ragflow-api-base', type=str, help='RAGFlow API base URL')
    parser.add_argument('--ragflow-api-key', type=str, help='RAGFlow API key')
    parser.add_argument('--ragflow-chat-id', type=str, help='RAGFlow Chat Assistant ID')
    args = parser.parse_args()

    # Override config with command line args
    config = get_config()
    if args.input_dir:
        config['cobol_input_dir'] = args.input_dir
    if args.output_dir:
        config['cobol_output_dir'] = args.output_dir
    if args.vllm_api_base:
        config['vllm_api_base'] = args.vllm_api_base
    if args.vllm_model:
        config['vllm_model_name'] = args.vllm_model
    if args.use_ragflow:
        config['use_ragflow'] = True
    if args.ragflow_api_base:
        config['ragflow_api_base'] = args.ragflow_api_base
    if args.ragflow_api_key:
        config['ragflow_api_key'] = args.ragflow_api_key
    if args.ragflow_chat_id:
        config['ragflow_chat_id'] = args.ragflow_chat_id

    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("COBOL Summary Generator - Standalone Mode")
    print("=" * 60)

    # Show configuration
    if config.get('use_ragflow'):
        print("\nMode: RAGFlow Chat with Knowledge Base")
        print(f"  RAGFlow API: {config['ragflow_api_base']}")
        print(f"  Chat ID: {config.get('ragflow_chat_id', 'Not set')}")
        print("  Knowledge Bases: Cobol-Source-Code, Cobol_Manuals")
        print(f"  Similarity Threshold: {config.get('ragflow_similarity_threshold', 0.2)}")
        print(f"  Vector Weight: {config.get('ragflow_vector_weight', 0.3)}")
        print(f"  Top N: {config.get('ragflow_top_n', 8)}")
    else:
        print("\nMode: VLLM (Standard LLM without RAG)")
        print(f"  VLLM API: {config['vllm_api_base']}")
        print(f"  Model: {config['vllm_model_name']}")

    # Step 1: Load files
    print("\n[Step 1/5] Loading COBOL files...")
    df = read_cobol_files(config['cobol_input_dir'])
    print(f"  Loaded {len(df)} files")

    if len(df) == 0:
        print("No COBOL files found. Exiting.")
        return

    # Step 2-4: Generate analyses
    ragflow_client = None
    llm = None

    if config.get('use_ragflow'):
        print("\n  Initializing RAGFlow Chat client...")
        ragflow_client = create_ragflow_client(config)
        ragflow_client.create_session()

        print("\n[Step 2/5] Generating Program Overview (with RAGFlow)...")
        df = batch_process_with_ragflow(df, ragflow_client, PROGRAM_OVERVIEW_PROMPT, 'content', 'program_overview')

        print("\n[Step 3/5] Generating Flowcharts (with RAGFlow)...")
        df = batch_process_with_ragflow(df, ragflow_client, FLOWCHART_PROMPT, 'content', 'flowchart')

        print("\n[Step 4/5] Generating Input/Output descriptions (with RAGFlow)...")
        df = batch_process_with_ragflow(df, ragflow_client, INPUT_OUTPUT_PROMPT, 'content', 'input_output')

        # Close RAGFlow session
        ragflow_client.close_session()
    else:
        llm = create_llm(config)

        print("\n[Step 2/5] Generating Program Overview...")
        df = batch_process_with_llm(df, llm, PROGRAM_OVERVIEW_PROMPT, 'content', 'program_overview')

        print("\n[Step 3/5] Generating Flowcharts...")
        df = batch_process_with_llm(df, llm, FLOWCHART_PROMPT, 'content', 'flowchart')

        print("\n[Step 4/5] Generating Input/Output descriptions...")
        df = batch_process_with_llm(df, llm, INPUT_OUTPUT_PROMPT, 'content', 'input_output')

    # Step 5: Combine and save
    print("\n[Step 5/5] Combining and saving results...")
    combined_results = []
    for idx, row in df.iterrows():
        combined = combine_analysis_results(
            overview=row.get('program_overview', ''),
            flowchart=row.get('flowchart', ''),
            input_output=row.get('input_output', ''),
            file_name=row['file_name']
        )
        combined_results.append(combined)

    df['combined_result'] = combined_results
    save_results(df, config['cobol_output_dir'])

    print("\n" + "=" * 60)
    print(f"Processing completed! {len(df)} files processed.")
    print(f"Output saved to: {config['cobol_output_dir']}")
    print("=" * 60)


if __name__ == '__main__':
    run_standalone()
