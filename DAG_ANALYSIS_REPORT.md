# Airflow DAG 深度分析报告

本仓库包含 **两个 Airflow DAG**，均用于将 COBOL 源代码自动转换为结构化业务文档。以下按要求逐一分析。

---

# DAG 1: `code_summary`（Langflow 迁移版）

**文件**: `langflow_migration_dag.py` (881 行)

## 1. DAG 概览

| 属性 | 值 |
|------|-----|
| **DAG ID** | `code_summary` |
| **调度周期** | `schedule_interval=None` — 仅手动触发，无自动调度 |
| **catchup** | `False` — 不回填历史未执行的调度 |
| **tags** | `cobol`, `langflow`, `llm`, `documentation` |
| **描述** | COBOL Summary Generator - Migrated from Langflow |

### default_args 关键配置

```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,      # 任务不依赖上一次 DAG Run 的成功
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,     # 失败不发邮件
    'email_on_retry': False,       # 重试不发邮件
    'retries': 1,                  # 失败后重试 1 次
    'retry_delay': timedelta(minutes=5),  # 重试间隔 5 分钟
}
```

## 2. 任务节点 (Tasks) 拆解

共 **5 个 Task**，全部使用 `PythonOperator`。

| # | Task ID | Operator | 功能说明 |
|---|---------|----------|----------|
| 1 | `load_cobol_files` | PythonOperator | 扫描输入目录，读取所有 COBOL 源文件（支持 `.cob`, `.cbl`, `.txt`, `.cobol` 等扩展名），将文件名、路径和内容封装成 Pandas DataFrame，序列化为 JSON 通过 XCom 传递给下游 |
| 2 | `generate_program_overview` | PythonOperator | 为每个 COBOL 文件生成**程序概述**文档（Program ID、功能描述、主要流程）。根据 `USE_RAGFLOW` 配置决定使用 RAGFlow（带知识库检索增强）或直接调用 VLLM |
| 3 | `generate_flowchart` | PythonOperator | 为每个 COBOL 文件生成 **Mermaid 语法的流程图**，可视化 PROCEDURE DIVISION 的执行流程 |
| 4 | `generate_input_output` | PythonOperator | 为每个 COBOL 文件生成**输入/输出描述**文档，列出所有输入文件和输出文件的名称、用途和关键字段 |
| 5 | `combine_and_save_results` | PythonOperator | 将前三个生成任务的结果合并为一个完整的 Markdown 文档，按文件逐个保存到输出目录（`{文件名}_summary.md`） |

## 3. 依赖关系 (Dependencies)

```
load_cobol_files >> generate_program_overview >> generate_flowchart >> generate_input_output >> combine_and_save_results
```

```
┌──────────────────┐   ┌────────────────────────────┐   ┌─────────────────────┐   ┌───────────────────────┐   ┌────────────────────────────┐
│ load_cobol_files │──>│ generate_program_overview   │──>│ generate_flowchart  │──>│ generate_input_output │──>│ combine_and_save_results   │
└──────────────────┘   └────────────────────────────┘   └─────────────────────┘   └───────────────────────┘   └────────────────────────────┘
```

**纯线性流水线**，无分支逻辑、无触发规则（TriggerRule）。每个 Task 必须等上游成功后才执行。

## 4. 关键逻辑说明

### XCom 参数传递

本 DAG **重度依赖 XCom** 在 Task 之间传递数据。每个任务将 Pandas DataFrame 序列化为 JSON 字符串作为返回值推送到 XCom，下游通过 `ti.xcom_pull(task_ids='...')` 拉取：

```python
# Task 1 推送数据
def task_load_cobol_files(**context) -> str:
    df = read_cobol_files(input_dir)
    return df.to_json(orient='records')  # XCom push (返回值自动推送)

# Task 2 拉取数据
def task_generate_program_overview(**context) -> str:
    ti = context['ti']
    df_json = ti.xcom_pull(task_ids='load_cobol_files')  # XCom pull
    df = pd.read_json(df_json, orient='records')
```

每个生成任务（Task 2/3/4）在 DataFrame 上新增一列（`program_overview`、`flowchart`、`input_output`），持续向下传递累积结果。

### Jinja 模板 / Macros

**未使用** Airflow 原生的 Jinja 模板或 Macros。所有参数通过 Python 代码（`get_config()` 函数）从 Airflow Variables 或环境变量读取。

### 动态生成的 Task

**没有**动态生成的 Task。5 个任务在 DAG 定义时静态声明。

### 双模式架构（RAGFlow vs VLLM）

代码通过 `USE_RAGFLOW` 配置标志实现两种 LLM 调用路径：

- **RAGFlow 模式** (`use_ragflow=True`)：通过 `RAGFlowChatClient` 调用 RAGFlow Chat API，自动从知识库（Cobol-Source-Code、Cobol_Manuals）检索相关内容，注入到 `{knowledge}` 占位符中实现 RAG 增强生成
- **VLLM 模式** (`use_ragflow=False`)：通过 LangChain 的 `ChatOpenAI` 直接调用 VLLM API（由 `src.config.get_llm()` 创建）

### 配置管理优先级

`get_config()` 实现了三层配置回退机制：
1. **Airflow Variables**（优先）
2. **环境变量**（Airflow Variables 不可用时回退）
3. **DEFAULT_CONFIG 硬编码默认值**（最终兜底）

---
---

# DAG 2: `cobol_business_summary_v3`（增强版 v3.0）

**文件**: `code_summary.py` (2,727 行)

## 1. DAG 概览

| 属性 | 值 |
|------|-----|
| **DAG ID** | `cobol_business_summary_v3` |
| **调度周期** | `schedule_interval=None` — 仅手动触发 |
| **catchup** | `False` |
| **tags** | `cobol`, `mainframe`, `modernization`, `documentation`, `v3`, `ragflow`, `enhanced-parser` |
| **描述** | COBOL Business Summary Generator v3.0 - Enhanced with Column-Sensitive Parsing, Pre-Analysis Context, RAGFlow Integration & Validation |
| **doc_md** | 使用了 `__doc__`，将模块级文档字符串作为 DAG 的 Markdown 文档展示在 Airflow UI |

### default_args 关键配置

```python
default_args = {
    'owner': 'mainframe-modernization',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,                          # 比 v1 多 1 次重试 (2 vs 1)
    'retry_delay': timedelta(minutes=2),   # 比 v1 更短的重试间隔 (2 分钟 vs 5 分钟)
}
```

与 DAG 1 的关键差异：`retries=2`（更多重试）、`retry_delay=2min`（更短间隔）、`owner='mainframe-modernization'`（更具团队归属标识）。

## 2. 任务节点 (Tasks) 拆解

共 **11 个 Task**，全部使用 `PythonOperator`。

| # | Task ID | Operator | 功能说明 |
|---|---------|----------|----------|
| 1 | `load_and_enrich_files` | PythonOperator | 加载 COBOL 源文件并**解析 COPY 语句内联 Copybook 内容**。使用增强解析器（v3.0）进行列敏感的 COBOL 解析（区分 Area A/B）、提取控制流、段落和业务模式，生成 LLM 上下文摘要 |
| 2 | `chunk_large_programs` | PythonOperator | 对超过 token 限制的大程序按**语义边界**（DIVISION/SECTION）进行智能分块，而非简单按行数切割。增强模式下会为 PROCEDURE DIVISION 块保留 DATA DIVISION 上下文 |
| 3 | `generate_overview_parallel` | PythonOperator | **并行**生成程序概述。使用 `ThreadPoolExecutor` 多线程处理。注入预分析的代码结构上下文到 LLM 提示词中。RAGFlow 模式下串行处理以维护会话上下文 |
| 4 | `generate_flowchart_parallel` | PythonOperator | **并行**生成业务级 Mermaid 流程图。仅处理 PROCEDURE DIVISION 块或完整程序。注入预分析的控制流信息 |
| 5 | `generate_io_parallel` | PythonOperator | **并行**生成输入/输出文档，包含业务上下文的表格化输出（Input Sources、Output Destinations、Working Storage、External Calls） |
| 6 | `generate_structure_parallel` | PythonOperator | **并行**生成程序结构分析，覆盖四大 DIVISION（IDENTIFICATION、ENVIRONMENT、DATA、PROCEDURE）的详细说明 |
| 7 | `generate_core_logic_parallel` | PythonOperator | **并行**生成核心逻辑文档：列出所有函数/段落（含行号）、每个函数的 Mermaid 子流程图、详细步骤描述（每函数至少 150 词） |
| 8 | `generate_dependencies_parallel` | PythonOperator | **并行**生成依赖文档：列出所有 COPY 语句（Copybook 依赖）和 CALL 语句（子程序调用） |
| 9 | `aggregate_chunks` | PythonOperator | 将分块处理的结果**聚合**回完整的程序级摘要。按文件名分组，合并各节（overview 用分隔线连接，flowchart 取最佳，core_logic 取 PROCEDURE 块的结果） |
| 10 | `validate_summaries` | PythonOperator | 对生成的摘要进行**多层验证**：检查提及的段落/节/文件名是否存在于源码中，识别幻觉内容，计算置信度分数。增强模式使用解析后的结构进行更精确的验证 |
| 11 | `combine_and_save` | PythonOperator | 将所有章节（概述、流程图、I/O、结构、核心逻辑、依赖）和验证报告组合成最终 Markdown 文档，保存到输出目录 |

## 3. 依赖关系 (Dependencies)

```
load_and_enrich_files >> chunk_large_programs >> generate_overview_parallel >> generate_flowchart_parallel >> generate_io_parallel >> generate_structure_parallel >> generate_core_logic_parallel >> generate_dependencies_parallel >> aggregate_chunks >> validate_summaries >> combine_and_save
```

```
┌─────────────────────┐   ┌──────────────────────┐   ┌─────────────────────────┐   ┌───────────────────────────┐   ┌──────────────────────┐
│ load_and_enrich     │──>│ chunk_large_programs  │──>│ generate_overview       │──>│ generate_flowchart        │──>│ generate_io          │
│ _files              │   │                      │   │ _parallel               │   │ _parallel                 │   │ _parallel            │
└─────────────────────┘   └──────────────────────┘   └─────────────────────────┘   └───────────────────────────┘   └──────────────────────┘
                                                                                                                            │
                        ┌───────────────────────────────────────────────────────────────────────────────────────────────────┘
                        ▼
┌─────────────────────────┐   ┌──────────────────────────────┐   ┌─────────────────────────────┐   ┌──────────────────────┐
│ generate_structure      │──>│ generate_core_logic          │──>│ generate_dependencies       │──>│ aggregate_chunks     │
│ _parallel               │   │ _parallel                    │   │ _parallel                   │   │                      │
└─────────────────────────┘   └──────────────────────────────┘   └─────────────────────────────┘   └──────────────────────┘
                                                                                                            │
                        ┌───────────────────────────────────────────────────────────────────────────────────┘
                        ▼
                ┌──────────────────────┐   ┌──────────────────────┐
                │ validate_summaries   │──>│ combine_and_save     │
                └──────────────────────┘   └──────────────────────┘
```

**依然是纯线性流水线**（11 个节点顺序链接），无 BranchPythonOperator、无自定义 TriggerRule。

> **注意**：虽然 Task 之间是线性依赖，但 Task 3~8 的**内部实现**使用了 `ThreadPoolExecutor` 进行多文件/多块的**并行处理**（`parallel_workers=3`）。这是 Python 级别的并行，而非 Airflow Task 级别的并行。RAGFlow 模式下由于需要维护会话上下文，强制串行处理。

## 4. 关键逻辑说明

### XCom 参数传递（与 DAG 1 相同模式）

所有 Task 通过 XCom 传递 JSON 序列化的 DataFrame。每个 Task 从上游拉取 DataFrame → 新增列 → 序列化后推给下游：

```python
# 拉取上游数据
ti = context['ti']
df_json = ti.xcom_pull(task_ids='chunk_large_programs')
df = pd.read_json(df_json, orient='records')

# 处理后推送（返回值自动推送到 XCom）
df['program_overview'] = [r[1] for r in results]
return df.to_json(orient='records')
```

**累积传递的列**：
- Task 1: `file_name`, `enriched_content`, `llm_context`, `control_flow`, `paragraphs`, `business_patterns` 等
- Task 2: 新增 `chunk_id`, `chunk_type`, `chunk_content`, `data_context`, `is_chunked`
- Task 3~8: 分别新增 `program_overview`, `flowchart`, `input_output`, `program_structure`, `core_logic`, `dependencies`
- Task 10: 新增 `validation_*` 系列列

### Jinja 模板 / Macros

**未使用** Airflow 原生的 Jinja 模板或 Macros。

但在 **Prompt 模板** 层面使用了 Python 的 `str.format()` 进行变量注入，例如：

```python
PROGRAM_OVERVIEW_PROMPT_V2 = """Analyze this COBOL program...
COBOL Source Code:
```cobol
{cobol_code}
```"""

# 调用时
prompt = PROGRAM_OVERVIEW_PROMPT_V2.format(cobol_code=row['chunk_content'][:30000])
```

RAGFlow 的系统提示词使用了 `{knowledge}` 占位符，由 RAGFlow 服务端自动注入检索到的知识库内容：

```python
RAGFLOW_SYSTEM_PROMPT = """...
Here is the knowledge base:
{knowledge}
The above is the knowledge base."""
```

### 动态生成的 Task

**没有**动态生成的 Task。所有 11 个任务在 DAG 定义时静态声明。

不过，**条件跳过逻辑**存在于 Task 内部：
- Task 6 (`generate_structure_parallel`): 检查 `config.generate_structure`，如果为 `False` 则跳过生成
- Task 7 (`generate_core_logic_parallel`): 检查 `config.generate_core_logic`
- Task 8 (`generate_dependencies_parallel`): 检查 `config.generate_dependencies`
- Task 10 (`validate_summaries`): 检查 `config.enable_validation`

这些不是 Airflow 级别的 Task 跳过（Task 仍会运行并标记为成功），而是在 Python 函数内部通过配置标志控制是否执行实际逻辑。

### 增强解析器双轨机制 (v3.0)

代码通过 try/except import 实现**增强解析器与遗留解析器的双轨兼容**：

```python
try:
    from cobol_parser_enhanced import EnhancedCOBOLParser, ...
    from cobol_analysis_integration import ThreadSafeLLMClient, ...
    ENHANCED_PARSER_AVAILABLE = True
except ImportError:
    ENHANCED_PARSER_AVAILABLE = False
```

运行时根据 `ENHANCED_PARSER_AVAILABLE` 和 `config.use_enhanced_parser` 决定使用哪条路径。增强解析器提供：
- **列敏感解析**（Area A / Area B 识别）
- **完整的 COPY REPLACING 实现**
- **预分析的控制流和业务模式注入到 LLM 提示词**
- **DATA DIVISION 上下文保留**给 PROCEDURE DIVISION 的分块

### 核心类说明

| 类名 | 位置 | 用途 |
|------|------|------|
| `CopybookResolver` | code_summary.py:289 | 解析 COPY 语句，递归解析嵌套 Copybook，支持 REPLACING 子句 |
| `COBOLChunker` | code_summary.py:470 | 按 DIVISION/SECTION 语义边界智能分块，避免跨逻辑单元切割 |
| `BusinessRuleExtractor` | code_summary.py:633 | 提取控制流（PERFORM/GO TO）、识别业务模式（验证、计算、文件处理、日期、错误处理） |
| `SummaryValidator` | code_summary.py:745 | 验证生成摘要中提到的代码元素是否真实存在于源码中，计算置信度分数 |
| `RAGFlowClient` | code_summary.py:914 | 使用官方 ragflow-sdk 调用 RAGFlow Chat API，支持会话管理、自动重试、超时控制 |

---

# 两个 DAG 的对比总结

| 维度 | DAG 1 (`code_summary`) | DAG 2 (`cobol_business_summary_v3`) |
|------|------------------------|-------------------------------------|
| **定位** | Langflow 迁移版，轻量级 | 生产增强版，功能全面 |
| **Task 数量** | 5 | 11 |
| **输出章节** | 3（概述、流程图、I/O） | 6（概述、流程图、I/O、结构分析、核心逻辑、依赖） |
| **Copybook 处理** | 无 | 有（支持递归解析、REPLACING 子句） |
| **代码分块** | 无 | 有（按 DIVISION/SECTION 语义边界） |
| **并行处理** | 无（逐个文件串行） | 有（ThreadPoolExecutor，3 workers） |
| **准确性验证** | 无 | 有（多层验证 + 置信度评分） |
| **RAGFlow 集成** | 手写 HTTP 客户端 | 使用官方 ragflow-sdk |
| **增强解析** | 无 | 有（列敏感、预分析上下文注入） |
| **重试策略** | 1 次 / 5 分钟 | 2 次 / 2 分钟 |
| **配置来源** | Airflow Variables > 环境变量 | 环境变量（无 Airflow Variables 层） |
