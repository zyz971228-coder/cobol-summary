# COBOL Parser Enhancement Guide

## 代码审查总结 (Code Review Summary)

### 审查范围
- 文件: `code_summary.py` (2,514 行)
- 版本: v2.0.0
- 审查日期: 2026-02-04

---

## 一、发现的关键问题 (Critical Issues)

### 1. COBOL 解析逻辑准确性

| 问题 | 严重程度 | 位置 | 说明 |
|------|---------|------|------|
| Area A/B 列敏感性未处理 | 🔴 严重 | L265, L442-455 | 正则 `^\s{6}\s*` 无法区分 Area A (列8-11) 和 Area B (列12-72) |
| 注释行未过滤 | 🔴 严重 | 整个解析 | 列7为 `*`, `/`, `D` 的注释行被误识别 |
| 续行未处理 | 🟡 中等 | 整个解析 | 列7为 `-` 的续行未拼接 |
| COPY REPLACING 未实现 | 🔴 严重 | L328-331 | `_parse_replacing` 只返回原文本 |
| 段落误匹配 | 🟡 中等 | L452-455 | 可能匹配 DATA DIVISION 中的元素 |
| PERFORM 变体不完整 | 🟡 中等 | L629-631 | 缺少 UNTIL, VARYING, TIMES 处理 |
| CALL 语句未提取 | 🟡 中等 | BusinessRuleExtractor | 缺失程序间调用分析 |

### 2. 摘要生成质量

| 问题 | 严重程度 | 说明 |
|------|---------|------|
| 预分析结果未用于增强提示 | 🔴 严重 | `BusinessRuleExtractor` 提取的 control_flow 未注入 LLM 提示 |
| 分块导致上下文丢失 | 🟡 中等 | PROCEDURE DIVISION chunk 看不到 DATA DIVISION 定义 |
| 业务模式识别过于粗糙 | 🟡 中等 | 只检查关键词存在，未分析具体业务含义 |

### 3. 代码性能与健壮性

| 问题 | 严重程度 | 位置 | 说明 |
|------|---------|------|------|
| 嵌套 Copybook 替换 Bug | 🟡 中等 | L377-381 | 相同名称多个 COPY 只替换第一个 |
| 线程安全风险 | 🟡 中等 | L1604-1616 | LLM 实例在多线程间共享 |
| 大量重复正则编译 | 🟢 低 | 多处 | 每次调用重新编译正则 |

---

## 二、解决方案架构

### 新增模块

```
cobol-summary/
├── code_summary.py              # 原有 DAG (保留)
├── cobol_parser_enhanced.py     # 新增：增强 COBOL 解析器
├── cobol_analysis_integration.py # 新增：集成层
└── COBOL_PARSER_ENHANCEMENT_GUIDE.md  # 本文档
```

### 核心类重构

#### 1. COBOLLineParser (新增)
- 列敏感解析（固定格式 vs 自由格式）
- 注释行识别（`*`, `/`, `D`）
- 续行处理（`-`）

```python
@dataclass
class COBOLLine:
    line_number: int
    raw_text: str
    sequence_area: str  # 列 1-6
    indicator: str      # 列 7
    area_a: str         # 列 8-11
    area_b: str         # 列 12-72
    content: str        # 列 8-72
    is_comment: bool
    is_continuation: bool
```

#### 2. EnhancedCopybookResolver (重构)
- 完整 REPLACING 子句解析和执行
- 多行 COPY 语句处理
- 更好的库路径搜索

```python
# 原始代码
def _parse_replacing(self, replacing_text: str) -> List[str]:
    return [replacing_text.strip()]  # ❌ 无实际替换

# 重构后
def _parse_replacing(self, replacing_text: str) -> List[Tuple[str, str]]:
    # 解析 ==old== BY ==new== 格式
    replacements = []
    for match in REPLACING_ITEM_PATTERN.finditer(replacing_text):
        old_text = match.group(1).strip('==')
        new_text = match.group(2).strip('==')
        replacements.append((old_text, new_text))
    return replacements

def _apply_replacing(self, content: str, replacements: List[Tuple[str, str]]) -> str:
    for old, new in replacements:
        content = re.sub(rf'\b{re.escape(old)}\b', new, content, flags=re.IGNORECASE)
    return content
```

#### 3. EnhancedCOBOLParser (重构)
- Area A 检测用于 DIVISION/SECTION/PARAGRAPH 识别
- 仅在 PROCEDURE DIVISION 内识别段落
- 完整 PERFORM 变体和 CALL 提取

```python
def _is_in_area_a(self, line: COBOLLine) -> bool:
    """段落名必须从 Area A 开始"""
    return bool(line.area_a.strip()) and not line.is_comment

# PERFORM 变体
perform_patterns = [
    r'PERFORM\s+(\w+)(?:\s+THRU\s+(\w+))?',
    r'PERFORM\s+(\w+)\s+UNTIL\s+',
    r'PERFORM\s+(\w+)\s+VARYING\s+',
    r'PERFORM\s+(\w+)\s+\d+\s+TIMES',
]

# CALL 语句
call_patterns = [
    r"CALL\s+'([^']+)'",      # CALL 'PROGRAM'
    r'CALL\s+"([^"]+)"',      # CALL "PROGRAM"
    r'CALL\s+(\w[\w-]*)',     # CALL WS-PROG-NAME
]
```

#### 4. 增强的提示模板
将预分析结果注入 LLM 提示：

```python
def create_enhanced_overview_prompt(cobol_code, llm_context, data_context):
    return f"""
## Pre-Analyzed Code Structure (Use this to guide your analysis):
{llm_context}

## Data Structures (Reference for understanding procedure logic):
{data_context}

## Your Task:
Analyze this COBOL program...

### COBOL Source Code:
```cobol
{cobol_code}
```"""
```

#### 5. 线程安全 LLM 客户端

```python
class ThreadSafeLLMClient:
    def __init__(self, llm_factory):
        self._llm_factory = llm_factory
        self._local = threading.local()  # 线程本地存储

    def get_client(self):
        if not hasattr(self._local, 'client'):
            self._local.client = self._llm_factory()
        return self._local.client
```

---

## 三、集成步骤

### 步骤 1: 更新导入

在 `code_summary.py` 顶部添加：

```python
# Enhanced COBOL parsing
from cobol_parser_enhanced import (
    EnhancedCopybookResolver,
    EnhancedCOBOLParser,
    EnhancedBusinessRuleExtractor,
    EnhancedCOBOLChunker,
)
from cobol_analysis_integration import (
    ThreadSafeLLMClient,
    process_cobol_file_enhanced,
    create_enhanced_overview_prompt,
    create_enhanced_flowchart_prompt,
    create_enhanced_core_logic_prompt,
    validate_summary_enhanced,
)
```

### 步骤 2: 替换 Task 1 (load_and_enrich)

```python
def task_load_and_enrich_cobol_files(**context) -> str:
    """Enhanced Task 1: Use new parser."""
    config = get_config()
    input_dir = config["cobol_input_dir"]
    copybook_dirs = config["copybook_dirs"] + [input_dir]

    enriched_files = []
    for file_path in glob.glob(f"{input_dir}/**/*.cob", recursive=True):
        processed = process_cobol_file_enhanced(file_path, copybook_dirs, config)

        enriched_files.append({
            'file_name': processed['file_name'],
            'program_name': processed['program_name'],
            'original_content': processed['original_source'],
            'enriched_content': processed['enriched_source'],
            'llm_context': processed['llm_context'],  # 新增
            'control_flow': json.dumps(processed['control_flow']),  # 新增
            'paragraphs': json.dumps(processed['paragraphs']),  # 新增
            'chunks': processed['chunks'],
            ...
        })

    return pd.DataFrame(enriched_files).to_json(orient='records')
```

### 步骤 3: 更新生成任务使用增强提示

```python
def task_generate_overview_parallel(**context) -> str:
    """Enhanced Task 3: Use pre-analyzed context."""
    ...
    for idx, row in df.iterrows():
        prompt = create_enhanced_overview_prompt(
            cobol_code=row['enriched_content'],
            llm_context=row['llm_context'],  # 注入预分析
            data_context=row.get('data_context', '')
        )
        ...
```

### 步骤 4: 替换验证逻辑

```python
def task_validate_summaries(**context) -> str:
    """Enhanced Task 7: Use parsed structure validation."""
    ...
    for idx, row in df.iterrows():
        # 需要重建 parser (或从序列化恢复)
        parser = EnhancedCOBOLParser(row['enriched_content'])
        parser.parse()

        validation = validate_summary_enhanced({
            'parser': parser,
            'original_source': row['original_content'],
            'enriched_source': row['enriched_content'],
        }, combined_summary)
        ...
```

---

## 四、测试验证

### 测试用例 1: 列敏感性

```cobol
000100 IDENTIFICATION DIVISION.
000200 PROGRAM-ID. TEST-PROG.
000300*THIS IS A COMMENT
000400 PROCEDURE DIVISION.
000500     MAIN-PARA.
000600         PERFORM SUB-PARA.
```

预期：
- 第3行识别为注释 ✓
- MAIN-PARA 识别为段落 (Area A) ✓
- PERFORM 语句在 Area B ✓

### 测试用例 2: COPY REPLACING

```cobol
       COPY CUSTOMER-REC REPLACING ==:PREFIX:== BY ==WS-==.
```

预期：
- 原始 `:PREFIX:-NAME` 替换为 `WS-NAME`

### 测试用例 3: PERFORM 变体

```cobol
       PERFORM PROCESS-RECORD UNTIL END-OF-FILE.
       PERFORM CALC-TOTAL VARYING I FROM 1 BY 1 UNTIL I > 10.
       PERFORM INIT-PARA 5 TIMES.
       PERFORM RANGE-START THRU RANGE-END.
```

预期：所有变体都被识别并提取

---

## 五、性能考量

### 内存优化
- 大文件分块处理
- Copybook 缓存避免重复读取
- 流式 JSON 序列化（pandas chunks）

### CPU 优化
- 预编译正则（类级别常量）
- 线程池并行处理
- 延迟解析（按需解析 PROCEDURE DIVISION）

### 网络优化
- LLM 请求批处理
- 重试指数退避
- 连接复用

---

## 六、向后兼容性

- 原有 `code_summary.py` 保持不变
- 新模块作为可选增强
- 配置开关控制是否使用增强解析

```python
DEFAULT_CONFIG = {
    ...
    "use_enhanced_parser": True,  # 新增配置
    ...
}
```

---

## 七、总结

### 修复的问题

| 原问题 | 解决方案 | 影响 |
|--------|---------|------|
| Area A/B 未区分 | COBOLLine 数据类 + 列解析 | 准确的段落识别 |
| 注释行误识别 | indicator 字段检查 | 避免注释中的关键词干扰 |
| REPLACING 未实现 | 完整解析 + 替换逻辑 | Copybook 数据结构正确 |
| 预分析未用于提示 | 增强提示模板 | 更高质量的 LLM 输出 |
| 上下文丢失 | DATA DIVISION 摘要注入 | PROCEDURE 分析更完整 |
| 线程安全风险 | ThreadSafeLLMClient | 并行处理稳定性 |

### 文件清单

1. `cobol_parser_enhanced.py` - 增强 COBOL 解析器 (新增)
2. `cobol_analysis_integration.py` - 集成模块 (新增)
3. `COBOL_PARSER_ENHANCEMENT_GUIDE.md` - 本文档 (新增)

---

*文档版本: 3.0.0*
*最后更新: 2026-02-04*
