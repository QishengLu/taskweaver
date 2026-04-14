# CLAUDE.md — TaskWeaver / RCA Agent

## 项目概述

基于微软 TaskWeaver 框架的根因分析（RCA）Agent。TaskWeaver 采用 Planner + CodeInterpreter 多角色架构，通过代码生成调用 DuckDB 插件查询 parquet 遥测数据，自动分析故障传播路径并定位根因服务。

**与 thinkdepthai 的区别：**
- thinkdepthai 使用 LangGraph 单 Agent 循环（llm_call → tool_node → compress）
- TaskWeaver 使用 Planner 规划 + CodeInterpreter 生成 Python 代码 + Plugin 执行的多角色架构
- TaskWeaver 的工具调用是通过生成 Python 代码隐式调用插件，而非直接 tool_calls

---

## Prompt 映射策略

所有 agent 共用同一套 RolloutRunner prompt 模版（tools 和 prompt 相同），框架逻辑各自保留。

| RolloutRunner 字段 | TaskWeaver 映射 | 注入位置 |
|---|---|---|
| `system_prompt` | → Planner system prompt | 运行时动态追加到 `planner_prompt.yaml` 的 `instruction_template` |
| `compress_user_prompt` | → Planner system prompt | 同上，追加为输出格式要求 |
| `user_prompt` | → `send_message()` 用户消息 | 增强版 question（含事件描述 + 调查策略） |
| `compress_system_prompt` | 不使用 | TaskWeaver 无独立 compress 步骤 |
| `data_dir` | → `send_message()` 用户消息 | 追加数据位置提示 |

**注入机制：**
- `build_planner_prompt_yaml()` 读取 upstream `planner_prompt.yaml`，追加 `system_prompt` + `compress_user_prompt`（`{`/`}` 转义为 `{{`/`}}`），写入临时 YAML 文件
- **think_tool 过滤**：系统会自动过滤 system_prompt 中的 think_tool 引用（TaskWeaver 无 tool_calls 机制，使用代码生成调用插件）
- 通过 `config_override["planner.prompt_file_path"]` 指向临时文件
- 不修改框架源码，Planner 原生 `.format()` 链路不受影响

**关键原则：**
- Planner system prompt = upstream 框架协调指令 + RCA 领域指令 + 输出格式
- 用户消息 = user_prompt（增强版 question）+ data_dir
- 输出由 Planner 最终回复直接提取（TaskWeaver 原生流程）

---

## 入口文件

| 文件 | 作用 |
|------|------|
| `agent_runner.py` | **RolloutRunner 接口**。stdin/stdout JSON，兼容统一评测管线 |
| `auto_rca_script.py` | 独立运行脚本（读取 problem.json，手动调试用） |

---

## Agent 核心架构

### TaskWeaver 多角色执行流

```
User Query (user_prompt + data_dir)
  │
  ▼
Planner（LLM 任务分解）
  │  system prompt: upstream planner_prompt.yaml + system_prompt + compress_user_prompt
  │  输出：init_plan / plan / current_plan_step
  │
  ▼
CodeInterpreter（LLM 代码生成）
  │  生成 Python 代码，调用 plugin 函数：
  │    list_tables_in_directory(directory)
  │    get_schema(parquet_file)
  │    query_parquet_files(parquet_files, query, limit)
  │
  ▼
CodeExecutor（本地 kernel 执行）
  │  执行代码，返回执行结果
  │
  ▼
Planner（汇总 + 下一步决策）
  │  循环直到分析完成
  │
  ▼
Planner → User（最终 CausalGraph JSON）
  │
  ▼
stdout JSON { output, trajectory }
```

### 关键特性

- **代码生成式工具调用**：不同于直接 tool_calls，TaskWeaver 生成完整 Python 代码来调用插件
- **多轮规划**：Planner 可在多轮内迭代调整分析策略
- **Prompt 压缩**：长对话自动压缩，避免 context 溢出
- **插件式工具**：工具以 Plugin 类 + YAML schema 定义，易于扩展
- **原生输出**：Planner 最终回复直接作为输出，无额外 compress 步骤

---

## RCA 工具（Plugins）

位于 `project/plugins/` 目录，与 thinkdepthai 的 `src/rca_tools.py` 功能对齐：

| Plugin | 文件 | 功能 |
|--------|------|------|
| `list_tables_in_directory` | `list_tables_in_directory.py/.yaml` | 扫描目录中所有 parquet 文件，返回文件名/行数/列数 |
| `get_schema` | `get_schema.py/.yaml` | 获取 parquet 文件的列名和类型 |
| `query_parquet_files` | `query_parquet_files.py/.yaml` | DuckDB SQL 查询 parquet 数据，token 限制 5000 |

其他插件（anomaly_detection、klarna_search 等）已禁用，不影响 RCA 任务。

---

## 配置

### TaskWeaver 配置（`project/taskweaver_config.json`）

```json
{
  "llm.api_base": "https://coding.dashscope.aliyuncs.com/v1",
  "llm.api_type": "openai",
  "llm.api_key": "",
  "llm.model": "qwen3.5-plus",
  "execution_service.kernel_mode": "local",
  "session.max_internal_chat_round_num": 50,  // 仅作为 fallback；agent_runner.py 用 config_override 设 200
  "planner.prompt_compression": true,
  "code_generator.prompt_compression": true,
  "round_compressor.rounds_to_compress": 2,
  "round_compressor.rounds_to_retain": 3
}
```

**注意**：
- `planner.prompt_file_path` 由 `agent_runner.py` 运行时通过 `config_override` 指向动态生成的临时 YAML。
- `config_override` 同时覆盖 `llm.model`、`llm.api_key`、`llm.api_base`，从 `RCA_MODEL` 和 `.env` 环境变量动态读取，config.json 中的值仅作为 fallback。

### 环境变量

在项目根目录创建 `.env`：

```
# Qwen3.5-plus via Aliyun Coding Plan
OPENAI_API_KEY=                                          # 运行前命令行传入
OPENAI_BASE_URL=https://coding.dashscope.aliyuncs.com/v1
```

---

## RolloutRunner 集成

### agent_runner.py stdin/stdout 接口

**stdin (6 字段):**
```json
{
  "question": "augmented_question 原文",
  "system_prompt": "RCA_ANALYSIS_SP (已 format date)",
  "user_prompt": "RCA_ANALYSIS_UP (已 format incident_description)",
  "compress_system_prompt": "COMPRESS_FINDINGS_SP",
  "compress_user_prompt": "COMPRESS_FINDINGS_UP",
  "data_dir": "/path/to/eval-data/<exp_id>/data_XXXXXXXX"
}
```

**stdout (最后一行 JSON):**
```json
{
  "output": "{\"nodes\": [...], \"edges\": [...], \"root_causes\": [...]}",
  "trajectory": [
    {"role": "assistant", "content": "...", "tool_calls": [...]},
    {"role": "tool", "content": "...", "tool_call_id": "..."}
  ]
}
```

### 数据流

```
RolloutRunner 构建 payload
  → stdin 传入 agent_runner.py
  → system_prompt + compress_user_prompt → 动态生成 planner_prompt.yaml（Planner system prompt）
  → user_prompt + data_dir → 构建用户消息
  → 将 data_dir 的 parquet 文件软链接到 session cwd
  → TaskWeaverApp(config_override={"planner.prompt_file_path": temp.yaml})
  → session.send_message(user_message)
  → TaskWeaver 内部循环：Planner ↔ CodeInterpreter（10+ 轮）
  → 从 Round.post_list 提取 trajectory（OpenAI 格式）
  → 从 Planner 最终回复提取 CausalGraph JSON
  → stdout 输出 {"output": ..., "trajectory": ...}
```

### RolloutRunner Agent 配置

`RolloutRunner/configs/agents/taskweaver.yaml`：
```yaml
name: taskweaver
cmd: ["/home/nn/miniconda3/envs/taskweaver/bin/python", "agent_runner.py"]
cwd: /home/nn/SOTA-agents/TaskWeaver
exp_id: taskweaver-qwen3.5-plus
model_name: qwen3.5-plus
agent_type: taskweaver
concurrency: 5
timeout: 1800
data_dir: /home/nn/SOTA-agents/RolloutRunner/data
```

---

## Trajectory 转换

TaskWeaver 的 Post 消息需转换为 OpenAI role 格式：

| TaskWeaver Post | OpenAI role | 内容 |
|----------------|------------|------|
| User → Planner | user | 用户任务描述 |
| Planner → CodeInterpreter | assistant | 规划步骤（含 plan attachment） |
| CodeInterpreter → Planner | assistant + tool_calls | 生成的代码 + 执行结果 |
| Planner → User | assistant | 最终分析结论 |

代码执行结果从 attachment (type=execution_result) 中提取，映射为 tool role 消息。
Plugin 调用从 CodeInterpreter 生成的 Python 代码中正则提取，映射为 tool_call。

---

## 运行命令

```bash
# === 独立测试 ===
cd /home/nn/SOTA-agents/TaskWeaver
python auto_rca_script.py

# === RolloutRunner 集成 ===
cd /home/nn/SOTA-agents/RolloutRunner

# 冒烟测试（1 条）
python scripts/run_rollout.py --agent taskweaver --source_exp_id rcabench_evaluation --limit 1

# 全量运行
nohup python -u scripts/run_rollout.py --agent taskweaver --source_exp_id rcabench_evaluation \
  > rollout_taskweaver.log 2>&1 &
```

---

## 关键约束

| 约束 | 值 |
|------|---|
| 最大内部对话轮数 | **200**（`session.max_internal_chat_round_num`，config_override 覆盖 config.json。按 ~2 rounds/工具调用 计算，可支持 ~80 次真实工具调用 + 25% buffer。2026-04-14 从 150 调至 200） |
| `--log-file` 参数 | ✅ 支持（同 thinkdepthai/aiq/mabc），可通过 `--log-file path.log` 或 `TW_LOG_FILE` 环境变量把 Planner/CodeInterpreter 详细对话 + LLM prompt 写到独立日志文件，便于 `run_rollout_with_retry.py --log_dir` 批量跑时实时 tail 查看每个样本的 Planner 推理过程 |
| DuckDB 结果 token 限制 | 5000 tokens |
| Prompt 压缩 | 开启（保留最近 3 轮，压缩前 2 轮），但单 Round 不触发 |
| 执行模式 | local kernel（非 container） |
| 模型（RCA 评测） | qwen3.5-plus（百炼 Coding Plan） |

---

## 关键文件索引

```
TaskWeaver/
├── agent_runner.py              # RolloutRunner 接口（stdin/stdout JSON）
├── auto_rca_script.py           # 独立运行脚本
├── project/
│   ├── taskweaver_config.json   # TaskWeaver 配置（不含 prompt_file_path）
│   ├── planner_prompt.yaml      # 备用 Planner prompt（当前不使用，用 upstream 默认）
│   └── plugins/
│       ├── list_tables_in_directory.py/.yaml  # 列目录
│       ├── get_schema.py/.yaml               # 获取 schema
│       └── query_parquet_files.py/.yaml      # SQL 查询
├── taskweaver/                  # TaskWeaver 框架源码（保持 upstream 不修改）
│   ├── app/app.py               # TaskWeaverApp 入口
│   ├── session/session.py       # Session 管理
│   ├── planner/planner.py       # Planner 角色
│   ├── planner/planner_prompt.yaml  # Planner system prompt（upstream 默认使用此文件）
│   ├── code_interpreter/        # CodeInterpreter 角色
│   ├── llm/openai.py            # OpenAI LLM 后端
│   ├── plugin/base.py           # Plugin 基类
│   └── memory/                  # Round/Post/Attachment 数据结构
├── requirements.txt             # Python 依赖
├── modification.md              # RCA 适配修改记录
└── .env                         # API 密钥（不提交 git）
```
