#!/usr/bin/env python
"""
agent_runner.py — TaskWeaver RCA 评测接口

stdin:  JSON { question, system_prompt, user_prompt,
               compress_system_prompt, compress_user_prompt, data_dir }
stdout: JSON { output (CausalGraph JSON), trajectory (OpenAI 格式) }

Prompt 映射策略：
  system_prompt (RCA_ANALYSIS_SP) → Planner instruction_template 末尾
    + Final Answer Format 指令（明确要求 message 字段输出纯 JSON）
  user_prompt (RCA_ANALYSIS_UP) + data_dir → send_message() 用户消息
  compress_system_prompt / compress_user_prompt → 不使用（无独立 compress 步骤）
  输出 = Planner 最终 message 字段（直接为 CausalGraph JSON）
"""
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/home/nn/SOTA-agents/RolloutRunner")
from src.usage_tracker import UsageTracker

_tracker = UsageTracker()
_tracker.install_openai_hooks()

# 清理 RolloutRunner 路径和 src 模块缓存，避免与本项目的 src 包冲突
sys.path.remove("/home/nn/SOTA-agents/RolloutRunner")
for _mod in list(sys.modules):
    if _mod == "src" or _mod.startswith("src."):
        del sys.modules[_mod]


import yaml
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# Add TaskWeaver root to sys.path
sys.path.insert(0, str(Path(__file__).parent))

from taskweaver.app.app import TaskWeaverApp
from taskweaver.memory.attachment import AttachmentType
from taskweaver.llm.openai import OpenAIService as _OpenAIService

# Patch: force non-streaming so UsageTracker can read response.usage → token_source=actual.
# TaskWeaver's chat_completion default is stream=True; callers pass stream=True explicitly
# too, so we override at the method level. Functional behavior is identical — the same
# content is yielded, just delivered as one complete message instead of delta chunks.
_orig_chat_completion = _OpenAIService.chat_completion


def _non_streaming_chat_completion(
    self, messages, stream=False,
    temperature=None, max_tokens=None, top_p=None, stop=None, **kwargs
):
    # Force stream=False regardless of caller — enables UsageTracker to read
    # response.usage and record actual token counts (token_source="actual").
    return _orig_chat_completion(
        self, messages, False, temperature, max_tokens, top_p, stop, **kwargs
    )


_OpenAIService.chat_completion = _non_streaming_chat_completion


# ── 工具函数 ─────────────────────────────────────────────────────────────────

def strip_markdown_json(text: str) -> str:
    """剥离 LLM 返回的 ```json ... ``` 代码块，提取纯 JSON。"""
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        return m.group(1).strip()
    # 尝试直接找 JSON object
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        return m.group(0).strip()
    return text.strip()


def link_data_files(data_dir: str, session_cwd: str):
    """将 data_dir 中的 parquet 文件软链接到 session cwd。"""
    data_path = Path(data_dir)
    cwd_path = Path(session_cwd)
    cwd_path.mkdir(parents=True, exist_ok=True)

    if not data_path.exists():
        print(f"WARNING: data_dir does not exist: {data_dir}", file=sys.stderr)
        return

    for f in data_path.glob("*.parquet"):
        dst = cwd_path / f.name
        if not dst.exists():
            try:
                os.symlink(f, dst)
            except OSError:
                shutil.copy2(f, dst)


# ── Planner System Prompt 动态生成 ──────────────────────────────────────────

_FINAL_ANSWER_FORMAT = """
  ## Final Answer Format

  When you have completed the investigation and are ready to send the final answer to the User:
  - Set `stop` to `true`, `send_to` to `User`
  - Your `message` field MUST contain ONLY the raw JSON object — no markdown code blocks, no explanations, no surrounding text
  - The `message` field must start with `{{` and end with `}}`
  - Follow the CausalGraph format specified in the Output Requirements section above
"""


def build_planner_prompt_yaml(system_prompt: str) -> str:
    """基于 upstream planner_prompt.yaml，追加 RCA 领域指令，生成临时 YAML 文件。

    注入内容（追加到 instruction_template 末尾）：
    1. system_prompt 的调查指令部分（--- 分隔线之前）：RCA 专家角色 + 可用工具 + 分析步骤 + 输出格式规范
    2. Final Answer Format 指令：明确要求 Planner 的 message 字段输出纯 JSON

    system_prompt（RCA_ANALYSIS_SP）结构：
      Part 1（调查指令）：角色定义、工具说明、分析步骤、评估 Schema、Critical Rules
      --- 分隔线 ---
      Part 2（压缩触发器）：INVESTIGATION TOPIC + "Output the JSON object NOW:"
    只注入 Part 1，避免 Planner 看到 "Output JSON NOW" 后立即输出模板跳过调查。

    关键：instruction_template 经过 Python .format() 处理，
    所以注入内容中的 { } 必须转义为 {{ }}。
    """
    upstream_path = os.path.join(
        os.path.dirname(__file__),
        "taskweaver", "planner", "planner_prompt.yaml",
    )
    with open(upstream_path, "r") as f:
        prompt_data = yaml.safe_load(f)

    # 只取调查指令部分（--- 分隔线之前），去掉 compress 触发器（"Output JSON NOW:"）
    investigation_part = re.split(r'\n\s*---\s*\n', system_prompt, maxsplit=1)[0]

    # 过滤 think_tool（TaskWeaver 无 think_tool，避免 LLM 尝试调用不存在的工具）
    investigation_part = re.sub(r"  4\. \*\*think_tool\*\*.*\n", "", investigation_part)
    investigation_part = investigation_part.replace("four tools", "three tools")

    # 转义 { } → {{ }}（防止 .format() 解析出错）
    escaped_sp = investigation_part.replace("{", "{{").replace("}", "}}")

    prompt_data["instruction_template"] += (
        f"\n\n  ## RCA Analysis Instructions\n\n{escaped_sp}"
        + _FINAL_ANSWER_FORMAT
    )

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="planner_prompt_",
        dir=tempfile.gettempdir(), delete=False,
    )
    yaml.dump(prompt_data, tmp, default_flow_style=False, allow_unicode=True)
    tmp.close()

    print(f"Generated planner prompt: {tmp.name}", file=sys.stderr)
    return tmp.name


def build_user_message(payload: dict) -> str:
    """构建 send_message() 的用户消息。

    使用 user_prompt（已 format incident_description 的 RCA_ANALYSIS_UP），
    包含具体事件描述 + 调查策略 + "Begin your investigation now."
    追加 data_dir 数据位置提示。
    """
    user_prompt = payload.get("user_prompt", "") or payload.get("question", "")
    data_dir = payload.get("data_dir", "")

    parts = [user_prompt]

    if data_dir:
        parts.append(
            f"\n\n## Data Location\n\n"
            f"The telemetry data for this incident is stored at: `{data_dir}`\n\n"
            f"Start by calling `list_tables_in_directory(directory=\"{data_dir}\")` "
            f"to discover available parquet files."
        )

    return "".join(parts)


# ── Trajectory 转换 ──────────────────────────────────────────────────────────

# 三个 RCA plugin 的正则：匹配函数调用并提取参数
_PLUGIN_CALL_PATTERN = re.compile(
    r"""(?P<name>list_tables_in_directory|get_schema|query_parquet_files)"""
    r"""\s*\((?P<args>[^)]*(?:\([^)]*\)[^)]*)*)\)""",
)


def _extract_plugin_calls(code: str) -> list[dict]:
    """从 CodeInterpreter 生成的 Python 代码中提取实际的 plugin 调用。"""
    calls = []
    for m in _PLUGIN_CALL_PATTERN.finditer(code):
        name = m.group("name")
        raw_args = m.group("args").strip()
        args_dict = _parse_python_kwargs(name, raw_args)
        calls.append({"name": name, "arguments": args_dict})
    return calls


def _parse_python_kwargs(func_name: str, raw_args: str) -> dict:
    """尽力将 Python 函数调用参数解析为 dict。"""
    result = {}
    positional_names = {
        "list_tables_in_directory": ["directory"],
        "get_schema": ["parquet_file"],
        "query_parquet_files": ["parquet_files", "query", "limit"],
    }

    if not raw_args:
        return result

    parts = _split_args(raw_args)
    pos_names = positional_names.get(func_name, [])
    pos_idx = 0

    for part in parts:
        part = part.strip()
        if not part:
            continue
        kw_match = re.match(r"(\w+)\s*=\s*(.*)", part, re.DOTALL)
        if kw_match:
            key = kw_match.group(1)
            val = _try_eval(kw_match.group(2).strip())
            result[key] = val
        else:
            if pos_idx < len(pos_names):
                result[pos_names[pos_idx]] = _try_eval(part)
            pos_idx += 1

    return result


def _split_args(s: str) -> list[str]:
    """按顶层逗号分割参数字符串，忽略括号/引号内的逗号。"""
    parts = []
    depth = 0
    in_str = None
    current = []

    for ch in s:
        if in_str:
            current.append(ch)
            if ch == in_str:
                in_str = None
            continue

        if ch in ('"', "'"):
            in_str = ch
            current.append(ch)
        elif ch in ("(", "[", "{"):
            depth += 1
            current.append(ch)
        elif ch in (")", "]", "}"):
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)

    if current:
        parts.append("".join(current))
    return parts


def _try_eval(s: str) -> str:
    """尝试用 ast.literal_eval 解析 Python 字面量，失败则返回原字符串。"""
    import ast

    s = s.strip()
    try:
        return ast.literal_eval(s)
    except Exception:
        return s


def convert_trajectory(post_list) -> list[dict]:
    """将 TaskWeaver Round.post_list 转换为 OpenAI role 格式的 trajectory。

    从 CodeInterpreter 生成的代码中提取 plugin 调用，
    每个调用映射为独立 tool_call，确保 tool_bonus 计算公平。
    """
    trajectory = []
    tool_call_counter = 0

    for post in post_list:
        send_from = post.send_from

        if send_from == "User":
            trajectory.append({
                "role": "user",
                "content": post.message,
            })

        elif send_from == "Planner":
            plan_parts = []
            for att in post.attachment_list:
                if att.type in (
                    AttachmentType.init_plan,
                    AttachmentType.plan,
                    AttachmentType.current_plan_step,
                    AttachmentType.plan_reasoning,
                ):
                    plan_parts.append(f"[{att.type.value}] {att.content}")

            content = post.message
            if plan_parts:
                content = "\n".join(plan_parts) + "\n\n" + content

            trajectory.append({
                "role": "assistant",
                "content": content,
            })

        elif send_from == "CodeInterpreter":
            code_content = ""
            exec_results = []
            has_error = False

            for att in post.attachment_list:
                if att.type == AttachmentType.thought:
                    code_content += att.content + "\n"
                elif att.type == AttachmentType.reply_content:
                    code_content += att.content + "\n"
                elif att.type == AttachmentType.execution_result:
                    exec_results.append(att.content)
                elif att.type == AttachmentType.code_error:
                    exec_results.append(f"[ERROR] {att.content}")
                    has_error = True

            result_content = "\n".join(exec_results) if exec_results else post.message

            # 从代码中提取 plugin 调用
            plugin_calls = _extract_plugin_calls(code_content)

            if plugin_calls:
                tool_calls = []
                for pc in plugin_calls:
                    call_id = f"call_{tool_call_counter}"
                    tool_call_counter += 1
                    tool_calls.append({
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": pc["name"],
                            "arguments": json.dumps(
                                pc["arguments"], ensure_ascii=False
                            ),
                        },
                    })

                trajectory.append({
                    "role": "assistant",
                    "content": post.message or "",
                    "tool_calls": tool_calls,
                })

                for tc in tool_calls:
                    tool_result = result_content
                    if has_error:
                        tool_result = "An error occurred while running the tool. " + tool_result
                    trajectory.append({
                        "role": "tool",
                        "content": tool_result,
                        "tool_call_id": tc["id"],
                    })

            else:
                # Fallback：没有可识别的 plugin 调用
                call_id = f"call_{tool_call_counter}"
                tool_call_counter += 1

                trajectory.append({
                    "role": "assistant",
                    "content": post.message or "",
                    "tool_calls": [
                        {
                            "id": call_id,
                            "type": "function",
                            "function": {
                                "name": "code_interpreter",
                                "arguments": json.dumps(
                                    {"code": code_content.strip()},
                                    ensure_ascii=False,
                                ),
                            },
                        }
                    ],
                })

                tool_result = result_content
                if has_error:
                    tool_result = "An error occurred while running the tool. " + tool_result
                trajectory.append({
                    "role": "tool",
                    "content": tool_result,
                    "tool_call_id": call_id,
                })

        else:
            if post.message:
                trajectory.append({
                    "role": "assistant",
                    "content": post.message,
                })

    return trajectory


def extract_output(post_list) -> str:
    """从 Planner 最终回复中提取 CausalGraph JSON（TaskWeaver 原生输出方式）。"""
    for post in reversed(post_list):
        if post.send_from == "Planner" and post.send_to == "User":
            return strip_markdown_json(post.message)

    # fallback: 搜索所有 post
    for post in reversed(post_list):
        text = post.message or ""
        if '"nodes"' in text or '"root_causes"' in text:
            return strip_markdown_json(text)

    return ""


# ── 主流程 ────────────────────────────────────────────────────────────────────

def main():
    payload = json.loads(sys.stdin.read())

    data_dir = payload.get("data_dir", "")
    system_prompt = payload.get("system_prompt", "")

    # 1. 动态生成 planner_prompt.yaml：upstream 框架指令 + RCA 领域指令 + Final Answer Format
    prompt_yaml_path = build_planner_prompt_yaml(system_prompt)

    # 2. 构建用户消息：user_prompt（增强版 question）+ data_dir
    user_message = build_user_message(payload)

    # 初始化 TaskWeaver
    app_dir = os.path.join(os.path.dirname(__file__), "project")

    # 动态读取模型配置：RCA_MODEL 由 RolloutRunner 传入，API key/base_url 从 .env 读取
    rca_model = os.environ.get("RCA_MODEL", "claude-sonnet-4-6")

    config_override = {
        "execution_service.kernel_mode": "local",
        "session.max_internal_chat_round_num": 150,
        "planner.prompt_file_path": prompt_yaml_path,
        "llm.model": rca_model,
        "llm.api_key": os.environ.get("OPENAI_API_KEY", ""),
        "llm.api_base": os.environ.get("OPENAI_BASE_URL", "https://api.shubiaobiao.cn/v1"),
    }

    app = TaskWeaverApp(app_dir=app_dir, config=config_override)

    try:
        session = app.get_session()

        # 将 data_dir 的 parquet 文件链接到 session 的 cwd
        session_cwd = os.path.join(
            app_dir, "workspace", "sessions", session.session_id, "cwd"
        )
        if data_dir:
            link_data_files(data_dir, session_cwd)

        # 执行 RCA 分析：Planner 协调 CodeInterpreter 查询遥测数据，最终 message 即为 CausalGraph JSON
        print(f"Starting TaskWeaver session: {session.session_id}", file=sys.stderr)
        inv_round = session.send_message(user_message)
        print(
            f"[Investigate] state={inv_round.state}, posts={len(inv_round.post_list)}",
            file=sys.stderr,
        )
        trajectory = convert_trajectory(inv_round.post_list)

        # 从 Planner 最终回复提取 CausalGraph JSON（Planner 被指令要求 message 字段即为纯 JSON）
        output = extract_output(inv_round.post_list)
        print(f"[Output] len={len(output)}, starts_with={output[:30]!r}", file=sys.stderr)

        result = {
            "output": output,
            "trajectory": trajectory,
            "usage": _tracker.get_usage(),
        }

        # 单行 JSON 输出，runner._parse_last_json 从末行解析
        print(json.dumps(result, ensure_ascii=False))

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        error_result = {
            "output": "",
            "trajectory": [],
            "error": str(e),
        }
        print(json.dumps(error_result, ensure_ascii=False))
        sys.exit(1)

    finally:
        app.stop()
        # 清理临时文件
        try:
            os.unlink(prompt_yaml_path)
        except OSError:
            pass


if __name__ == "__main__":
    main()
