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
import argparse
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

# Logger initialized in main() after flag parse; module-level so the shadow
# patch wrapper and role hooks can emit before main() is called.
logger = logging.getLogger("agent_runner")

# Per-sample tag set at start of main() and read by log emitters inside the
# CodeExecutor / Planner monkey-patches. Lets us grep a single case out of an
# aggregated multi-sample log.
_SAMPLE_TAG = ""

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

# ── Plugin Call Capture: shadow prelude via code rewrite ─────────────────
# Plugins run in an out-of-process Jupyter kernel (ces.environment uses
# jupyter_client.MultiKernelManager even in "local" mode), so monkey-patching
# Plugin.__call__ in the parent process never fires. The only correct hook is
# to rewrite the `code` string passed to CodeExecutor.execute_code: prepend a
# prelude that rebinds the three plugin names in the kernel's user_ns to
# logging wrappers, and have those wrappers append jsonl records to a shared
# file. Parent reads the file by exec_id after execute_code returns.
# exec_id == post_proxy.post.id (see code_interpreter.py:245-248), so each
# CodeInterpreter post maps 1:1 to a list of resolved plugin calls.

_PLUGIN_LOG_DIR = Path(tempfile.gettempdir()) / f"tw_plugin_log_{os.getpid()}"
_PLUGIN_LOG_DIR.mkdir(parents=True, exist_ok=True)
_PLUGIN_LOG_PATH = str(_PLUGIN_LOG_DIR / "calls.jsonl")
# Ensure file exists so seek works on first call
Path(_PLUGIN_LOG_PATH).touch()

_calls_by_exec_id: dict = {}
_shadow_fallback_flag = {"triggered": False}

_SHADOW_PRELUDE_TEMPLATE = r"""
__TW_EXEC_ID = {exec_id!r}
__TW_LOG_PATH = {log_path!r}

if "__tw_install_done" not in globals():
    import json as __tw_json, time as __tw_time

    def __tw_log(__name, __args, __result, __is_error):
        try:
            __rec = {{
                "exec_id": __TW_EXEC_ID,
                "name": __name,
                "arguments": __args,
                "ts": __tw_time.time(),
                "is_error": __is_error,
                "result_preview": str(__result)[:2000],
            }}
            with open(__TW_LOG_PATH, "a") as __f:
                __f.write(__tw_json.dumps(__rec, default=str, ensure_ascii=False) + "\n")
        except Exception:
            pass

    if "list_tables_in_directory" in globals():
        __tw_orig_list = list_tables_in_directory
        def list_tables_in_directory(directory):
            try:
                __r = __tw_orig_list(directory)
                __tw_log("list_tables_in_directory", {{"directory": directory}}, __r, False)
                return __r
            except Exception as __e:
                __tw_log("list_tables_in_directory", {{"directory": directory}}, __e, True)
                raise

    if "get_schema" in globals():
        __tw_orig_schema = get_schema
        def get_schema(parquet_files):
            try:
                __r = __tw_orig_schema(parquet_files)
                __tw_log("get_schema", {{"parquet_files": parquet_files}}, __r, False)
                return __r
            except Exception as __e:
                __tw_log("get_schema", {{"parquet_files": parquet_files}}, __e, True)
                raise

    if "query_parquet_files" in globals():
        __tw_orig_query = query_parquet_files
        def query_parquet_files(parquet_files, query, limit=10):
            try:
                __r = __tw_orig_query(parquet_files, query, limit)
                __tw_log("query_parquet_files", {{"parquet_files": parquet_files, "query": query, "limit": limit}}, __r, False)
                return __r
            except Exception as __e:
                __tw_log("query_parquet_files", {{"parquet_files": parquet_files, "query": query, "limit": limit}}, __e, True)
                raise

    __tw_install_done = True

## ===== USER CODE BELOW =====
"""


def _truncate(s, n=150):
    s = str(s)
    return s if len(s) <= n else s[:n] + "..."


def _fmt_args(rec: dict) -> str:
    """Render captured plugin arguments in a log-friendly, truncated form."""
    args = rec.get("arguments", {})
    name = rec.get("name", "?")
    if name == "query_parquet_files":
        q = _truncate(args.get("query", ""), 180)
        pf = args.get("parquet_files", "")
        pf_str = _truncate(str(pf), 80)
        return f"query={q!r} | files={pf_str} | limit={args.get('limit')}"
    if name == "list_tables_in_directory":
        return f"directory={args.get('directory')!r}"
    if name == "get_schema":
        pf = args.get("parquet_files", "")
        return f"parquet_files={_truncate(str(pf), 120)}"
    return _truncate(json.dumps(args, default=str, ensure_ascii=False), 200)


def _install_code_executor_patch():
    from taskweaver.code_interpreter.code_executor import CodeExecutor
    if getattr(CodeExecutor, "_tw_shadow_patched", False):
        return
    _orig_execute = CodeExecutor.execute_code

    def _patched_execute(self, exec_id: str, code: str):
        prelude = _SHADOW_PRELUDE_TEMPLATE.format(
            exec_id=exec_id, log_path=_PLUGIN_LOG_PATH,
        )
        try:
            log_pos_before = os.path.getsize(_PLUGIN_LOG_PATH)
        except OSError:
            log_pos_before = 0
        # Original code length (without prelude) for logging
        logger.info(
            f"[{_SAMPLE_TAG}] [CI exec_id={exec_id}] code_len={len(code)} "
            f"— executing in kernel"
        )
        try:
            return _orig_execute(self, exec_id=exec_id, code=prelude + "\n" + code)
        finally:
            calls = []
            try:
                with open(_PLUGIN_LOG_PATH, "r") as f:
                    f.seek(log_pos_before)
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if rec.get("exec_id") == exec_id:
                            calls.append(rec)
            except FileNotFoundError:
                pass
            _calls_by_exec_id[exec_id] = calls

            if not calls:
                logger.info(
                    f"[{_SAMPLE_TAG}] [CI exec_id={exec_id}] 0 plugin calls "
                    f"captured (pure pandas / no-op / pre-plugin error)"
                )
            for i, rec in enumerate(calls, 1):
                status = "ERROR" if rec.get("is_error") else "OK"
                result_prev = _truncate(rec.get("result_preview", ""), 200)
                logger.info(
                    f"[{_SAMPLE_TAG}] [CI exec_id={exec_id}] call#{i} "
                    f"{rec['name']} {status} | {_fmt_args(rec)} → {result_prev!r}"
                )

    CodeExecutor.execute_code = _patched_execute
    CodeExecutor._tw_shadow_patched = True


def _install_planner_logging_patch():
    """Wrap Planner.reply to log plan reasoning + target routing in real time.

    Planner runs between CodeInterpreter cycles. Logging its posts reveals
    the decision chain (current_plan_step / plan_reasoning) that drives the
    next CI execution, matching thinkdepthai's middleware step-by-step trace.
    """
    try:
        from taskweaver.planner.planner import Planner
    except Exception as e:
        logger.warning(f"Planner import failed, skipping planner logging: {e}")
        return
    if getattr(Planner, "_tw_logging_patched", False):
        return

    from taskweaver.memory.attachment import AttachmentType as _AT
    _orig_planner_reply = Planner.reply

    def _logged_reply(self, memory, prompt_log_path=None, **kwargs):
        post = _orig_planner_reply(
            self, memory, prompt_log_path=prompt_log_path, **kwargs
        )
        try:
            send_to = getattr(post, "send_to", "?")
            parts = []
            for att in getattr(post, "attachment_list", []) or []:
                if att.type in (_AT.init_plan, _AT.plan, _AT.current_plan_step):
                    parts.append(f"{att.type.value}={_truncate(att.content, 120)}")
                elif att.type == _AT.plan_reasoning:
                    parts.append(f"reasoning={_truncate(att.content, 180)}")
            msg_prev = _truncate(getattr(post, "message", "") or "", 120)
            logger.info(
                f"[{_SAMPLE_TAG}] [Planner → {send_to}] "
                f"{' | '.join(parts) if parts else ''} | msg={msg_prev!r}"
            )
        except Exception as e:
            logger.debug(f"Planner logging hook error: {e}")
        return post

    Planner.reply = _logged_reply
    Planner._tw_logging_patched = True


_install_code_executor_patch()
_install_planner_logging_patch()

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
#
# Source of truth: _calls_by_exec_id[post.id] -> list of resolved plugin calls
# captured at runtime via the shadow prelude (see top of file). Each record has
# {name, arguments, result_preview, is_error, ts}. post.id == exec_id, so a
# CodeInterpreter post maps 1:1 to a (possibly empty) list of captures.
#
# If capture is missing for a post (kernel crash, code raised before any plugin
# call, etc.), we fall back to emitting a single code_interpreter tool_call
# wrapping the raw code content, and set _shadow_fallback_flag so downstream
# can audit.


def convert_trajectory(post_list) -> list[dict]:
    """Convert TaskWeaver Round.post_list to OpenAI role-format trajectory.

    Uses runtime-captured plugin calls (resolved literal arguments) rather than
    regex over generated code. Each CodeInterpreter post becomes one assistant
    message with N tool_calls (one per real plugin invocation) and N matching
    tool messages with per-call result previews.
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
            captured = _calls_by_exec_id.get(post.id, [])

            if captured:
                tool_calls = []
                for rec in captured:
                    call_id = f"call_{tool_call_counter}"
                    tool_call_counter += 1
                    tool_calls.append({
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": rec["name"],
                            "arguments": json.dumps(
                                rec.get("arguments", {}),
                                ensure_ascii=False,
                                default=str,
                            ),
                        },
                    })

                trajectory.append({
                    "role": "assistant",
                    "content": post.message or "",
                    "tool_calls": tool_calls,
                })

                for tc, rec in zip(tool_calls, captured):
                    result_str = rec.get("result_preview", "")
                    if rec.get("is_error"):
                        result_str = "An error occurred while running the tool. " + result_str
                    trajectory.append({
                        "role": "tool",
                        "content": result_str,
                        "tool_call_id": tc["id"],
                    })

            else:
                # Fallback: kernel produced no plugin capture for this post.
                # Emit code_interpreter pseudo-tool so downstream analysis still
                # sees a turn, and flag for audit.
                _shadow_fallback_flag["triggered"] = True
                print(
                    f"WARN: no shadow capture for exec_id={post.id}, falling back to raw code",
                    file=sys.stderr,
                )

                code_content = ""
                exec_results = []
                has_error = False
                for att in post.attachment_list:
                    if att.type in (AttachmentType.thought, AttachmentType.reply_content):
                        code_content += att.content + "\n"
                    elif att.type == AttachmentType.execution_result:
                        exec_results.append(att.content)
                    elif att.type == AttachmentType.code_error:
                        exec_results.append(f"[ERROR] {att.content}")
                        has_error = True

                result_content = "\n".join(exec_results) if exec_results else (post.message or "")
                call_id = f"call_{tool_call_counter}"
                tool_call_counter += 1

                trajectory.append({
                    "role": "assistant",
                    "content": post.message or "",
                    "tool_calls": [{
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": "code_interpreter",
                            "arguments": json.dumps(
                                {"code": code_content.strip()}, ensure_ascii=False
                            ),
                        },
                    }],
                })
                trajectory.append({
                    "role": "tool",
                    "content": ("An error occurred while running the tool. " if has_error else "") + result_content,
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

def _configure_logging():
    """Parse --verbose / --log-file (mirrors thinkdepthai/Deep_Research) and
    honor TW_LOG_FILE / TW_VERBOSE env vars so RolloutRunner can enable
    real-time logging without editing the yaml command.

    Design for multi-sample rollouts:
    - append mode (subprocess per sample does NOT truncate the shared file)
    - every line tagged with pid so concurrent runs are greppable
    - Linux O_APPEND makes small-record writes atomic, so lines don't break
      even under AIMD concurrency (capacity up to 5)
    - FileHandler flushes after each record (StreamHandler.emit default),
      so `tail -f` sees events live
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Stream per-event logs to stderr")
    parser.add_argument("--log-file", default=None,
                        help="Append per-event logs to file (implies --verbose)")
    args, _ = parser.parse_known_args()

    log_file = args.log_file or os.environ.get("TW_LOG_FILE")
    verbose = args.verbose or bool(os.environ.get("TW_VERBOSE")) or bool(log_file)

    if verbose:
        # Include process id so concurrent rollout subprocesses are greppable.
        fmt = logging.Formatter(
            "%(asctime)s pid=%(process)d %(message)s", datefmt="%H:%M:%S"
        )
        handlers = []
        h = logging.StreamHandler(sys.stderr)
        h.setFormatter(fmt)
        handlers.append(h)
        if log_file:
            os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
            # APPEND mode — must not truncate between samples in a batch run.
            fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
            fh.setFormatter(fmt)
            handlers.append(fh)
        # Configure root so both our logger and taskweaver internal logs surface
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        # Clear any pre-existing handlers (taskweaver installs its own)
        root.handlers = handlers
    else:
        logging.basicConfig(level=logging.WARNING)


def main():
    _configure_logging()

    payload = json.loads(sys.stdin.read())

    data_dir = payload.get("data_dir", "")
    system_prompt = payload.get("system_prompt", "")

    # data_XXXXXXXX basename is unique per sample (RCAgentEval's preprocess
    # embeds it in augmented_question and reuses it across reruns). Use it
    # as the per-sample tag so all lines for one case are grep-able even
    # when several subprocesses write to the same aggregated log file.
    global _SAMPLE_TAG
    _SAMPLE_TAG = os.path.basename(data_dir) if data_dir else f"pid{os.getpid()}"

    logger.info(f"=== START sample={_SAMPLE_TAG} data_dir={data_dir} ===")
    logger.info(
        f"[{_SAMPLE_TAG}] payload question_len={len(payload.get('question',''))} "
        f"sp_len={len(system_prompt)} up_len={len(payload.get('user_prompt',''))}"
    )

    # 1. 动态生成 planner_prompt.yaml：upstream 框架指令 + RCA 领域指令 + Final Answer Format
    prompt_yaml_path = build_planner_prompt_yaml(system_prompt)

    # 2. 构建用户消息：user_prompt（增强版 question）+ data_dir
    user_message = build_user_message(payload)

    # 初始化 TaskWeaver
    app_dir = os.path.join(os.path.dirname(__file__), "project")

    # 动态读取模型配置：RCA_MODEL 由 RolloutRunner 传入，API key/base_url 从 .env 读取
    rca_model = os.environ.get("RCA_MODEL", "claude-sonnet-4-6")
    logger.info(f"[{_SAMPLE_TAG}] Model: {rca_model}")

    config_override = {
        "execution_service.kernel_mode": "local",
        "session.max_internal_chat_round_num": 200,  # ~80 工具调用 (2N+2) + 25% 余量
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
        logger.info(f"[{_SAMPLE_TAG}] Session start: id={session.session_id}")
        logger.info(
            f"[{_SAMPLE_TAG}] user_message preview: {_truncate(user_message, 200)!r}"
        )
        _t0 = time.time()
        inv_round = session.send_message(user_message)
        _elapsed = time.time() - _t0
        logger.info(
            f"[{_SAMPLE_TAG}] [Investigate] state={inv_round.state} "
            f"posts={len(inv_round.post_list)} elapsed={_elapsed:.1f}s"
        )
        _role_counts = {}
        for _p in inv_round.post_list:
            _role_counts[_p.send_from] = _role_counts.get(_p.send_from, 0) + 1
        logger.info(f"[{_SAMPLE_TAG}] Post breakdown: {_role_counts}")

        trajectory = convert_trajectory(inv_round.post_list)

        output = extract_output(inv_round.post_list)
        logger.info(
            f"[{_SAMPLE_TAG}] [Output] len={len(output)} preview={output[:80]!r} "
            f"shadow_fallback={_shadow_fallback_flag['triggered']}"
        )

        plugin_calls_raw = []
        for post in inv_round.post_list:
            if post.send_from == "CodeInterpreter":
                plugin_calls_raw.extend(_calls_by_exec_id.get(post.id, []))
        logger.info(
            f"[{_SAMPLE_TAG}] plugin_calls_raw total={len(plugin_calls_raw)} "
            + ", ".join(
                f"{n}={sum(1 for c in plugin_calls_raw if c['name']==n)}"
                for n in ("list_tables_in_directory", "get_schema", "query_parquet_files")
            )
        )

        result = {
            "output": output,
            "trajectory": trajectory,
            "usage": _tracker.get_usage(),
            "plugin_calls_raw": plugin_calls_raw,
            "taskweaver_session_id": session.session_id,
            "shadow_fallback": _shadow_fallback_flag["triggered"],
        }

        # 单行 JSON 输出，runner._parse_last_json 从末行解析
        print(json.dumps(result, ensure_ascii=False))

    except Exception as e:
        logger.exception(f"[{_SAMPLE_TAG}] agent_runner main() crashed: {e}")
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
        logger.info(f"=== END sample={_SAMPLE_TAG} ===")
        try:
            app.stop()
        except Exception:
            pass
        # 清理临时文件
        try:
            os.unlink(prompt_yaml_path)
        except OSError:
            pass
        # 清理 plugin capture 日志目录
        try:
            shutil.rmtree(_PLUGIN_LOG_DIR, ignore_errors=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
