import json
from pathlib import Path
from typing import Union, List
from taskweaver.plugin import Plugin, register_plugin

TOKEN_LIMIT = 5000


def _import_duckdb():
    """Import duckdb with helpful error message."""
    try:
        import duckdb
        return duckdb
    except ImportError:
        raise ImportError(
            "duckdb is required. Install it with: pip install duckdb"
        )


def _sanitize_column_name(name: str) -> str:
    """Replace dots in column names with underscores to avoid DuckDB dot-notation ambiguity."""
    return name.replace(".", "_")


def _estimate_token_count(text: str) -> int:
    """Estimate token count using character-based approximation."""
    average_chars_per_token = 3
    return (len(text) + average_chars_per_token - 1) // average_chars_per_token


def _enforce_token_limit(payload: str, context: str) -> str:
    """Ensure payload stays within the token budget before returning"""
    token_estimate = _estimate_token_count(payload)
    if token_estimate <= TOKEN_LIMIT:
        return payload

    current_size = len(json.loads(payload)) if payload.startswith("[") else None
    suggested_limit = None
    if current_size:
        ratio = TOKEN_LIMIT / token_estimate
        suggested_limit = max(1, int(current_size * ratio * 0.8))

    warning = {
        "error": "Result exceeds token budget",
        "context": context,
        "estimated_tokens": token_estimate,
        "token_limit": TOKEN_LIMIT,
        "rows_returned": current_size,
        "suggested_limit": suggested_limit,
    }
    return json.dumps(warning, ensure_ascii=False, indent=2)


def _get_schema_one(parquet_file: str) -> dict:
    """Get schema for a single parquet file, returning a dict."""
    duckdb = _import_duckdb()

    if not Path(parquet_file).exists():
        return {"error": f"Parquet file not found: {parquet_file}"}

    conn = duckdb.connect(":memory:")
    try:
        result = conn.execute(f"SELECT * FROM read_parquet('{parquet_file}') LIMIT 0")
        schema = [{"name": _sanitize_column_name(desc[0]), "type": str(desc[1])} for desc in result.description]

        row_count_result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_file}')").fetchone()
        if row_count_result is None:
            row_count = 0
        else:
            row_count = row_count_result[0]

        return {
            "file": parquet_file,
            "row_count": row_count,
            "columns": schema,
        }

    except Exception as e:
        return {"error": f"Failed to extract schema: {str(e)}"}
    finally:
        conn.close()


@register_plugin
class GetSchemaPlugin(Plugin):
    def __call__(self, parquet_files: Union[str, List[str]]) -> str:
        """
        Get schema information of a parquet file, or a list of parquet files.

        :param parquet_files: Path to a parquet file, or list of paths for batch lookup
        :return schema_info: JSON string containing file metadata — single object if one file, list if multiple
        """
        if isinstance(parquet_files, str):
            result_json = json.dumps(_get_schema_one(parquet_files), ensure_ascii=False, indent=2)
        else:
            result_json = json.dumps([_get_schema_one(f) for f in parquet_files], ensure_ascii=False, indent=2)

        return _enforce_token_limit(result_json, "get_schema")
