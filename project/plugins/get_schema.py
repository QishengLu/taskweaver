import json
from datetime import datetime
from pathlib import Path
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


def _estimate_token_count(text: str) -> int:
    """Estimate token count using character-based approximation.
    
    Approximate for Chinese/English mixed text.
    Average: 3 characters per token.
    """
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


@register_plugin
class GetSchemaPlugin(Plugin):
    def __call__(self, parquet_file: str) -> str:
        """
        Get schema information of a parquet file.
        
        :param parquet_file: Path to parquet file to inspect
        :return schema_info: JSON string containing file metadata
        """
        duckdb = _import_duckdb()
        
        if not Path(parquet_file).exists():
            raise FileNotFoundError(
                f"Parquet file not found: {parquet_file}\n"
                f"Please verify the file path. Use 'list_tables_in_directory' to discover available files."
            )

        conn = duckdb.connect(":memory:")
        try:
            cwd = Path.cwd()
            parquet_file_obj = Path(parquet_file)
            if parquet_file_obj.is_absolute():
                try:
                    parquet_file = str(parquet_file_obj.relative_to(cwd))
                except ValueError:
                    parquet_file = str(parquet_file_obj)

            result = conn.execute(f"SELECT * FROM read_parquet('{parquet_file}') LIMIT 0")
            schema = [{"name": desc[0], "type": str(desc[1])} for desc in result.description]

            row_count_result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_file}')").fetchone()
            if row_count_result is None:
                raise RuntimeError("Failed to read row count from parquet file")
            row_count = row_count_result[0]

            schema_info = {
                "file": parquet_file,
                "row_count": row_count,
                "columns": schema,
            }

            result_json = json.dumps(schema_info, ensure_ascii=False, indent=2)
            return _enforce_token_limit(result_json, "get_schema")

        except FileNotFoundError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"Failed to extract schema from parquet file: {str(e)}\n"
                f"File: {parquet_file}\n"
                f"This may indicate a corrupted file or unsupported parquet format."
            ) from e
        finally:
            conn.close()
