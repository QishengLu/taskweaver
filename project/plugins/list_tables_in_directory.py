import json
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


@register_plugin
class ListTablesInDirectoryPlugin(Plugin):
    def __call__(self, directory: str) -> str:
        """
        List all parquet files in a directory with metadata.
        
        :param directory: Directory path to search for parquet files
        :return files_info: JSON string containing list of files with metadata
        """
        duckdb = _import_duckdb()
        
        dir_path = Path(directory)
        if not dir_path.exists():
            raise FileNotFoundError(
                f"Directory not found: {directory}\n"
                f"Please verify the directory path exists and is accessible."
            )

        if not dir_path.is_dir():
            raise ValueError(
                f"Path is not a directory: {directory}\n"
                f"Please provide a valid directory path, not a file path."
            )

        files_info = []
        cwd = Path.cwd()
        
        for file_path in dir_path.glob("*.parquet"):
            file_path_str = str(file_path)
            file_path_obj = Path(file_path_str)
            if file_path_obj.is_absolute():
                try:
                    file_path_str = str(file_path_obj.relative_to(cwd))
                except ValueError:
                    file_path_str = str(file_path_obj)
            
            try:
                conn = duckdb.connect(":memory:")
                row_count_result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file_path_str}')").fetchone()
                if row_count_result is None:
                    raise RuntimeError("Failed to read row count from parquet file")
                row_count = row_count_result[0]
                
                result = conn.execute(f"SELECT * FROM read_parquet('{file_path_str}') LIMIT 0")
                column_count = len(result.description)
                conn.close()

                files_info.append(
                    {
                        "filename": file_path.name,
                        "path": str(file_path),
                        "row_count": row_count,
                        "column_count": column_count,
                    }
                )
            except Exception as e:
                files_info.append({
                    "filename": file_path.name, 
                    "path": str(file_path), 
                    "error": str(e)
                })

        result_json = json.dumps(files_info, ensure_ascii=False, indent=2)
        return _enforce_token_limit(result_json, "list_tables_in_directory")
