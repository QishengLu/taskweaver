import json  
from datetime import datetime  
from pathlib import Path  
from typing import Union, List, Dict, Any  
  
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


def _serialize_datetime(obj):
    """Convert datetime objects to ISO format strings for JSON serialization"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: _serialize_datetime(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_serialize_datetime(item) for item in obj]
    else:
        return obj


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

    # Calculate suggested limit reduction
    current_size = len(json.loads(payload)) if payload.startswith("[") else None
    suggested_limit = None
    if current_size:
        # Estimate how many rows would fit within token limit
        ratio = TOKEN_LIMIT / token_estimate
        suggested_limit = max(1, int(current_size * ratio * 0.8))  # 80% safety margin

    suggestion_parts = [
        "The query result is too large. Please adjust your query:",
        "  • Reduce the LIMIT value" + (f" (try LIMIT {suggested_limit})" if suggested_limit else ""),
        "  • Filter rows with WHERE clauses to reduce result size",
        "  • Select only necessary columns instead of SELECT *",
        "  • Use aggregation (COUNT, SUM, AVG) instead of retrieving raw rows",
    ]

    warning = {
        "error": "Result exceeds token budget",
        "context": context,
        "estimated_tokens": token_estimate,
        "token_limit": TOKEN_LIMIT,
        "rows_returned": current_size,
        "suggested_limit": suggested_limit,
        "suggestion": "\n".join(suggestion_parts),
    }
    return json.dumps(warning, ensure_ascii=False, indent=2)


def _validate_parquet_files(parquet_files: Union[str, List[str]]) -> List[str]:
    """Validate parquet files exist and return as list."""
    if isinstance(parquet_files, str):
        parquet_files = [parquet_files]

    for file_path in parquet_files:
        if not Path(file_path).exists():
            raise FileNotFoundError(
                f"Parquet file not found: {file_path}\n"
                f"Please check the file path and ensure the file exists. "
                f"You may use 'list_tables_in_directory' to discover available parquet files."
            )
    return parquet_files


@register_plugin  
class QueryParquetFilesPlugin(Plugin):  
    def __call__(self, parquet_files: Union[str, List[str]], query: str, limit: int = 10) -> str:  
        """  
        Query parquet files using SQL syntax for data analysis and exploration.  
          
        :param parquet_files: Path(s) to parquet file(s)  
        :param query: SQL query to execute  
        :param limit: Maximum number of records to return  
        :return result: JSON string of query results  
        """  
        duckdb = _import_duckdb()
        parquet_files = _validate_parquet_files(parquet_files)

        conn = duckdb.connect(":memory:")
        table_names: set = set()

        try:
            # Convert absolute paths to relative paths where possible
            cwd = Path.cwd()
            relative_parquet_files = []
            for file_path in parquet_files:
                file_path_obj = Path(file_path)
                if file_path_obj.is_absolute():
                    try:
                        file_path = str(file_path_obj.relative_to(cwd))
                    except ValueError:
                        # File is not under cwd (e.g., temp files), use absolute path
                        file_path = str(file_path_obj)
                relative_parquet_files.append(file_path)
            parquet_files = relative_parquet_files

            # Register parquet files as views using filename as table name
            for file_path in parquet_files:
                base_name = Path(file_path).stem
                table_name = base_name
                counter = 1
                while table_name in table_names:
                    table_name = f"{base_name}_{counter}"
                    counter += 1
                table_names.add(table_name)
                conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}')")

            # Execute query
            result = conn.execute(query).fetchall()
            columns = [desc[0] for desc in conn.description]

            # Convert to list of dictionaries and serialize datetime
            rows = [dict(zip(columns, row, strict=False)) for row in result]
            serialized_rows = _serialize_datetime(rows)

            # Apply limit if specified
            if len(serialized_rows) > limit:
                serialized_rows = serialized_rows[:limit]

            result_json = json.dumps(serialized_rows, ensure_ascii=False, indent=2)
            return _enforce_token_limit(result_json, "query_parquet_files")

        except FileNotFoundError:
            # Re-raise FileNotFoundError as-is (already has good message)
            raise
        except Exception as e:
            error_msg = str(e)
            # Provide contextual error messages
            if "syntax error" in error_msg.lower() or "parser error" in error_msg.lower():
                raise RuntimeError(
                    f"SQL syntax error in query: {error_msg}\n"
                    f"Query: {query}\n"
                    f"Available tables: {', '.join(table_names) if table_names else 'None'}\n"
                    f"Tip: Use 'get_schema' to check column names before querying."
                ) from e
            elif "catalog" in error_msg.lower() or "table" in error_msg.lower():
                raise RuntimeError(
                    f"Table reference error: {error_msg}\n"
                    f"Query: {query}\n"
                    f"Available tables: {', '.join(table_names) if table_names else 'None'}\n"
                    f"Make sure table names in your query match the registered table names."
                ) from e
            else:
                raise RuntimeError(
                    f"Query execution failed: {error_msg}\n"
                    f"Query: {query}\n"
                    f"Available tables: {', '.join(table_names) if table_names else 'None'}"
                ) from e
        finally:
            conn.close()


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
            # Convert absolute path to relative where possible
            cwd = Path.cwd()
            parquet_file_obj = Path(parquet_file)
            if parquet_file_obj.is_absolute():
                try:
                    parquet_file = str(parquet_file_obj.relative_to(cwd))
                except ValueError:
                    # File is not under cwd (e.g., temp files), use absolute path
                    parquet_file = str(parquet_file_obj)

            # Get schema
            result = conn.execute(f"SELECT * FROM read_parquet('{parquet_file}') LIMIT 0")
            schema = [{"name": desc[0], "type": str(desc[1])} for desc in result.description]

            # Get row count
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
            # Re-raise FileNotFoundError as-is (already has good message)
            raise
        except Exception as e:
            raise RuntimeError(
                f"Failed to extract schema from parquet file: {str(e)}\n"
                f"File: {parquet_file}\n"
                f"This may indicate a corrupted file or unsupported parquet format."
            ) from e
        finally:
            conn.close()

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
                    # File is not under cwd, use absolute path
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
