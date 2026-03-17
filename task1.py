import csv
import io
from functools import lru_cache


class KeyMismatch(Exception):
    """Exception raised when search keys don't match data headers."""
    pass


@lru_cache(maxsize=32)
def _parse_data(data: str) -> tuple:
    """
    Parse CSV data and return headers and rows as tuples (for caching).
    Uses tuple of tuples for immutable, hashable cache key.
    """
    reader = csv.DictReader(io.StringIO(data))
    headers = tuple(reader.fieldnames)
    rows = tuple(tuple(row.values()) for row in reader)
    return headers, rows


def task1(search: dict, data: str) -> str:
    """
    Find first match in CSV data and return the value.
    
    Args:
        search: Dictionary of column names and values to search for
        data: CSV string with headers
        
    Returns:
        The value from the 'value' column of the first matching row,
        or '-1' if no match found
        
    Raises:
        KeyMismatch: If search keys don't match data headers
    """
    # Parse and cache the data
    headers, rows = _parse_data(data)
    
    # Convert headers to set for O(1) lookup
    headers_set = set(headers)
    
    # Validate that all search keys exist in headers
    for key in search.keys():
        if key not in headers_set:
            raise KeyMismatch("Key mismatch")
    
    # Get column indices from headers
    header_to_idx = {header: idx for idx, header in enumerate(headers)}
    
    # Get indices for search keys (excluding 'value' which we don't search on)
    search_indices = {key: header_to_idx[key] for key in search}
    
    # Get index for value column
    value_idx = header_to_idx['value']
    
    # Search for first matching row
    for row in rows:
        match = True
        for key, idx in search_indices.items():
            if row[idx] != search[key]:
                match = False
                break
        if match:
            return row[value_idx]
    
    return '-1'
