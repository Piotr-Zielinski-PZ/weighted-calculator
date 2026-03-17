import csv
import io
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from functools import lru_cache


# ============== Data Layer ==============

class DataLoader(ABC):
    """Abstract data loader - Open/Closed Principle"""
    
    @abstractmethod
    def load(self, source: str) -> str:
        """Load and return data as string"""
        pass


class LZ4DataLoader(DataLoader):
    """Concrete loader for LZ4 compressed files"""
    
    def load(self, source: str) -> str:
        try:
            result = subprocess.run(
                ['lz4', '-d', '-c', source],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            raise RuntimeError(f"Failed to load LZ4 data: {e}")


class PlainDataLoader(DataLoader):
    """Concrete loader for plain text files"""
    
    def load(self, source: str) -> str:
        try:
            with open(source, 'r') as f:
                return f.read()
        except FileNotFoundError as e:
            raise RuntimeError(f"Failed to load data: {e}")


class DataLoaderFactory:
    """Factory for creating appropriate data loaders"""
    
    @staticmethod
    def create(source: str) -> DataLoader:
        if source.endswith('.lz4'):
            return LZ4DataLoader()
        return PlainDataLoader()


# ============== Domain Layer ==============

@dataclass
class SearchCriteria:
    """Value object for search criteria"""
    conditions: Dict[str, Any]
    
    def __post_init__(self):
        """Convert all values to strings for CSV comparison"""
        self.conditions = {k: str(v) for k, v in self.conditions.items()}
    
    @property
    def keys(self) -> List[str]:
        return list(self.conditions.keys())


@dataclass
class WeightedValue:
    """Value object for weighted value calculation"""
    value: int
    weight: int
    
    @property
    def weighted_sum(self) -> int:
        return self.value * self.weight
    
    @staticmethod
    def from_value(value: int) -> 'WeightedValue':
        weight = 10 if value % 2 != 0 else 20
        return WeightedValue(value=value, weight=weight)


# ============== Service Layer ==============

class KeyMismatch(Exception):
    """Exception raised when search keys don't match data headers."""
    pass


class DataParseError(Exception):
    """Exception raised when data parsing fails."""
    pass


class DataParser:
    """Parser for CSV data - Single Responsibility"""
    
    @lru_cache(maxsize=32)
    def parse(self, data: str) -> tuple:
        """Parse CSV and return headers and rows as tuples"""
        try:
            reader = csv.DictReader(io.StringIO(data))
            headers = tuple(reader.fieldnames)
            rows = tuple(tuple(row.values()) for row in reader)
            return headers, rows
        except Exception as e:
            raise DataParseError(f"Failed to parse CSV: {e}")


class WeightedAverageCalculator:
    """
    Calculator for weighted average - Single Responsibility
    Uses dict-based O(1) lookup for optimal performance
    """
    
    def calculate(
        self, 
        search_criteria: List[SearchCriteria], 
        headers: tuple, 
        rows: tuple
    ) -> str:
        """
        Calculate weighted average using hash-based lookup.
        Time complexity: O(rows + searches)
        """
        headers_set = set(headers)
        
        for criteria in search_criteria:
            for key in criteria.keys:
                if key not in headers_set:
                    raise KeyMismatch("Key mismatch")
        
        header_to_idx = {header: idx for idx, header in enumerate(headers)}
        value_idx = header_to_idx['value']
        
        search_dict = {
            tuple(criteria.conditions[k] for k in sorted(criteria.conditions.keys())): idx
            for idx, criteria in enumerate(search_criteria)
        }
        
        if search_criteria:
            col_indices = [header_to_idx[k] for k in sorted(search_criteria[0].conditions.keys())]
        else:
            col_indices = []
        
        n = len(search_criteria)
        weighted_sums = [0] * n
        total_weights = [0] * n
        
        for row in rows:
            row_key = tuple(row[idx] for idx in col_indices)
            idx = search_dict.get(row_key)
            
            if idx is not None:
                row_value = int(row[value_idx])
                weight = 10 if row_value % 2 != 0 else 20
                weighted_sums[idx] += row_value * weight
                total_weights[idx] += weight
        
        total_weighted = sum(weighted_sums)
        total_weight = sum(total_weights)
        
        if total_weight == 0:
            return "0.0"
        
        return f"{(total_weighted / total_weight):.1f}"


class Task2Service:
    """
    Service orchestrator - Facade Pattern
    Coordinates data loading, parsing, and calculation
    """
    
    def __init__(
        self,
        data_loader: Optional[DataLoader] = None,
        parser: Optional[DataParser] = None,
        calculator: Optional[WeightedAverageCalculator] = None
    ):
        self._data_loader = data_loader
        self._parser = parser or DataParser()
        self._calculator = calculator or WeightedAverageCalculator()
    
    def execute(self, search_list: List[Dict[str, Any]], data_source: str) -> str:
        """
        Execute weighted average calculation.
        
        Args:
            search_list: List of search criteria dictionaries
            data_source: Path to data file or CSV string with headers
            
        Returns:
            Weighted average rounded to 1 decimal place as string
        """
        if self._data_loader is None:
            self._data_loader = DataLoaderFactory.create(data_source)
        
        data = self._data_loader.load(data_source)
        headers, rows = self._parser.parse(data)
        search_criteria = [SearchCriteria(conditions=s) for s in search_list]
        
        return self._calculator.calculate(search_criteria, headers, rows)


# ============== Entry Point ==============

def task2(search_list: List[Dict[str, Any]], data_source: str) -> str:
    """
    Calculate weighted average for matching rows.
    
    Args:
        search_list: List of dictionaries with column names and values
        data_source: Path to data file or CSV string with headers
        
    Returns:
        Weighted average rounded to 1 decimal place as string
    """
    service = Task2Service()
    
    # Determine if data_source is a file path or raw CSV data,
    #  because keeping the 400MB of data in a variable
    #  (see data example from the task) is a great idea...
    if '\n' in data_source:
        parser = DataParser()
        calculator = WeightedAverageCalculator()
        headers, rows = parser.parse(data_source)
        search_criteria = [SearchCriteria(conditions=s) for s in search_list]
        return calculator.calculate(search_criteria, headers, rows)
    
    return service.execute(search_list, data_source)


if __name__ == "__main__":
    DATA_SOURCE = "data/find_match_average_v2.dat.lz4"
    SEARCH_LIST = [
        {'a': 862984, 'b': 29105, 'c': 605280, 'd': 678194, 'e': 302120},
        {'a': 20226, 'b': 781899, 'c': 186952, 'd': 506894, 'e': 325696}
    ]
    
    result = task2(SEARCH_LIST, DATA_SOURCE)
    print(f"Result: {result}")
