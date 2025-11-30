import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame, Column
from functools import wraps
from typing import Callable

def has_columns(*column_args: str):
    """Decorator that verifies the dataframe contains the specified columns.
    
    Args:
        *column_args: Column parameter names to verify in the dataframe.
    
    Returns:
        A decorator function that checks columns before executing the wrapped function.
    """
    def decorator(fn: Callable):
        @wraps(fn)
        def wrapper(dataframe: DataFrame, *args, **kwargs):
            # Extract column names from kwargs and function signature
            columns_to_check = []
            param_names = list(fn.__code__.co_varnames[1:])  # Skip 'dataframe'
            
            for arg in column_args:
                # First try kwargs
                if arg in kwargs:
                    columns_to_check.append(kwargs[arg])
                # Then try positional args
                elif arg in param_names:
                    idx = param_names.index(arg)
                    if idx < len(args):
                        columns_to_check.append(args[idx])
                    # Otherwise get from function defaults
                    else:
                        # Find default values
                        defaults = fn.__defaults__ or ()
                        num_defaults = len(defaults)
                        default_start = len(param_names) - num_defaults
                        if idx >= default_start:
                            default_idx = idx - default_start
                            columns_to_check.append(defaults[default_idx])
            
            # Verify all required columns exist in dataframe
            missing_columns = [col for col in columns_to_check if col not in dataframe.columns]
            if missing_columns:
                raise ValueError(f"Dataframe is missing the following columns: {missing_columns}")
            
            return fn(dataframe, *args, **kwargs)
        return wrapper
    return decorator


def columnize(*column_params: str):
    """Decorator that converts string column parameters to Column objects.
    
    Args:
        *column_params: Parameter names that should be converted from strings to Column objects.
    
    Returns:
        A decorator function that converts specified string parameters to F.col() objects.
    """
    def decorator(fn: Callable):
        @wraps(fn)
        def wrapper(dataframe: DataFrame, *args, **kwargs):
            # Get function parameter names
            param_names = list(fn.__code__.co_varnames[1:])  # Skip 'dataframe'
            
            # Convert specified parameters from strings to Column objects
            for param in column_params:
                # Check in kwargs
                if param in kwargs and isinstance(kwargs[param], str):
                    kwargs[param] = F.col(kwargs[param])
                # Check in positional args
                elif param in param_names:
                    idx = param_names.index(param)
                    if idx < len(args) and isinstance(args[idx], str):
                        args = list(args)
                        args[idx] = F.col(args[idx])
                        args = tuple(args)
            
            return fn(dataframe, *args, **kwargs)
        return wrapper
    return decorator

def datify(dataframe: DataFrame, col: str | Column = "date") -> DataFrame:
    """Cast a column to a date value"""
    return dataframe.withColumn(col, F.to_date(col))