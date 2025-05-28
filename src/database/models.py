# src/database/models.py
"""
Database Models and Schema Definitions
"""

from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime

@dataclass
class Province:
    province_id: int
    province_code: str
    province_name: str
    region: Optional[str] = None
    population_estimate: Optional[int] = None
    created_at: Optional[datetime] = None

@dataclass
class Procedure:
    procedure_id: int
    procedure_code: str
    procedure_name: str
    procedure_category: str
    description: Optional[str] = None
    is_surgery: bool = False
    created_at: Optional[datetime] = None

@dataclass
class Metric:
    metric_id: int
    metric_code: str
    metric_name: str
    metric_type: str
    unit_of_measurement: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[datetime] = None

@dataclass
class WaitTime:
    wait_time_id: str
    province_id: int
    procedure_id: int
    metric_id: int
    reporting_level_id: int
    data_year: int
    indicator_result: Optional[float] = None
    is_estimate: bool = False
    data_quality_flag: Optional[str] = None
    region_name: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

