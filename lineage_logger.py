import json
import csv
import uuid
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import os

class LineageLogger:
    def __init__(self, events_dir: str = "lineage/events"):
        self.events_dir = Path(events_dir)
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.csv_file = self.events_dir / "events.csv"
        self._initialize_csv()
    
    def _initialize_csv(self):
        if not self.csv_file.exists():
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'event_time', 'run_id', 'job_namespace', 'job_name', 
                    'input_names', 'output_names', 'transform', 'columns_map',
                    'rowcount_in', 'rowcount_out', 'status', 'duration_ms'
                ])
    
    def start_run(self, job_namespace: str, job_name: str) -> str:
        run_id = str(uuid.uuid4())
        start_time = datetime.now().isoformat()
        
        event = {
            'eventType': 'START',
            'eventTime': start_time,
            'run': {
                'runId': run_id,
                'facets': {}
            },
            'job': {
                'namespace': job_namespace,
                'name': job_name,
                'facets': {}
            },
            'inputs': [],
            'outputs': []
        }
        
        self._write_json_event(run_id, event)
        return run_id
    
    def log_io(self, run_id: str, inputs: List[Dict[str, Any]] = None, 
               outputs: List[Dict[str, Any]] = None, schema_facets: Dict = None, 
               row_counts: Dict[str, int] = None):
        event_time = datetime.now().isoformat()
        
        event = {
            'eventType': 'RUNNING',
            'eventTime': event_time,
            'run': {
                'runId': run_id,
                'facets': {}
            },
            'inputs': inputs or [],
            'outputs': outputs or [],
            'schema_facets': schema_facets or {},
            'row_counts': row_counts or {}
        }
        
        self._write_json_event(run_id, event, suffix='_io')
    
    def log_column_lineage(self, run_id: str, mappings: List[Dict[str, Any]]):
        event_time = datetime.now().isoformat()
        
        event = {
            'eventType': 'COLUMN_LINEAGE',
            'eventTime': event_time,
            'run': {
                'runId': run_id
            },
            'columnLineage': {
                'fields': mappings
            }
        }
        
        self._write_json_event(run_id, event, suffix='_lineage')
    
    def end_run(self, run_id: str, status: str = "COMPLETED", duration_ms: int = None,
                job_namespace: str = "", job_name: str = "", 
                input_names: str = "", output_names: str = "",
                transform: str = "", columns_map: str = "",
                rowcount_in: int = 0, rowcount_out: int = 0):
        end_time = datetime.now().isoformat()
        
        event = {
            'eventType': 'COMPLETE',
            'eventTime': end_time,
            'run': {
                'runId': run_id,
                'facets': {
                    'status': status,
                    'duration_ms': duration_ms
                }
            }
        }
        
        self._write_json_event(run_id, event, suffix='_complete')
        
        # Write to CSV
        with open(self.csv_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                end_time, run_id, job_namespace, job_name,
                input_names, output_names, transform, columns_map,
                rowcount_in, rowcount_out, status, duration_ms
            ])
    
    def _write_json_event(self, run_id: str, event: Dict[str, Any], suffix: str = ""):
        filename = f"{run_id}{suffix}.json"
        filepath = self.events_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(event, f, indent=2)

# Global logger instance
logger = LineageLogger()

def start_run(job_namespace: str, job_name: str) -> str:
    return logger.start_run(job_namespace, job_name)

def log_io(run_id: str, inputs: List[Dict[str, Any]] = None, 
           outputs: List[Dict[str, Any]] = None, schema_facets: Dict = None, 
           row_counts: Dict[str, int] = None):
    return logger.log_io(run_id, inputs, outputs, schema_facets, row_counts)

def log_column_lineage(run_id: str, mappings: List[Dict[str, Any]]):
    return logger.log_column_lineage(run_id, mappings)

def end_run(run_id: str, status: str = "COMPLETED", duration_ms: int = None,
            job_namespace: str = "", job_name: str = "", 
            input_names: str = "", output_names: str = "",
            transform: str = "", columns_map: str = "",
            rowcount_in: int = 0, rowcount_out: int = 0):
    return logger.end_run(run_id, status, duration_ms, job_namespace, job_name,
                         input_names, output_names, transform, columns_map,
                         rowcount_in, rowcount_out)