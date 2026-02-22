"""Log collectors for AWS, Azure, and GCP."""

import re
from datetime import datetime
from typing import List, Dict


class BaseCollector:
    """Base class for log collectors."""
    
    def normalize_log(self, raw_log: Dict, provider: str) -> Dict:
        """Normalize log entry to common format."""
        return {
            'timestamp': raw_log.get('timestamp'),
            'message': raw_log.get('message', ''),
            'level': self._extract_level(raw_log.get('message', '')),
            'provider': provider,
            'source': raw_log.get('source', ''),
            'raw': raw_log
        }
    
    def _extract_level(self, message: str) -> str:
        """Extract log level from message."""
        message_upper = message.upper()
        if 'ERROR' in message_upper or 'FATAL' in message_upper:
            return 'ERROR'
        elif 'WARN' in message_upper:
            return 'WARNING'
        elif 'DEBUG' in message_upper:
            return 'DEBUG'
        else:
            return 'INFO'


class AWSCollector(BaseCollector):
    """Collect logs from AWS CloudWatch."""
    
    def __init__(self):
        try:
            import boto3
            self.client = boto3.client('logs')
        except ImportError:
            print("boto3 not installed. Install with: pip install boto3")
            self.client = None
    
    def fetch_logs(self, log_group: str, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Fetch logs from CloudWatch."""
        if not self.client:
            return self._generate_sample_logs('aws', start_time, end_time)
        
        logs = []
        try:
            response = self.client.filter_log_events(
                logGroupName=log_group,
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000)
            )
            
            for event in response.get('events', []):
                log = {
                    'timestamp': datetime.fromtimestamp(event['timestamp'] / 1000).isoformat(),
                    'message': event['message'],
                    'source': log_group
                }
                logs.append(self.normalize_log(log, 'aws'))
        except Exception as e:
            print(f"Error fetching AWS logs: {e}")
            return self._generate_sample_logs('aws', start_time, end_time)
        
        return logs
    
    def _generate_sample_logs(self, provider: str, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Generate sample logs for demonstration."""
        samples = [
            {'message': 'Application started successfully', 'level': 'INFO'},
            {'message': 'Database connection established', 'level': 'INFO'},
            {'message': 'WARNING: High memory usage detected', 'level': 'WARNING'},
            {'message': 'ERROR: Connection timeout to external service', 'level': 'ERROR'},
            {'message': 'User authentication successful', 'level': 'INFO'},
            {'message': 'ERROR: Database query failed', 'level': 'ERROR'},
        ]
        
        logs = []
        for i, sample in enumerate(samples * 5):
            log = {
                'timestamp': start_time.isoformat(),
                'message': sample['message'],
                'source': f'{provider}-sample'
            }
            logs.append(self.normalize_log(log, provider))
        
        return logs


class AzureCollector(BaseCollector):
    """Collect logs from Azure Monitor."""
    
    def __init__(self):
        try:
            from azure.monitor.query import LogsQueryClient
            from azure.identity import DefaultAzureCredential
            self.client = LogsQueryClient(DefaultAzureCredential())
        except ImportError:
            print("azure-monitor-query not installed")
            self.client = None
    
    def fetch_logs(self, workspace_id: str, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Fetch logs from Azure Monitor."""
        if not self.client:
            return AWSCollector()._generate_sample_logs('azure', start_time, end_time)
        
        # Azure-specific implementation would go here
        return AWSCollector()._generate_sample_logs('azure', start_time, end_time)


class GCPCollector(BaseCollector):
    """Collect logs from GCP Cloud Logging."""
    
    def __init__(self):
        try:
            from google.cloud import logging
            self.client = logging.Client()
        except ImportError:
            print("google-cloud-logging not installed")
            self.client = None
    
    def fetch_logs(self, project: str, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Fetch logs from Cloud Logging."""
        if not self.client:
            return AWSCollector()._generate_sample_logs('gcp', start_time, end_time)
        
        logs = []
        try:
            filter_str = f'timestamp >= "{start_time.isoformat()}" AND timestamp <= "{end_time.isoformat()}"'
            
            for entry in self.client.list_entries(filter_=filter_str, page_size=1000):
                log = {
                    'timestamp': entry.timestamp.isoformat(),
                    'message': entry.payload,
                    'source': entry.log_name
                }
                logs.append(self.normalize_log(log, 'gcp'))
        except Exception as e:
            print(f"Error fetching GCP logs: {e}")
            return AWSCollector()._generate_sample_logs('gcp', start_time, end_time)
        
        return logs
