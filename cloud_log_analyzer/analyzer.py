"""Log analysis and statistics generation."""

from datetime import datetime
from typing import Dict, List, Tuple, Optional
from collections import Counter
from cloud_log_analyzer.storage import ElasticsearchStorage


class LogAnalyzer:
    """Analyze logs and generate insights."""
    
    def __init__(self):
        self.storage = ElasticsearchStorage()
    
    def generate_stats(self, start_time: Optional[datetime] = None, 
                      group_by: str = 'level') -> Dict:
        """Generate statistics grouped by specified field."""
        logs = self.storage.search_logs(start_time=start_time, limit=10000)
        
        total = len(logs)
        breakdown = Counter(log.get(group_by, 'unknown') for log in logs)
        
        return {
            'total': total,
            'breakdown': dict(breakdown),
            'start_time': start_time.isoformat() if start_time else None
        }
    
    def analyze_errors(self, start_time: Optional[datetime] = None) -> List[Tuple[str, int]]:
        """Analyze error messages and return top errors."""
        logs = self.storage.search_logs(start_time=start_time, limit=10000)
        
        error_logs = [
            log for log in logs 
            if log.get('level') in ['ERROR', 'FATAL']
        ]
        
        # Extract and count error messages
        error_messages = []
        for log in error_logs:
            message = log.get('message', '')
            # Simplify error message by removing dynamic parts
            simplified = self._simplify_error_message(message)
            error_messages.append(simplified)
        
        error_counts = Counter(error_messages)
        return error_counts.most_common(10)
    
    def _simplify_error_message(self, message: str) -> str:
        """Simplify error message by removing dynamic content."""
        import re
        
        # Remove timestamps
        message = re.sub(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}', '[TIMESTAMP]', message)
        
        # Remove IDs and hashes
        message = re.sub(r'\b[0-9a-f]{8,}\b', '[ID]', message, flags=re.IGNORECASE)
        
        # Remove IP addresses
        message = re.sub(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', '[IP]', message)
        
        # Remove numbers
        message = re.sub(r'\b\d+\b', '[NUM]', message)
        
        # Truncate long messages
        if len(message) > 100:
            message = message[:100] + '...'
        
        return message.strip()
    
    def get_time_series(self, start_time: datetime, interval: str = '1h') -> Dict:
        """Get log count time series."""
        # This would use Elasticsearch date histogram aggregation
        # Simplified version for demonstration
        logs = self.storage.search_logs(start_time=start_time, limit=10000)
        
        from collections import defaultdict
        time_buckets = defaultdict(int)
        
        for log in logs:
            timestamp = log.get('timestamp', '')
            if timestamp:
                # Bucket by hour
                bucket = timestamp[:13]  # YYYY-MM-DDTHH
                time_buckets[bucket] += 1
        
        return dict(sorted(time_buckets.items()))
    
    def detect_anomalies(self, start_time: Optional[datetime] = None) -> List[Dict]:
        """Detect anomalies in log patterns."""
        logs = self.storage.search_logs(start_time=start_time, limit=10000)
        
        anomalies = []
        
        # Simple anomaly detection: sudden spike in errors
        error_logs = [log for log in logs if log.get('level') == 'ERROR']
        error_rate = len(error_logs) / len(logs) if logs else 0
        
        if error_rate > 0.1:  # More than 10% errors
            anomalies.append({
                'type': 'high_error_rate',
                'severity': 'high',
                'message': f'Error rate is {error_rate*100:.1f}% (threshold: 10%)',
                'count': len(error_logs)
            })
        
        # Check for repeated errors
        error_messages = [log.get('message', '') for log in error_logs]
        message_counts = Counter(error_messages)
        
        for message, count in message_counts.most_common(5):
            if count > 10:
                anomalies.append({
                    'type': 'repeated_error',
                    'severity': 'medium',
                    'message': f'Error repeated {count} times: {message[:50]}...',
                    'count': count
                })
        
        return anomalies
