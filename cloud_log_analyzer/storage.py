"""Elasticsearch storage operations."""

import os
from datetime import datetime
from typing import List, Dict, Optional


class ElasticsearchStorage:
    """Handle Elasticsearch operations for log storage."""
    
    def __init__(self, host: Optional[str] = None):
        self.host = host or os.getenv('ELASTICSEARCH_HOST', 'localhost:9200')
        self.index_prefix = 'cloud-logs'
        
        try:
            from elasticsearch import Elasticsearch
            self.es = Elasticsearch([f'http://{self.host}'])
            self.es.info()
            self._create_index_template()
        except ImportError:
            print("elasticsearch package not installed. Using mock storage.")
            self.es = None
            self.mock_storage = []
        except Exception as e:
            print(f"Could not connect to Elasticsearch: {e}. Using mock storage.")
            self.es = None
            self.mock_storage = []
    
    def _create_index_template(self):
        """Create index template for log data."""
        if not self.es:
            return
        
        template = {
            'index_patterns': [f'{self.index_prefix}-*'],
            'mappings': {
                'properties': {
                    'timestamp': {'type': 'date'},
                    'message': {'type': 'text'},
                    'level': {'type': 'keyword'},
                    'provider': {'type': 'keyword'},
                    'source': {'type': 'keyword'}
                }
            }
        }
        
        try:
            self.es.indices.put_index_template(
                name=f'{self.index_prefix}-template',
                body=template
            )
        except Exception as e:
            print(f"Warning: Could not create index template: {e}")
    
    def _get_index_name(self) -> str:
        """Generate index name with current date."""
        return f"{self.index_prefix}-{datetime.utcnow().strftime('%Y.%m.%d')}"
    
    def bulk_index(self, logs: List[Dict]) -> int:
        """Bulk index logs into Elasticsearch."""
        if not logs:
            return 0
        
        if not self.es:
            # Mock storage
            self.mock_storage.extend(logs)
            return len(logs)
        
        from elasticsearch.helpers import bulk
        
        actions = [
            {
                '_index': self._get_index_name(),
                '_source': log
            }
            for log in logs
        ]
        
        try:
            success, failed = bulk(self.es, actions)
            return success
        except Exception as e:
            print(f"Error indexing logs: {e}")
            return 0
    
    def search_logs(self, text: Optional[str] = None, 
                   start_time: Optional[datetime] = None,
                   limit: int = 50) -> List[Dict]:
        """Search logs with optional filters."""
        if not self.es:
            # Mock search
            results = self.mock_storage
            if text:
                results = [log for log in results if text.lower() in log.get('message', '').lower()]
            return results[:limit]
        
        query = {'bool': {'must': []}}
        
        if text:
            query['bool']['must'].append({
                'match': {'message': text}
            })
        
        if start_time:
            query['bool']['must'].append({
                'range': {
                    'timestamp': {
                        'gte': start_time.isoformat()
                    }
                }
            })
        
        if not query['bool']['must']:
            query = {'match_all': {}}
        
        try:
            response = self.es.search(
                index=f'{self.index_prefix}-*',
                body={'query': query, 'size': limit, 'sort': [{'timestamp': 'desc'}]}
            )
            
            return [hit['_source'] for hit in response['hits']['hits']]
        except Exception as e:
            print(f"Error searching logs: {e}")
            return []
    
    def aggregate(self, field: str, start_time: Optional[datetime] = None) -> Dict:
        """Aggregate logs by field."""
        if not self.es:
            # Mock aggregation
            from collections import Counter
            values = [log.get(field, 'unknown') for log in self.mock_storage]
            return dict(Counter(values))
        
        query = {'match_all': {}}
        if start_time:
            query = {
                'range': {
                    'timestamp': {
                        'gte': start_time.isoformat()
                    }
                }
            }
        
        agg = {
            'group_by_field': {
                'terms': {'field': field, 'size': 100}
            }
        }
        
        try:
            response = self.es.search(
                index=f'{self.index_prefix}-*',
                body={'query': query, 'aggs': agg, 'size': 0}
            )
            
            buckets = response['aggregations']['group_by_field']['buckets']
            return {bucket['key']: bucket['doc_count'] for bucket in buckets}
        except Exception as e:
            print(f"Error aggregating logs: {e}")
            return {}
