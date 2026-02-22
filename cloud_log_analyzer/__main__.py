"""CLI entry point for cloud log analyzer."""

import argparse
import sys
from datetime import datetime, timedelta
from cloud_log_analyzer.collectors import AWSCollector, AzureCollector, GCPCollector
from cloud_log_analyzer.storage import ElasticsearchStorage
from cloud_log_analyzer.analyzer import LogAnalyzer


def parse_time_range(time_str):
    """Parse time range string like '24h', '7d', '1h'."""
    unit = time_str[-1]
    value = int(time_str[:-1])
    
    if unit == 'h':
        return timedelta(hours=value)
    elif unit == 'd':
        return timedelta(days=value)
    elif unit == 'm':
        return timedelta(minutes=value)
    else:
        raise ValueError(f"Invalid time unit: {unit}")


def collect_command(args):
    """Collect logs from cloud providers."""
    storage = ElasticsearchStorage()
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=args.hours)
    
    if args.provider == 'aws':
        collector = AWSCollector()
        logs = collector.fetch_logs(args.log_group, start_time, end_time)
    elif args.provider == 'azure':
        collector = AzureCollector()
        logs = collector.fetch_logs(args.workspace_id, start_time, end_time)
    elif args.provider == 'gcp':
        collector = GCPCollector()
        logs = collector.fetch_logs(args.project, start_time, end_time)
    else:
        print(f"Unknown provider: {args.provider}")
        return
    
    count = storage.bulk_index(logs)
    print(f"Indexed {count} logs from {args.provider}")


def query_command(args):
    """Query logs from Elasticsearch."""
    storage = ElasticsearchStorage()
    
    if args.last:
        time_range = parse_time_range(args.last)
        start_time = datetime.utcnow() - time_range
    else:
        start_time = None
    
    results = storage.search_logs(text=args.text, start_time=start_time, limit=args.limit)
    
    print(f"\nFound {len(results)} matching logs:\n")
    for log in results:
        timestamp = log.get('timestamp', 'N/A')
        level = log.get('level', 'INFO')
        message = log.get('message', '')[:100]
        print(f"[{timestamp}] {level}: {message}")


def stats_command(args):
    """Generate log statistics."""
    analyzer = LogAnalyzer()
    
    if args.last:
        time_range = parse_time_range(args.last)
        start_time = datetime.utcnow() - time_range
    else:
        start_time = datetime.utcnow() - timedelta(hours=24)
    
    stats = analyzer.generate_stats(start_time=start_time, group_by=args.group_by)
    
    print(f"\nLog Statistics (Last {args.last or '24h'})")
    print("=" * 50)
    print(f"Total Logs: {stats['total']:,}")
    print(f"\nBreakdown by {args.group_by}:")
    for key, count in stats['breakdown'].items():
        percentage = (count / stats['total'] * 100) if stats['total'] > 0 else 0
        print(f"  {key}: {count:,} ({percentage:.2f}%)")


def analyze_command(args):
    """Analyze logs for patterns and trends."""
    analyzer = LogAnalyzer()
    
    if args.last:
        time_range = parse_time_range(args.last)
        start_time = datetime.utcnow() - time_range
    else:
        start_time = datetime.utcnow() - timedelta(days=7)
    
    if args.type == 'errors':
        results = analyzer.analyze_errors(start_time=start_time)
        print(f"\nTop Error Messages:")
        print("=" * 50)
        for i, (message, count) in enumerate(results[:10], 1):
            print(f"{i}. {message} ({count} occurrences)")


def main():
    parser = argparse.ArgumentParser(description="Cloud Log Analyzer")
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Collect command
    collect_parser = subparsers.add_parser('collect', help='Collect logs from cloud providers')
    collect_parser.add_argument('provider', choices=['aws', 'azure', 'gcp'])
    collect_parser.add_argument('--log-group', help='AWS log group name')
    collect_parser.add_argument('--workspace-id', help='Azure workspace ID')
    collect_parser.add_argument('--project', help='GCP project ID')
    collect_parser.add_argument('--hours', type=int, default=24, help='Hours to fetch')
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Query logs')
    query_parser.add_argument('--text', help='Search text')
    query_parser.add_argument('--last', help='Time range (e.g., 1h, 24h, 7d)')
    query_parser.add_argument('--limit', type=int, default=50, help='Result limit')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Generate statistics')
    stats_parser.add_argument('--group-by', default='level', help='Group by field')
    stats_parser.add_argument('--last', help='Time range (e.g., 1h, 24h, 7d)')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze logs')
    analyze_parser.add_argument('--type', choices=['errors'], default='errors')
    analyze_parser.add_argument('--last', help='Time range (e.g., 1h, 24h, 7d)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'collect':
        collect_command(args)
    elif args.command == 'query':
        query_command(args)
    elif args.command == 'stats':
        stats_command(args)
    elif args.command == 'analyze':
        analyze_command(args)


if __name__ == '__main__':
    main()
