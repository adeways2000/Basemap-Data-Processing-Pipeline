"""
Metrics Collection System

Implements comprehensive metrics collection and monitoring for the basemap
data processing pipeline. Provides real-time visibility into system performance,
data quality, and operational health.

This module demonstrates:
- Application performance metrics
- Business metrics tracking
- Custom metric definitions
- Integration with monitoring systems (Prometheus, CloudWatch)
- Real-time dashboards and alerting
- SLA monitoring and reporting
"""

import time
import threading
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass, field
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json
import statistics
import os

import boto3
import structlog
from prometheus_client import Counter, Histogram, Gauge, Summary, CollectorRegistry, push_to_gateway


@dataclass
class MetricValue:
    """Represents a single metric value with metadata."""
    name: str
    value: Union[int, float]
    timestamp: datetime
    labels: Dict[str, str] = field(default_factory=dict)
    unit: str = ""
    description: str = ""


@dataclass
class AlertThreshold:
    """Defines alert thresholds for metrics."""
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    comparison_operator: str = "greater_than"  # greater_than, less_than, equal_to
    evaluation_period: int = 300  # seconds
    datapoints_to_alarm: int = 2


class MetricsCollector:
    """
    Comprehensive metrics collection system for basemap processing pipeline.
    
    Supports multiple backends including Prometheus, CloudWatch, and custom
    storage systems. Provides real-time metrics collection, aggregation,
    and alerting capabilities.
    """
    
    def __init__(
        self,
        enable_prometheus: bool = True,
        enable_cloudwatch: bool = True,
        prometheus_gateway: Optional[str] = None,
        cloudwatch_namespace: str = "BasemapProcessing",
        aws_region: str = "us-west-2"
    ):
        """
        Initialize the metrics collector.
        
        Args:
            enable_prometheus: Enable Prometheus metrics collection
            enable_cloudwatch: Enable CloudWatch metrics collection
            prometheus_gateway: Prometheus pushgateway URL
            cloudwatch_namespace: CloudWatch namespace for metrics
            aws_region: AWS region for CloudWatch
        """
        self.enable_prometheus = enable_prometheus
        self.enable_cloudwatch = enable_cloudwatch
        self.prometheus_gateway = prometheus_gateway
        self.cloudwatch_namespace = cloudwatch_namespace
        self.aws_region = aws_region
        
        # Set up logging
        self.logger = structlog.get_logger(collector_type="MetricsCollector")
        
        # Initialize metric storage
        self.metrics_buffer = deque(maxlen=10000)  # Ring buffer for recent metrics
        self.metric_aggregates = defaultdict(list)
        self.alert_thresholds = {}
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Initialize backends
        if self.enable_prometheus:
            self._init_prometheus()
        
        if self.enable_cloudwatch:
            self._init_cloudwatch()
        
        # Built-in metrics
        self._init_builtin_metrics()
        
        # Background aggregation
        self._start_aggregation_thread()
        
        self.logger.info(
            "Metrics collector initialized",
            prometheus_enabled=enable_prometheus,
            cloudwatch_enabled=enable_cloudwatch
        )
    
    def _init_prometheus(self) -> None:
        """Initialize Prometheus metrics."""
        try:
            self.prometheus_registry = CollectorRegistry()
            
            # Define Prometheus metrics
            self.prometheus_counters = {}
            self.prometheus_histograms = {}
            self.prometheus_gauges = {}
            self.prometheus_summaries = {}
            
            # Core application metrics
            self._create_prometheus_metric(
                'counter', 'data_ingestion_records_total',
                'Total number of records ingested',
                ['source_type', 'status']
            )
            
            self._create_prometheus_metric(
                'counter', 'data_processing_jobs_total',
                'Total number of data processing jobs',
                ['job_type', 'status']
            )
            
            self._create_prometheus_metric(
                'histogram', 'data_processing_duration_seconds',
                'Duration of data processing operations',
                ['operation_type']
            )
            
            self._create_prometheus_metric(
                'gauge', 'active_spark_executors',
                'Number of active Spark executors',
                ['cluster_id']
            )
            
            self._create_prometheus_metric(
                'gauge', 'tile_generation_queue_size',
                'Number of tiles in generation queue',
                ['zoom_level']
            )
            
            self._create_prometheus_metric(
                'histogram', 'tile_generation_duration_seconds',
                'Duration of tile generation operations',
                ['tile_format', 'zoom_level']
            )
            
            self._create_prometheus_metric(
                'counter', 'data_quality_checks_total',
                'Total number of data quality checks',
                ['check_type', 'status']
            )
            
            self._create_prometheus_metric(
                'gauge', 'data_freshness_hours',
                'Age of data in hours',
                ['data_source']
            )
            
            self.logger.info("Prometheus metrics initialized")
            
        except Exception as e:
            self.logger.error("Failed to initialize Prometheus metrics", error=str(e))
            self.enable_prometheus = False
    
    def _init_cloudwatch(self) -> None:
        """Initialize CloudWatch metrics."""
        try:
            self.cloudwatch_client = boto3.client('cloudwatch', region_name=self.aws_region)
            self.cloudwatch_batch = []
            self.cloudwatch_batch_size = 20  # CloudWatch limit
            
            self.logger.info("CloudWatch metrics initialized")
            
        except Exception as e:
            self.logger.error("Failed to initialize CloudWatch metrics", error=str(e))
            self.enable_cloudwatch = False
    
    def _init_builtin_metrics(self) -> None:
        """Initialize built-in system metrics."""
        self.builtin_metrics = {
            'system_start_time': time.time(),
            'total_metrics_collected': 0,
            'metrics_collection_errors': 0,
            'last_metric_timestamp': None
        }
    
    def _create_prometheus_metric(
        self,
        metric_type: str,
        name: str,
        description: str,
        labels: List[str] = None
    ) -> None:
        """Create a Prometheus metric."""
        if labels is None:
            labels = []
        
        try:
            if metric_type == 'counter':
                metric = Counter(
                    name, description, labels,
                    registry=self.prometheus_registry
                )
                self.prometheus_counters[name] = metric
                
            elif metric_type == 'histogram':
                metric = Histogram(
                    name, description, labels,
                    registry=self.prometheus_registry
                )
                self.prometheus_histograms[name] = metric
                
            elif metric_type == 'gauge':
                metric = Gauge(
                    name, description, labels,
                    registry=self.prometheus_registry
                )
                self.prometheus_gauges[name] = metric
                
            elif metric_type == 'summary':
                metric = Summary(
                    name, description, labels,
                    registry=self.prometheus_registry
                )
                self.prometheus_summaries[name] = metric
                
        except Exception as e:
            self.logger.error(
                "Failed to create Prometheus metric",
                metric_name=name,
                error=str(e)
            )
    
    def _start_aggregation_thread(self) -> None:
        """Start background thread for metric aggregation."""
        def aggregation_worker():
            while True:
                try:
                    self._aggregate_metrics()
                    time.sleep(60)  # Aggregate every minute
                except Exception as e:
                    self.logger.error("Metric aggregation error", error=str(e))
                    time.sleep(10)  # Retry after 10 seconds on error
        
        aggregation_thread = threading.Thread(
            target=aggregation_worker,
            daemon=True,
            name="MetricsAggregation"
        )
        aggregation_thread.start()
        
        self.logger.info("Metrics aggregation thread started")
    
    def increment_counter(
        self,
        name: str,
        value: Union[int, float] = 1,
        labels: Dict[str, str] = None,
        description: str = ""
    ) -> None:
        """
        Increment a counter metric.
        
        Args:
            name: Metric name
            value: Value to increment by
            labels: Metric labels
            description: Metric description
        """
        if labels is None:
            labels = {}
        
        try:
            with self.lock:
                # Store in buffer
                metric_value = MetricValue(
                    name=name,
                    value=value,
                    timestamp=datetime.utcnow(),
                    labels=labels,
                    description=description
                )
                self.metrics_buffer.append(metric_value)
                
                # Update built-in metrics
                self.builtin_metrics['total_metrics_collected'] += 1
                self.builtin_metrics['last_metric_timestamp'] = time.time()
                
                # Send to Prometheus
                if self.enable_prometheus and name in self.prometheus_counters:
                    if labels:
                        self.prometheus_counters[name].labels(**labels).inc(value)
                    else:
                        self.prometheus_counters[name].inc(value)
                
                # Send to CloudWatch
                if self.enable_cloudwatch:
                    self._send_to_cloudwatch(name, value, 'Count', labels)
                
        except Exception as e:
            self.builtin_metrics['metrics_collection_errors'] += 1
            self.logger.error(
                "Failed to increment counter",
                metric_name=name,
                error=str(e)
            )
    
    def record_histogram(
        self,
        name: str,
        value: Union[int, float],
        labels: Dict[str, str] = None,
        description: str = ""
    ) -> None:
        """
        Record a histogram metric.
        
        Args:
            name: Metric name
            value: Value to record
            labels: Metric labels
            description: Metric description
        """
        if labels is None:
            labels = {}
        
        try:
            with self.lock:
                # Store in buffer
                metric_value = MetricValue(
                    name=name,
                    value=value,
                    timestamp=datetime.utcnow(),
                    labels=labels,
                    description=description
                )
                self.metrics_buffer.append(metric_value)
                
                # Update built-in metrics
                self.builtin_metrics['total_metrics_collected'] += 1
                self.builtin_metrics['last_metric_timestamp'] = time.time()
                
                # Send to Prometheus
                if self.enable_prometheus and name in self.prometheus_histograms:
                    if labels:
                        self.prometheus_histograms[name].labels(**labels).observe(value)
                    else:
                        self.prometheus_histograms[name].observe(value)
                
                # Send to CloudWatch
                if self.enable_cloudwatch:
                    self._send_to_cloudwatch(name, value, 'None', labels)
                
        except Exception as e:
            self.builtin_metrics['metrics_collection_errors'] += 1
            self.logger.error(
                "Failed to record histogram",
                metric_name=name,
                error=str(e)
            )
    
    def set_gauge(
        self,
        name: str,
        value: Union[int, float],
        labels: Dict[str, str] = None,
        description: str = ""
    ) -> None:
        """
        Set a gauge metric.
        
        Args:
            name: Metric name
            value: Value to set
            labels: Metric labels
            description: Metric description
        """
        if labels is None:
            labels = {}
        
        try:
            with self.lock:
                # Store in buffer
                metric_value = MetricValue(
                    name=name,
                    value=value,
                    timestamp=datetime.utcnow(),
                    labels=labels,
                    description=description
                )
                self.metrics_buffer.append(metric_value)
                
                # Update built-in metrics
                self.builtin_metrics['total_metrics_collected'] += 1
                self.builtin_metrics['last_metric_timestamp'] = time.time()
                
                # Send to Prometheus
                if self.enable_prometheus and name in self.prometheus_gauges:
                    if labels:
                        self.prometheus_gauges[name].labels(**labels).set(value)
                    else:
                        self.prometheus_gauges[name].set(value)
                
                # Send to CloudWatch
                if self.enable_cloudwatch:
                    self._send_to_cloudwatch(name, value, 'None', labels)
                
        except Exception as e:
            self.builtin_metrics['metrics_collection_errors'] += 1
            self.logger.error(
                "Failed to set gauge",
                metric_name=name,
                error=str(e)
            )
    
    def record_timing(
        self,
        name: str,
        duration: float,
        labels: Dict[str, str] = None,
        description: str = ""
    ) -> None:
        """
        Record a timing metric.
        
        Args:
            name: Metric name
            duration: Duration in seconds
            labels: Metric labels
            description: Metric description
        """
        self.record_histogram(name, duration, labels, description)
    
    def time_function(self, name: str, labels: Dict[str, str] = None):
        """
        Decorator to time function execution.
        
        Args:
            name: Metric name
            labels: Metric labels
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    duration = time.time() - start_time
                    self.record_timing(name, duration, labels)
                    return result
                except Exception as e:
                    duration = time.time() - start_time
                    error_labels = {**(labels or {}), 'status': 'error'}
                    self.record_timing(name, duration, error_labels)
                    raise
            return wrapper
        return decorator
    
    def _send_to_cloudwatch(
        self,
        name: str,
        value: Union[int, float],
        unit: str,
        labels: Dict[str, str] = None
    ) -> None:
        """Send metric to CloudWatch."""
        try:
            # Prepare CloudWatch metric data
            metric_data = {
                'MetricName': name,
                'Value': float(value),
                'Unit': unit,
                'Timestamp': datetime.utcnow()
            }
            
            # Add dimensions from labels
            if labels:
                dimensions = []
                for key, val in labels.items():
                    dimensions.append({
                        'Name': key,
                        'Value': str(val)
                    })
                metric_data['Dimensions'] = dimensions
            
            # Add to batch
            self.cloudwatch_batch.append(metric_data)
            
            # Send batch if it's full
            if len(self.cloudwatch_batch) >= self.cloudwatch_batch_size:
                self._flush_cloudwatch_batch()
                
        except Exception as e:
            self.logger.error(
                "Failed to send metric to CloudWatch",
                metric_name=name,
                error=str(e)
            )
    
    def _flush_cloudwatch_batch(self) -> None:
        """Flush CloudWatch metrics batch."""
        if not self.cloudwatch_batch:
            return
        
        try:
            self.cloudwatch_client.put_metric_data(
                Namespace=self.cloudwatch_namespace,
                MetricData=self.cloudwatch_batch
            )
            
            self.logger.debug(
                "Flushed CloudWatch metrics batch",
                batch_size=len(self.cloudwatch_batch)
            )
            
            self.cloudwatch_batch.clear()
            
        except Exception as e:
            self.logger.error(
                "Failed to flush CloudWatch batch",
                error=str(e)
            )
    
    def _aggregate_metrics(self) -> None:
        """Aggregate metrics for reporting and alerting."""
        try:
            with self.lock:
                # Get metrics from the last minute
                cutoff_time = datetime.utcnow() - timedelta(minutes=1)
                recent_metrics = [
                    m for m in self.metrics_buffer
                    if m.timestamp >= cutoff_time
                ]
                
                # Group metrics by name and labels
                grouped_metrics = defaultdict(list)
                for metric in recent_metrics:
                    key = (metric.name, tuple(sorted(metric.labels.items())))
                    grouped_metrics[key].append(metric.value)
                
                # Calculate aggregates
                for (name, labels_tuple), values in grouped_metrics.items():
                    labels = dict(labels_tuple)
                    
                    if values:
                        aggregates = {
                            'count': len(values),
                            'sum': sum(values),
                            'avg': statistics.mean(values),
                            'min': min(values),
                            'max': max(values)
                        }
                        
                        if len(values) > 1:
                            aggregates['stddev'] = statistics.stdev(values)
                            aggregates['p50'] = statistics.median(values)
                            aggregates['p95'] = self._percentile(values, 0.95)
                            aggregates['p99'] = self._percentile(values, 0.99)
                        
                        # Store aggregates
                        self.metric_aggregates[name].append({
                            'timestamp': datetime.utcnow(),
                            'labels': labels,
                            'aggregates': aggregates
                        })
                        
                        # Keep only recent aggregates (last hour)
                        cutoff = datetime.utcnow() - timedelta(hours=1)
                        self.metric_aggregates[name] = [
                            agg for agg in self.metric_aggregates[name]
                            if agg['timestamp'] >= cutoff
                        ]
                
                # Check alert thresholds
                self._check_alert_thresholds()
                
        except Exception as e:
            self.logger.error("Metric aggregation failed", error=str(e))
    
    def _percentile(self, values: List[float], percentile: float) -> float:
        """Calculate percentile value."""
        sorted_values = sorted(values)
        k = (len(sorted_values) - 1) * percentile
        f = int(k)
        c = k - f
        
        if f == len(sorted_values) - 1:
            return sorted_values[f]
        else:
            return sorted_values[f] * (1 - c) + sorted_values[f + 1] * c
    
    def _check_alert_thresholds(self) -> None:
        """Check metrics against alert thresholds."""
        try:
            for threshold in self.alert_thresholds.values():
                metric_name = threshold.metric_name
                
                if metric_name not in self.metric_aggregates:
                    continue
                
                # Get recent aggregates
                cutoff = datetime.utcnow() - timedelta(seconds=threshold.evaluation_period)
                recent_aggregates = [
                    agg for agg in self.metric_aggregates[metric_name]
                    if agg['timestamp'] >= cutoff
                ]
                
                if len(recent_aggregates) < threshold.datapoints_to_alarm:
                    continue
                
                # Check threshold violations
                violations = 0
                for agg in recent_aggregates[-threshold.datapoints_to_alarm:]:
                    value = agg['aggregates']['avg']  # Use average for threshold comparison
                    
                    if self._threshold_violated(value, threshold):
                        violations += 1
                
                # Trigger alert if threshold is violated
                if violations >= threshold.datapoints_to_alarm:
                    self._trigger_alert(threshold, recent_aggregates[-1])
                    
        except Exception as e:
            self.logger.error("Alert threshold checking failed", error=str(e))
    
    def _threshold_violated(self, value: float, threshold: AlertThreshold) -> bool:
        """Check if a value violates the threshold."""
        if threshold.comparison_operator == "greater_than":
            return value > threshold.critical_threshold
        elif threshold.comparison_operator == "less_than":
            return value < threshold.critical_threshold
        elif threshold.comparison_operator == "equal_to":
            return abs(value - threshold.critical_threshold) < 0.001
        else:
            return False
    
    def _trigger_alert(self, threshold: AlertThreshold, aggregate: Dict[str, Any]) -> None:
        """Trigger an alert for threshold violation."""
        try:
            alert_data = {
                'metric_name': threshold.metric_name,
                'threshold_type': 'critical',
                'threshold_value': threshold.critical_threshold,
                'actual_value': aggregate['aggregates']['avg'],
                'timestamp': aggregate['timestamp'].isoformat(),
                'labels': aggregate['labels']
            }
            
            self.logger.warning(
                "Alert threshold violated",
                **alert_data
            )
            
            # Send alert to external systems (SNS, Slack, etc.)
            self._send_alert_notification(alert_data)
            
        except Exception as e:
            self.logger.error("Failed to trigger alert", error=str(e))
    
    def _send_alert_notification(self, alert_data: Dict[str, Any]) -> None:
        """Send alert notification to external systems."""
        try:
            # Send to SNS if configured
            sns_topic_arn = os.environ.get('ALERT_SNS_TOPIC_ARN')
            if sns_topic_arn:
                sns_client = boto3.client('sns', region_name=self.aws_region)
                message = json.dumps(alert_data, indent=2)
                
                sns_client.publish(
                    TopicArn=sns_topic_arn,
                    Message=message,
                    Subject=f"Alert: {alert_data['metric_name']} threshold violated"
                )
            
            # Send to Slack if configured
            slack_webhook = os.environ.get('SLACK_WEBHOOK_URL')
            if slack_webhook:
                import requests
                
                slack_message = {
                    "text": f"🚨 Alert: {alert_data['metric_name']} threshold violated",
                    "attachments": [
                        {
                            "color": "danger",
                            "fields": [
                                {
                                    "title": "Metric",
                                    "value": alert_data['metric_name'],
                                    "short": True
                                },
                                {
                                    "title": "Actual Value",
                                    "value": f"{alert_data['actual_value']:.2f}",
                                    "short": True
                                },
                                {
                                    "title": "Threshold",
                                    "value": f"{alert_data['threshold_value']:.2f}",
                                    "short": True
                                },
                                {
                                    "title": "Timestamp",
                                    "value": alert_data['timestamp'],
                                    "short": True
                                }
                            ]
                        }
                    ]
                }
                
                requests.post(slack_webhook, json=slack_message, timeout=10)
                
        except Exception as e:
            self.logger.error("Failed to send alert notification", error=str(e))
    
    def add_alert_threshold(self, threshold: AlertThreshold) -> None:
        """Add an alert threshold."""
        self.alert_thresholds[threshold.metric_name] = threshold
        self.logger.info(
            "Added alert threshold",
            metric_name=threshold.metric_name,
            critical_threshold=threshold.critical_threshold
        )
    
    def remove_alert_threshold(self, metric_name: str) -> None:
        """Remove an alert threshold."""
        if metric_name in self.alert_thresholds:
            del self.alert_thresholds[metric_name]
            self.logger.info("Removed alert threshold", metric_name=metric_name)
    
    def get_metric_summary(self, metric_name: str, hours: int = 1) -> Dict[str, Any]:
        """Get summary statistics for a metric."""
        try:
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            
            if metric_name not in self.metric_aggregates:
                return {'error': f'Metric {metric_name} not found'}
            
            recent_aggregates = [
                agg for agg in self.metric_aggregates[metric_name]
                if agg['timestamp'] >= cutoff
            ]
            
            if not recent_aggregates:
                return {'error': f'No recent data for metric {metric_name}'}
            
            # Calculate summary statistics
            all_values = []
            for agg in recent_aggregates:
                all_values.extend([agg['aggregates']['avg']])
            
            summary = {
                'metric_name': metric_name,
                'time_range_hours': hours,
                'data_points': len(recent_aggregates),
                'latest_value': recent_aggregates[-1]['aggregates']['avg'],
                'min_value': min(all_values),
                'max_value': max(all_values),
                'avg_value': statistics.mean(all_values)
            }
            
            if len(all_values) > 1:
                summary['stddev'] = statistics.stdev(all_values)
            
            return summary
            
        except Exception as e:
            return {'error': f'Failed to get metric summary: {str(e)}'}
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health metrics."""
        try:
            uptime = time.time() - self.builtin_metrics['system_start_time']
            
            health = {
                'status': 'healthy',
                'uptime_seconds': uptime,
                'total_metrics_collected': self.builtin_metrics['total_metrics_collected'],
                'metrics_collection_errors': self.builtin_metrics['metrics_collection_errors'],
                'last_metric_timestamp': self.builtin_metrics['last_metric_timestamp'],
                'active_alert_thresholds': len(self.alert_thresholds),
                'metrics_buffer_size': len(self.metrics_buffer),
                'backends': {
                    'prometheus_enabled': self.enable_prometheus,
                    'cloudwatch_enabled': self.enable_cloudwatch
                }
            }
            
            # Check if system is healthy
            error_rate = (
                self.builtin_metrics['metrics_collection_errors'] / 
                max(self.builtin_metrics['total_metrics_collected'], 1)
            )
            
            if error_rate > 0.1:  # More than 10% error rate
                health['status'] = 'degraded'
            
            if self.builtin_metrics['last_metric_timestamp']:
                time_since_last_metric = time.time() - self.builtin_metrics['last_metric_timestamp']
                if time_since_last_metric > 300:  # No metrics for 5 minutes
                    health['status'] = 'unhealthy'
            
            return health
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def push_to_prometheus_gateway(self, job_name: str = "basemap_processing") -> bool:
        """Push metrics to Prometheus pushgateway."""
        if not self.enable_prometheus or not self.prometheus_gateway:
            return False
        
        try:
            push_to_gateway(
                self.prometheus_gateway,
                job=job_name,
                registry=self.prometheus_registry
            )
            
            self.logger.info(
                "Pushed metrics to Prometheus gateway",
                gateway=self.prometheus_gateway,
                job=job_name
            )
            return True
            
        except Exception as e:
            self.logger.error(
                "Failed to push metrics to Prometheus gateway",
                error=str(e)
            )
            return False
    
    def export_metrics(self, format: str = "json") -> Union[str, Dict[str, Any]]:
        """Export metrics in various formats."""
        try:
            if format.lower() == "json":
                # Export recent metrics as JSON
                cutoff = datetime.utcnow() - timedelta(hours=1)
                recent_metrics = [
                    {
                        'name': m.name,
                        'value': m.value,
                        'timestamp': m.timestamp.isoformat(),
                        'labels': m.labels,
                        'description': m.description
                    }
                    for m in self.metrics_buffer
                    if m.timestamp >= cutoff
                ]
                
                export_data = {
                    'export_timestamp': datetime.utcnow().isoformat(),
                    'metrics_count': len(recent_metrics),
                    'time_range': '1 hour',
                    'metrics': recent_metrics
                }
                
                return json.dumps(export_data, indent=2)
            
            elif format.lower() == "prometheus":
                # Export in Prometheus format
                if self.enable_prometheus:
                    from prometheus_client import generate_latest
                    return generate_latest(self.prometheus_registry).decode('utf-8')
                else:
                    return "Prometheus not enabled"
            
            else:
                return f"Unsupported export format: {format}"
                
        except Exception as e:
            return f"Export failed: {str(e)}"
    
    def cleanup(self) -> None:
        """Clean up resources."""
        try:
            # Flush any remaining CloudWatch metrics
            if self.enable_cloudwatch:
                self._flush_cloudwatch_batch()
            
            # Push final metrics to Prometheus gateway
            if self.enable_prometheus and self.prometheus_gateway:
                self.push_to_prometheus_gateway()
            
            self.logger.info("Metrics collector cleanup completed")
            
        except Exception as e:
            self.logger.error("Cleanup failed", error=str(e))


# Example usage and configuration
if __name__ == "__main__":
    # Initialize metrics collector
    metrics = MetricsCollector(
        enable_prometheus=True,
        enable_cloudwatch=True,
        prometheus_gateway="http://localhost:9091",
        cloudwatch_namespace="BasemapProcessing/Dev"
    )
    
    # Add alert thresholds
    metrics.add_alert_threshold(AlertThreshold(
        metric_name="data_processing_duration_seconds",
        warning_threshold=300.0,  # 5 minutes
        critical_threshold=600.0,  # 10 minutes
        comparison_operator="greater_than"
    ))
    
    metrics.add_alert_threshold(AlertThreshold(
        metric_name="data_quality_checks_total",
        warning_threshold=0.95,
        critical_threshold=0.90,
        comparison_operator="less_than"
    ))
    
    # Example metric collection
    metrics.increment_counter(
        "data_ingestion_records_total",
        value=1000,
        labels={"source_type": "osm", "status": "success"}
    )
    
    metrics.record_histogram(
        "data_processing_duration_seconds",
        value=45.2,
        labels={"operation_type": "tile_generation"}
    )
    
    metrics.set_gauge(
        "active_spark_executors",
        value=8,
        labels={"cluster_id": "emr-cluster-001"}
    )
    
    # Example function timing
    @metrics.time_function("example_function_duration", {"component": "test"})
    def example_function():
        time.sleep(0.1)  # Simulate work
        return "completed"
    
    result = example_function()
    
    # Get system health
    health = metrics.get_system_health()
    print(f"System health: {health['status']}")
    
    # Get metric summary
    summary = metrics.get_metric_summary("data_processing_duration_seconds")
    print(f"Processing duration summary: {summary}")
    
    print("Metrics collection system initialized and running!")
    print(f"Prometheus enabled: {metrics.enable_prometheus}")
    print(f"CloudWatch enabled: {metrics.enable_cloudwatch}")
    print(f"Alert thresholds configured: {len(metrics.alert_thresholds)}")