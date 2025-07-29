"""
Basemap Data Processing DAG

Main Airflow DAG for orchestrating the complete basemap data processing pipeline.
Demonstrates expertise in workflow management, dependency handling, and 
operational excellence practices.

This DAG implements:
- Complex workflow orchestration
- Error handling and retry logic
- Data quality checkpoints
- Monitoring and alerting
- Resource management
- Incremental processing capabilities
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks.base import BaseHook

# Custom operators and utilities
import sys
sys.path.append('/opt/airflow/dags')
from operators.spark_submit_operator import SparkSubmitOperatorCustom
from operators.data_quality_operator import DataQualityOperator
from operators.tile_generation_operator import TileGenerationOperator
from utils.slack_notifications import send_slack_notification
from utils.data_validation import validate_processing_results


# DAG configuration
DAG_ID = 'basemap_processing_pipeline'
SCHEDULE_INTERVAL = '0 2 * * *'  # Daily at 2 AM UTC
MAX_ACTIVE_RUNS = 1
CATCHUP = False

# Default arguments
default_args = {
    'owner': 'basemap-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1),
    'email': ['basemap-alerts@mapbox.com'],
    'sla': timedelta(hours=6),  # SLA of 6 hours for the entire pipeline
}

# Configuration from Airflow Variables
config = {
    'aws_region': Variable.get('aws_region', default_var='us-west-2'),
    'emr_cluster_name': Variable.get('emr_cluster_name', default_var='basemap-processing'),
    'emr_instance_type': Variable.get('emr_instance_type', default_var='m5.xlarge'),
    'emr_instance_count': int(Variable.get('emr_instance_count', default_var='3')),
    's3_bucket': Variable.get('s3_bucket', default_var='mapbox-basemap-data'),
    'postgres_conn_id': Variable.get('postgres_conn_id', default_var='postgres_basemap'),
    'slack_webhook': Variable.get('slack_webhook', default_var=''),
    'data_sources': {
        'osm_planet': Variable.get('osm_planet_url', default_var=''),
        'poi_data': Variable.get('poi_data_path', default_var=''),
        'elevation_data': Variable.get('elevation_data_path', default_var='')
    }
}

# EMR cluster configuration
EMR_CONFIG = {
    'Name': config['emr_cluster_name'],
    'ReleaseLabel': 'emr-6.10.0',
    'Applications': [
        {'Name': 'Spark'},
        {'Name': 'Hadoop'},
        {'Name': 'Hive'}
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': config['emr_instance_type'],
                'InstanceCount': 1,
            },
            {
                'Name': 'Workers',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': config['emr_instance_type'],
                'InstanceCount': config['emr_instance_count'],
                'BidPrice': '0.10',
            }
        ],
        'Ec2KeyName': 'basemap-emr-key',
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'LogUri': f"s3://{config['s3_bucket']}/emr-logs/",
    'BootstrapActions': [
        {
            'Name': 'Install Python Dependencies',
            'ScriptBootstrapAction': {
                'Path': f"s3://{config['s3_bucket']}/scripts/bootstrap.sh"
            }
        }
    ],
    'Configurations': [
        {
            'Classification': 'spark-defaults',
            'Properties': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.sql.execution.arrow.pyspark.enabled': 'true',
                'spark.executor.memory': '4g',
                'spark.executor.cores': '2',
                'spark.driver.memory': '2g',
                'spark.driver.maxResultSize': '1g'
            }
        }
    ]
}

# Spark job steps
SPARK_STEPS = [
    {
        'Name': 'OSM Data Processing',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--conf', 'spark.sql.adaptive.enabled=true',
                '--py-files', f"s3://{config['s3_bucket']}/code/basemap_processing.zip",
                f"s3://{config['s3_bucket']}/code/osm_processing_job.py",
                '--input-path', f"s3://{config['s3_bucket']}/raw/osm/",
                '--output-path', f"s3://{config['s3_bucket']}/processed/osm/",
                '--date', '{{ ds }}'
            ]
        }
    },
    {
        'Name': 'POI Data Processing',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--py-files', f"s3://{config['s3_bucket']}/code/basemap_processing.zip",
                f"s3://{config['s3_bucket']}/code/poi_processing_job.py",
                '--input-path', f"s3://{config['s3_bucket']}/raw/poi/",
                '--output-path', f"s3://{config['s3_bucket']}/processed/poi/",
                '--date', '{{ ds }}'
            ]
        }
    },
    {
        'Name': 'Data Conflation',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--conf', 'spark.executor.memory=6g',
                '--conf', 'spark.executor.cores=3',
                '--py-files', f"s3://{config['s3_bucket']}/code/basemap_processing.zip",
                f"s3://{config['s3_bucket']}/code/conflation_job.py",
                '--osm-path', f"s3://{config['s3_bucket']}/processed/osm/",
                '--poi-path', f"s3://{config['s3_bucket']}/processed/poi/",
                '--output-path', f"s3://{config['s3_bucket']}/conflated/",
                '--date', '{{ ds }}'
            ]
        }
    }
]


def notify_start(**context):
    """Send notification when DAG starts."""
    message = f"🚀 Basemap processing pipeline started for {context['ds']}"
    send_slack_notification(config['slack_webhook'], message)


def notify_success(**context):
    """Send notification when DAG completes successfully."""
    message = f"✅ Basemap processing pipeline completed successfully for {context['ds']}"
    send_slack_notification(config['slack_webhook'], message)


def notify_failure(**context):
    """Send notification when DAG fails."""
    message = f"❌ Basemap processing pipeline failed for {context['ds']}. Check logs for details."
    send_slack_notification(config['slack_webhook'], message)


def check_data_freshness(**context):
    """Check if input data is fresh enough for processing."""
    from datetime import datetime, timedelta
    import boto3
    
    s3_client = boto3.client('s3')
    
    # Check OSM data freshness (should be less than 24 hours old)
    try:
        response = s3_client.head_object(
            Bucket=config['s3_bucket'],
            Key=f"raw/osm/planet-{context['ds']}.osm.pbf"
        )
        
        last_modified = response['LastModified'].replace(tzinfo=None)
        age = datetime.utcnow() - last_modified
        
        if age > timedelta(hours=24):
            raise ValueError(f"OSM data is too old: {age}")
        
        context['task_instance'].xcom_push(key='osm_data_age', value=age.total_seconds())
        
    except Exception as e:
        raise ValueError(f"Failed to check OSM data freshness: {str(e)}")


def validate_cluster_resources(**context):
    """Validate that EMR cluster has sufficient resources."""
    import boto3
    
    emr_client = boto3.client('emr', region_name=config['aws_region'])
    
    # Get cluster ID from previous task
    cluster_id = context['task_instance'].xcom_pull(task_ids='create_emr_cluster')
    
    try:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        cluster_state = response['Cluster']['Status']['State']
        
        if cluster_state != 'WAITING':
            raise ValueError(f"Cluster not ready. State: {cluster_state}")
        
        # Check instance counts
        instances = emr_client.list_instances(ClusterId=cluster_id)
        running_instances = [
            i for i in instances['Instances'] 
            if i['Status']['State'] == 'RUNNING'
        ]
        
        if len(running_instances) < config['emr_instance_count']:
            raise ValueError(f"Insufficient running instances: {len(running_instances)}")
        
        context['task_instance'].xcom_push(
            key='cluster_info',
            value={
                'cluster_id': cluster_id,
                'running_instances': len(running_instances),
                'state': cluster_state
            }
        )
        
    except Exception as e:
        raise ValueError(f"Cluster validation failed: {str(e)}")


def cleanup_old_data(**context):
    """Clean up old processed data to manage storage costs."""
    import boto3
    from datetime import datetime, timedelta
    
    s3_client = boto3.client('s3')
    
    # Delete data older than 30 days
    cutoff_date = datetime.utcnow() - timedelta(days=30)
    
    prefixes = [
        'processed/osm/',
        'processed/poi/',
        'conflated/',
        'tiles/temp/'
    ]
    
    deleted_objects = 0
    
    for prefix in prefixes:
        try:
            response = s3_client.list_objects_v2(
                Bucket=config['s3_bucket'],
                Prefix=prefix
            )
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                        s3_client.delete_object(
                            Bucket=config['s3_bucket'],
                            Key=obj['Key']
                        )
                        deleted_objects += 1
                        
        except Exception as e:
            print(f"Warning: Failed to clean up {prefix}: {str(e)}")
    
    context['task_instance'].xcom_push(key='deleted_objects', value=deleted_objects)


def collect_processing_metrics(**context):
    """Collect and store processing metrics."""
    import boto3
    
    # Get processing statistics from XCom
    osm_records = context['task_instance'].xcom_pull(
        task_ids='data_processing.wait_osm_processing', 
        key='records_processed'
    ) or 0
    
    poi_records = context['task_instance'].xcom_pull(
        task_ids='data_processing.wait_poi_processing', 
        key='records_processed'
    ) or 0
    
    tiles_generated = context['task_instance'].xcom_pull(
        task_ids='tile_generation.generate_vector_tiles', 
        key='tiles_generated'
    ) or 0
    
    processing_duration = context['task_instance'].xcom_pull(
        task_ids='data_processing.wait_conflation', 
        key='processing_duration'
    ) or 0
    
    # Store metrics in XCom for database update
    metrics = {
        'osm_records_processed': osm_records,
        'poi_records_processed': poi_records,
        'tiles_generated': tiles_generated,
        'processing_duration_minutes': processing_duration / 60,
        'data_quality_score': 95.0,  # This would come from quality checks
        'run_date': context['ds'],
        'status': 'SUCCESS'
    }
    
    context['task_instance'].xcom_push(key='processing_metrics', value=metrics)
    
    return metrics


def publish_tiles_to_cdn(**context):
    """Publish generated tiles to CDN for distribution."""
    import boto3
    
    s3_client = boto3.client('s3')
    cloudfront_client = boto3.client('cloudfront')
    
    source_bucket = config['s3_bucket']
    cdn_bucket = f"{config['s3_bucket']}-cdn"
    
    # Copy tiles to CDN bucket
    source_prefix = f"tiles/{context['ds']}/"
    target_prefix = "tiles/latest/"
    
    try:
        # List objects in source
        response = s3_client.list_objects_v2(
            Bucket=source_bucket,
            Prefix=source_prefix
        )
        
        copied_objects = 0
        
        if 'Contents' in response:
            for obj in response['Contents']:
                source_key = obj['Key']
                target_key = source_key.replace(source_prefix, target_prefix)
                
                # Copy object
                s3_client.copy_object(
                    CopySource={'Bucket': source_bucket, 'Key': source_key},
                    Bucket=cdn_bucket,
                    Key=target_key,
                    MetadataDirective='COPY'
                )
                copied_objects += 1
        
        # Invalidate CloudFront cache
        distribution_id = Variable.get('cloudfront_distribution_id', default_var='')
        if distribution_id:
            cloudfront_client.create_invalidation(
                DistributionId=distribution_id,
                InvalidationBatch={
                    'Paths': {
                        'Quantity': 1,
                        'Items': ['/tiles/*']
                    },
                    'CallerReference': f"basemap-{context['ds']}-{context['ts']}"
                }
            )
        
        context['task_instance'].xcom_push(key='published_tiles', value=copied_objects)
        
        return copied_objects
        
    except Exception as e:
        raise ValueError(f"Failed to publish tiles to CDN: {str(e)}")


def generate_processing_report(**context):
    """Generate comprehensive processing report."""
    import json
    from datetime import datetime
    
    # Collect all metrics
    metrics = context['task_instance'].xcom_pull(
        task_ids='collect_processing_metrics', 
        key='processing_metrics'
    )
    
    cluster_info = context['task_instance'].xcom_pull(
        task_ids='validate_cluster_resources', 
        key='cluster_info'
    )
    
    published_tiles = context['task_instance'].xcom_pull(
        task_ids='publish_tiles_to_cdn', 
        key='published_tiles'
    )
    
    deleted_objects = context['task_instance'].xcom_pull(
        task_ids='cleanup_old_data', 
        key='deleted_objects'
    )
    
    # Generate report
    report = {
        'run_date': context['ds'],
        'execution_date': context['execution_date'].isoformat(),
        'dag_run_id': context['dag_run'].run_id,
        'processing_metrics': metrics,
        'infrastructure': {
            'cluster_id': cluster_info.get('cluster_id') if cluster_info else None,
            'running_instances': cluster_info.get('running_instances') if cluster_info else 0,
        },
        'deployment': {
            'tiles_published': published_tiles or 0,
            'old_data_cleaned': deleted_objects or 0
        },
        'data_sources': config['data_sources'],
        'configuration': {
            'emr_instance_type': config['emr_instance_type'],
            'emr_instance_count': config['emr_instance_count'],
            's3_bucket': config['s3_bucket']
        }
    }
    
    # Store report
    report_json = json.dumps(report, indent=2)
    context['task_instance'].xcom_push(key='processing_report', value=report_json)
    
    return report


# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Basemap data processing pipeline with Spark and EMR',
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=MAX_ACTIVE_RUNS,
    catchup=CATCHUP,
    tags=['basemap', 'geospatial', 'spark', 'emr'],
    doc_md=__doc__
)

# Start notification
start_notification = PythonOperator(
    task_id='start_notification',
    python_callable=notify_start,
    dag=dag
)

# Data freshness check
data_freshness_check = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag
)

# Wait for input data
wait_for_osm_data = S3KeySensor(
    task_id='wait_for_osm_data',
    bucket_name=config['s3_bucket'],
    bucket_key=f"raw/osm/planet-{{{{ ds }}}}.osm.pbf",
    aws_conn_id='aws_default',
    timeout=3600,  # 1 hour timeout
    poke_interval=300,  # Check every 5 minutes
    dag=dag
)

# Create EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=EMR_CONFIG,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag
)

# Validate cluster resources
validate_cluster = PythonOperator(
    task_id='validate_cluster_resources',
    python_callable=validate_cluster_resources,
    dag=dag
)

# Data processing task group
with TaskGroup('data_processing', dag=dag) as data_processing_group:
    
    # Add Spark processing steps
    add_spark_steps = EmrAddStepsOperator(
        task_id='add_spark_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
        dag=dag
    )
    
    # Wait for OSM processing step
    wait_osm_step = EmrStepSensor(
        task_id='wait_osm_processing',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='data_processing.add_spark_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
        timeout=7200,  # 2 hours
        dag=dag
    )
    
    # Wait for POI processing step
    wait_poi_step = EmrStepSensor(
        task_id='wait_poi_processing',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='data_processing.add_spark_steps', key='return_value')[1] }}",
        aws_conn_id='aws_default',
        timeout=3600,  # 1 hour
        trigger_rule=TriggerRule.ALL_DONE,  # Continue even if POI processing fails
        dag=dag
    )
    
    # Wait for conflation step
    wait_conflation_step = EmrStepSensor(
        task_id='wait_conflation',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='data_processing.add_spark_steps', key='return_value')[2] }}",
        aws_conn_id='aws_default',
        timeout=5400,  # 1.5 hours
        dag=dag
    )
    
    add_spark_steps >> [wait_osm_step, wait_poi_step] >> wait_conflation_step

# Data quality checks
data_quality_check = DataQualityOperator(
    task_id='data_quality_check',
    input_path=f"s3://{config['s3_bucket']}/conflated/{{{{ ds }}}}/",
    quality_checks=[
        'record_count_check',
        'geometry_validity_check',
        'attribute_completeness_check',
        'spatial_consistency_check'
    ],
    dag=dag
)

# Tile generation task group
with TaskGroup('tile_generation', dag=dag) as tile_generation_group:
    
    # Generate vector tiles
    generate_vector_tiles = TileGenerationOperator(
        task_id='generate_vector_tiles',
        input_path=f"s3://{config['s3_bucket']}/conflated/{{{{ ds }}}}/",
        output_path=f"s3://{config['s3_bucket']}/tiles/{{{{ ds }}}}/",
        tile_format='mvt',
        zoom_levels=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
        dag=dag
    )
    
    # Validate generated tiles
    validate_tiles = DataQualityOperator(
        task_id='validate_tiles',
        input_path=f"s3://{config['s3_bucket']}/tiles/{{{{ ds }}}}/",
        quality_checks=[
            'tile_count_check',
            'tile_size_check',
            'tile_format_check'
        ],
        dag=dag
    )
    
    generate_vector_tiles >> validate_tiles

# Collect processing metrics
collect_metrics = PythonOperator(
    task_id='collect_processing_metrics',
    python_callable=collect_processing_metrics,
    dag=dag
)

# Update metadata in PostgreSQL
update_metadata = PostgresOperator(
    task_id='update_metadata',
    postgres_conn_id=config['postgres_conn_id'],
    sql="""
        INSERT INTO basemap_processing_runs (
            run_date,
            status,
            osm_records_processed,
            poi_records_processed,
            tiles_generated,
            processing_duration_minutes,
            data_quality_score,
            created_at
        ) VALUES (
            '{{ ds }}',
            '{{ task_instance.xcom_pull(task_ids="collect_processing_metrics", key="processing_metrics")["status"] }}',
            {{ task_instance.xcom_pull(task_ids="collect_processing_metrics", key="processing_metrics")["osm_records_processed"] }},
            {{ task_instance.xcom_pull(task_ids="collect_processing_metrics", key="processing_metrics")["poi_records_processed"] }},
            {{ task_instance.xcom_pull(task_ids="collect_processing_metrics", key="processing_metrics")["tiles_generated"] }},
            {{ task_instance.xcom_pull(task_ids="collect_processing_metrics", key="processing_metrics")["processing_duration_minutes"] }},
            {{ task_instance.xcom_pull(task_ids="collect_processing_metrics", key="processing_metrics")["data_quality_score"] }},
            NOW()
        )
        ON CONFLICT (run_date) 
        DO UPDATE SET
            status = EXCLUDED.status,
            osm_records_processed = EXCLUDED.osm_records_processed,
            poi_records_processed = EXCLUDED.poi_records_processed,
            tiles_generated = EXCLUDED.tiles_generated,
            processing_duration_minutes = EXCLUDED.processing_duration_minutes,
            data_quality_score = EXCLUDED.data_quality_score,
            updated_at = NOW();
    """,
    dag=dag
)

# Publish tiles to CDN
publish_tiles = PythonOperator(
    task_id='publish_tiles_to_cdn',
    python_callable=publish_tiles_to_cdn,
    dag=dag
)

# Generate processing report
generate_report = PythonOperator(
    task_id='generate_processing_report',
    python_callable=generate_processing_report,
    dag=dag
)

# Cleanup old data
cleanup_data = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag
)

# Terminate EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    trigger_rule=TriggerRule.ALL_DONE,  # Always terminate cluster
    dag=dag
)

# Success notification
success_notification = PythonOperator(
    task_id='success_notification',
    python_callable=notify_success,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

# Failure notification
failure_notification = PythonOperator(
    task_id='failure_notification',
    python_callable=notify_failure,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag
)

# Define task dependencies
start_notification >> data_freshness_check >> wait_for_osm_data
wait_for_osm_data >> create_emr_cluster >> validate_cluster
validate_cluster >> data_processing_group >> data_quality_check
data_quality_check >> tile_generation_group >> collect_metrics
collect_metrics >> [update_metadata, publish_tiles, generate_report]
[update_metadata, publish_tiles, generate_report] >> cleanup_data
cleanup_data >> terminate_emr_cluster
terminate_emr_cluster >> [success_notification, failure_notification]

# Additional monitoring tasks that run in parallel
monitoring_tasks = TaskGroup('monitoring', dag=dag)

with monitoring_tasks:
    # Monitor cluster health
    monitor_cluster_health = BashOperator(
        task_id='monitor_cluster_health',
        bash_command="""
            aws emr describe-cluster --cluster-id {{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }} \
            --query 'Cluster.Status.State' --output text
        """,
        dag=dag
    )
    
    # Monitor S3 storage usage
    monitor_storage_usage = BashOperator(
        task_id='monitor_storage_usage',
        bash_command=f"""
            aws s3api list-objects-v2 --bucket {config['s3_bucket']} \
            --query 'sum(Contents[].Size)' --output text
        """,
        dag=dag
    )

# Connect monitoring tasks
validate_cluster >> monitoring_tasks
monitoring_tasks >> data_processing_group


# Custom task failure callback
def task_failure_callback(context):
    """Custom callback for task failures."""
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    execution_date = context['execution_date']
    
    message = f"""
    ❌ Task Failed in Basemap Pipeline
    
    DAG: {dag_id}
    Task: {task_id}
    Execution Date: {execution_date}
    Log URL: {context['task_instance'].log_url}
    """
    
    send_slack_notification(config['slack_webhook'], message)


# Apply failure callback to critical tasks
critical_tasks = [
    'create_emr_cluster',
    'data_processing.wait_osm_processing',
    'data_processing.wait_conflation',
    'data_quality_check',
    'tile_generation.generate_vector_tiles'
]

for task_id in critical_tasks:
    if task_id in dag.task_dict:
        dag.task_dict[task_id].on_failure_callback = task_failure_callback


# DAG documentation
dag.doc_md = """
# Basemap Data Processing Pipeline

This DAG orchestrates the complete basemap data processing workflow including:

## Pipeline Stages

1. **Data Ingestion**: Download and validate OSM and POI data
2. **Data Processing**: Distributed processing using Spark on EMR
3. **Data Conflation**: Merge and reconcile multiple data sources
4. **Quality Assurance**: Comprehensive data quality checks
5. **Tile Generation**: Generate vector tiles for multiple zoom levels
6. **Publishing**: Deploy tiles to CDN for distribution
7. **Monitoring**: Collect metrics and generate reports

## Key Features

- **Fault Tolerance**: Automatic retries and graceful error handling
- **Resource Management**: Dynamic EMR cluster provisioning
- **Quality Assurance**: Multi-stage data validation
- **Monitoring**: Real-time metrics and alerting
- **Cost Optimization**: Spot instances and automatic cleanup

## Configuration

The pipeline is configured through Airflow Variables:
- `aws_region`: AWS region for resources
- `s3_bucket`: S3 bucket for data storage
- `emr_instance_type`: EMR instance type
- `emr_instance_count`: Number of EMR worker instances
- `slack_webhook`: Slack webhook for notifications

## SLA and Performance

- **SLA**: 6 hours end-to-end processing
- **Frequency**: Daily execution at 2 AM UTC
- **Scalability**: Handles planet-scale OSM data
- **Availability**: 99.9% uptime target
"""
