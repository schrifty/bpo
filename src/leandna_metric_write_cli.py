"""Backward-compatible re-exports for metric write CLIs."""

from src.leandna_metrics_cli import (  # noqa: F401
    add_metric_write_arguments,
    metric_write_args_from_namespace,
    print_result_env,
)
from src.leandna_metrics_write import (  # noqa: F401
    MetricWriteArgs,
    insert_metric_datapoint as _insert_via_data_api,
    delete_metric_datapoint_for_date as _delete_via_data_api,
    run_insert,
    run_upsert,
)

__all__ = [
    "MetricWriteArgs",
    "add_metric_write_arguments",
    "metric_write_args_from_namespace",
    "print_result_env",
    "run_insert",
    "run_upsert",
]
