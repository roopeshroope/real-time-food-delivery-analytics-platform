"""
PySpark schemas for the 6 event payloads.

These are a DELIBERATE 1:1 transcription of the dataclasses in
`simulator/schemas.py` -- same field names, same optionality, same types.
If you add/rename/retype a field in the simulator's dataclasses, mirror the
change here (and bump the dataclass's `schema_version` so Bronze can tell
old and new-shape events apart in the same table -- see README "Schema
evolution").

Do NOT add fields here that the simulator doesn't emit "for future use" --
Bronze's job is to reflect the actual event contract, not to anticipate one.
"""
from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Common envelope fields present on every event (BaseEvent in schemas.py)
_BASE_FIELDS = [
    StructField("event_time", StringType(), nullable=False),
    StructField("schema_version", LongType(), nullable=False),
    StructField("event_id", StringType(), nullable=False),
]

ORDER_LIFECYCLE_SCHEMA = StructType(
    _BASE_FIELDS
    + [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("restaurant_id", StringType(), nullable=False),
        StructField("zone_id", StringType(), nullable=False),
        StructField("state", StringType(), nullable=False),
        StructField("previous_state", StringType(), nullable=True),
        StructField("item_count", LongType(), nullable=True),
        StructField("order_value_inr", DoubleType(), nullable=True),
        StructField("promised_eta_ts", StringType(), nullable=True),
        StructField("predicted_ready_ts", StringType(), nullable=True),
        StructField("dp_id", StringType(), nullable=True),
        StructField("cancelled_by", StringType(), nullable=True),
        StructField("cancellation_reason", StringType(), nullable=True),
        StructField("rejection_reason", StringType(), nullable=True),
    ]
)

DP_STATUS_SCHEMA = StructType(
    _BASE_FIELDS
    + [
        StructField("dp_id", StringType(), nullable=False),
        StructField("zone_id", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("previous_status", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("lat", DoubleType(), nullable=True),
        StructField("lon", DoubleType(), nullable=True),
    ]
)

GPS_PINGS_SCHEMA = StructType(
    _BASE_FIELDS
    + [
        StructField("dp_id", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=True),
        StructField("lat", DoubleType(), nullable=False),
        StructField("lon", DoubleType(), nullable=False),
        StructField("speed_kmph", DoubleType(), nullable=True),
        StructField("heading_degrees", DoubleType(), nullable=True),
        StructField("accuracy_meters", DoubleType(), nullable=True),
        StructField("trip_leg", StringType(), nullable=True),
        StructField("debug_anomaly_type", StringType(), nullable=True),
    ]
)

RESTAURANT_STATUS_SCHEMA = StructType(
    _BASE_FIELDS
    + [
        StructField("restaurant_id", StringType(), nullable=False),
        StructField("zone_id", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("current_backlog", LongType(), nullable=True),
        StructField("capacity_orders", LongType(), nullable=True),
        StructField("avg_prep_minutes_observed", DoubleType(), nullable=True),
        StructField("reason", StringType(), nullable=True),
    ]
)

PAYMENT_SCHEMA = StructType(
    _BASE_FIELDS
    + [
        StructField("payment_id", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("attempt_number", LongType(), nullable=True),
        StructField("retry_of_payment_id", StringType(), nullable=True),
        StructField("amount_inr", DoubleType(), nullable=True),
        StructField("gateway", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("failure_code", StringType(), nullable=True),
        StructField("is_gateway_outage", BooleanType(), nullable=True),
    ]
)

DISPATCH_DECISION_SCHEMA = StructType(
    _BASE_FIELDS
    + [
        StructField("dispatch_id", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("zone_id", StringType(), nullable=False),
        StructField("assigned_dp_id", StringType(), nullable=True),
        StructField("candidates_considered", LongType(), nullable=True),
        StructField("assignment_score", DoubleType(), nullable=True),
        StructField("is_batched", BooleanType(), nullable=True),
        StructField("batch_id", StringType(), nullable=True),
        StructField("batched_with_order_ids", ArrayType(StringType()), nullable=True),
        StructField("planned_pickup_eta_seconds", DoubleType(), nullable=True),
        StructField("planned_route_distance_km", DoubleType(), nullable=True),
        StructField("decision", StringType(), nullable=False),
    ]
)

SCHEMAS: dict[str, StructType] = {
    "order_lifecycle": ORDER_LIFECYCLE_SCHEMA,
    "dp_status": DP_STATUS_SCHEMA,
    "gps_pings": GPS_PINGS_SCHEMA,
    "restaurant_status": RESTAURANT_STATUS_SCHEMA,
    "payment": PAYMENT_SCHEMA,
    "dispatch_decision": DISPATCH_DECISION_SCHEMA,
}
