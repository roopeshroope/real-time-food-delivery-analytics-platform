-- ============================================================================
-- Bronze layer DDL -- Unity Catalog.
-- Run once per environment (dev/staging/prod each get their own catalog,
-- e.g. food_delivery_dev / food_delivery_staging / food_delivery).
-- Column lists mirror bronze/schemas.py exactly; if you change one, change
-- both (and consider whether it needs a schema_version bump instead).
-- ============================================================================

CREATE CATALOG IF NOT EXISTS ${catalog};
CREATE SCHEMA IF NOT EXISTS ${catalog}.${bronze_schema};

-- ----------------------------------------------------------------------------
-- 1. order-lifecycle-events -> order_lifecycle_events
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze_schema}.order_lifecycle_events (
    event_time              STRING      COMMENT 'Business event time, ISO-8601 UTC, as emitted by the simulator -- NOT parsed to TIMESTAMP in Bronze',
    schema_version          BIGINT,
    event_id                STRING      COMMENT 'Dedup key for Silver -- Bronze may contain duplicates by design',
    order_id                STRING,
    customer_id             STRING,
    restaurant_id           STRING,
    zone_id                 STRING,
    state                   STRING      COMMENT 'PLACED|ACCEPTED|REJECTED|PREPARING|READY|ASSIGNED|PICKED_UP|DELIVERED|CANCELLED',
    previous_state          STRING,
    item_count              BIGINT,
    order_value_inr         DOUBLE,
    promised_eta_ts         STRING,
    predicted_ready_ts      STRING,
    dp_id                   STRING,
    cancelled_by            STRING,
    cancellation_reason     STRING,
    rejection_reason        STRING,
    raw_value               STRING      COMMENT 'Untouched original JSON payload, verbatim',
    kafka_topic             STRING,
    kafka_partition         INT,
    kafka_offset            BIGINT,
    kafka_timestamp         TIMESTAMP   COMMENT 'Kafka broker record timestamp (append time), NOT business event_time',
    ingestion_timestamp     TIMESTAMP   COMMENT 'Wall-clock time this Bronze row was written',
    ingest_date             DATE        COMMENT 'Partition column, derived from ingestion_timestamp'
)
USING DELTA
LOCATION '${storage_root}/order_lifecycle_events'
PARTITIONED BY (ingest_date)
COMMENT 'Raw order-lifecycle-events from Kafka, 1:1 with simulator/schemas.py:OrderLifecycleEvent';

-- ----------------------------------------------------------------------------
-- 2. dp-status-events -> dp_status_events
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze_schema}.dp_status_events (
    event_time              STRING,
    schema_version          BIGINT,
    event_id                STRING,
    dp_id                   STRING,
    zone_id                 STRING,
    status                  STRING      COMMENT 'OFFLINE|IDLE|ASSIGNED|EN_ROUTE_TO_RESTAURANT|WAITING_AT_RESTAURANT|PICKED_UP|EN_ROUTE_TO_CUSTOMER|DELIVERED',
    previous_status         STRING,
    order_id                STRING,
    lat                     DOUBLE,
    lon                     DOUBLE,
    raw_value               STRING,
    kafka_topic             STRING,
    kafka_partition         INT,
    kafka_offset            BIGINT,
    kafka_timestamp         TIMESTAMP,
    ingestion_timestamp     TIMESTAMP,
    ingest_date             DATE
)
USING DELTA
LOCATION '${storage_root}/dp_status_events'
PARTITIONED BY (ingest_date)
COMMENT 'Raw dp-status-events from Kafka, 1:1 with simulator/schemas.py:DPStatusEvent';

-- ----------------------------------------------------------------------------
-- 3. gps-pings -> gps_pings  (highest volume -- see README partitioning note)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze_schema}.gps_pings (
    event_time              STRING,
    schema_version          BIGINT,
    event_id                STRING,
    dp_id                   STRING,
    order_id                STRING,
    lat                     DOUBLE,
    lon                     DOUBLE,
    speed_kmph              DOUBLE,
    heading_degrees         DOUBLE,
    accuracy_meters         DOUBLE,
    trip_leg                STRING      COMMENT 'TO_RESTAURANT|TO_CUSTOMER|IDLE',
    debug_anomaly_type      STRING      COMMENT 'Simulator-only ground-truth label (teleport|stationary_drift) -- never sent by a real GPS SDK',
    raw_value               STRING,
    kafka_topic             STRING,
    kafka_partition         INT,
    kafka_offset            BIGINT,
    kafka_timestamp         TIMESTAMP,
    ingestion_timestamp     TIMESTAMP,
    ingest_date             DATE
)
USING DELTA
LOCATION '${storage_root}/gps_pings'
PARTITIONED BY (ingest_date)
COMMENT 'Raw gps-pings from Kafka, 1:1 with simulator/schemas.py:GPSPingEvent. '
        'Highest-volume Bronze table -- see README for the Liquid Clustering recommendation on dp_id.';

-- ----------------------------------------------------------------------------
-- 4. restaurant-status-events -> restaurant_status_events
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze_schema}.restaurant_status_events (
    event_time                  STRING,
    schema_version              BIGINT,
    event_id                    STRING,
    restaurant_id                STRING,
    zone_id                      STRING,
    status                       STRING  COMMENT 'ONLINE|OFFLINE|BUSY|NORMAL',
    current_backlog              BIGINT,
    capacity_orders               BIGINT,
    avg_prep_minutes_observed    DOUBLE,
    reason                       STRING,
    raw_value                    STRING,
    kafka_topic                  STRING,
    kafka_partition               INT,
    kafka_offset                  BIGINT,
    kafka_timestamp                TIMESTAMP,
    ingestion_timestamp            TIMESTAMP,
    ingest_date                    DATE
)
USING DELTA
LOCATION '${storage_root}/restaurant_status_events'
PARTITIONED BY (ingest_date)
COMMENT 'Raw restaurant-status-events from Kafka, 1:1 with simulator/schemas.py:RestaurantStatusEvent';

-- ----------------------------------------------------------------------------
-- 5. payment-events -> payment_events
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze_schema}.payment_events (
    event_time              STRING,
    schema_version          BIGINT,
    event_id                STRING,
    payment_id              STRING,
    order_id                STRING,
    customer_id             STRING,
    attempt_number           BIGINT,
    retry_of_payment_id      STRING,
    amount_inr               DOUBLE,
    gateway                  STRING,
    payment_method            STRING,
    status                    STRING    COMMENT 'INITIATED|SUCCESS|FAILED',
    failure_code              STRING,
    is_gateway_outage          BOOLEAN,
    raw_value                 STRING,
    kafka_topic                STRING,
    kafka_partition             INT,
    kafka_offset                 BIGINT,
    kafka_timestamp               TIMESTAMP,
    ingestion_timestamp            TIMESTAMP,
    ingest_date                    DATE
)
USING DELTA
LOCATION '${storage_root}/payment_events'
PARTITIONED BY (ingest_date)
COMMENT 'Raw payment-events from Kafka, 1:1 with simulator/schemas.py:PaymentEvent. Contains PII-adjacent customer_id -- see README governance section.';

-- ----------------------------------------------------------------------------
-- 6. dispatch-decision-events -> dispatch_decision_events
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze_schema}.dispatch_decision_events (
    event_time                    STRING,
    schema_version                BIGINT,
    event_id                      STRING,
    dispatch_id                   STRING,
    order_id                      STRING,
    zone_id                       STRING,
    assigned_dp_id                STRING,
    candidates_considered          BIGINT,
    assignment_score                DOUBLE,
    is_batched                       BOOLEAN,
    batch_id                          STRING,
    batched_with_order_ids              ARRAY<STRING>,
    planned_pickup_eta_seconds            DOUBLE,
    planned_route_distance_km              DOUBLE,
    decision                                STRING   COMMENT 'ASSIGNED|NO_DP_AVAILABLE|REASSIGNED',
    raw_value                                STRING,
    kafka_topic                               STRING,
    kafka_partition                            INT,
    kafka_offset                                BIGINT,
    kafka_timestamp                              TIMESTAMP,
    ingestion_timestamp                           TIMESTAMP,
    ingest_date                                    DATE
)
USING DELTA
LOCATION '${storage_root}/dispatch_decision_events'
PARTITIONED BY (ingest_date)
COMMENT 'Raw dispatch-decision-events from Kafka, 1:1 with simulator/schemas.py:DispatchDecisionEvent';

-- ----------------------------------------------------------------------------
-- Shared quarantine table -- every topic's unparsable / structurally
-- incomplete records land here, tagged by source_topic_key, instead of
-- being dropped.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze_schema}.malformed_events (
    source_topic_key         STRING      COMMENT 'e.g. gps_pings -- the bronze_config.yaml topic key, not the physical Kafka topic name',
    kafka_topic               STRING,
    kafka_partition            INT,
    kafka_offset                 BIGINT,
    kafka_timestamp                TIMESTAMP,
    ingestion_timestamp             TIMESTAMP,
    ingest_date                       DATE,
    raw_key                            STRING,
    raw_value                          STRING     COMMENT 'The exact bytes that failed to parse, verbatim, for replay/debugging',
    error_reason                        STRING    COMMENT 'unparsable_json | missing_required_fields'
)
USING DELTA
LOCATION '${storage_root}/malformed_events'
PARTITIONED BY (ingest_date)
COMMENT 'Cross-topic quarantine for records that failed Bronze parsing -- see README "Malformed records".';

-- ----------------------------------------------------------------------------
-- Recommended (not applied automatically -- requires DBR 13.3+):
-- swap PARTITIONED BY (ingest_date) for Liquid Clustering on each table's
-- natural join key, which matters once Silver starts doing point/range
-- lookups against these tables:
--
--   ALTER TABLE ${catalog}.${bronze_schema}.gps_pings
--     CLUSTER BY (dp_id, ingest_date);
--   ALTER TABLE ${catalog}.${bronze_schema}.order_lifecycle_events
--     CLUSTER BY (order_id, ingest_date);
--
-- Left as a recommendation, not applied here, since it depends on your
-- workspace's DBR version -- see README "Partitioning strategy".
-- ----------------------------------------------------------------------------
