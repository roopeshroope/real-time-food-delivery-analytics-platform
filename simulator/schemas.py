"""
Event schemas for the 6 source topics. Plain dataclasses (not Avro/pydantic)
to keep the simulator dependency-light -- `to_dict()` gives you the exact
JSON payload published to Kafka. If you later want Avro/Protobuf, these
dataclasses are the natural source of truth to generate schemas from.

Every event carries:
  - event_id        : globally unique id for this specific event
  - event_time       : ISO-8601 UTC timestamp of when the event *actually*
                        occurred in simulated time (NOT publish time -- see
                        chaos.py for late/out-of-order publish injection)
  - schema_version    : bump if you change a payload shape
"""
from __future__ import annotations

import dataclasses
import uuid
from datetime import datetime, timezone
from typing import Optional


def new_event_id() -> str:
    return uuid.uuid4().hex


def iso_now(dt: Optional[datetime] = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclasses.dataclass
class BaseEvent:
    event_time: str
    schema_version: int = 1
    event_id: str = dataclasses.field(default_factory=new_event_id)

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


# ---------------------------------------------------------------------------
# 1. order-lifecycle-events
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class OrderLifecycleEvent(BaseEvent):
    order_id: str = ""
    customer_id: str = ""
    restaurant_id: str = ""
    zone_id: str = ""
    state: str = ""                       # PLACED, ACCEPTED, REJECTED, PREPARING,
                                           # READY, ASSIGNED, PICKED_UP, DELIVERED,
                                           # CANCELLED
    previous_state: Optional[str] = None
    item_count: int = 0
    order_value_inr: float = 0.0
    promised_eta_ts: Optional[str] = None  # set at PLACED/ACCEPTED, carried forward
    predicted_ready_ts: Optional[str] = None
    dp_id: Optional[str] = None
    cancelled_by: Optional[str] = None     # CUSTOMER | RESTAURANT | SYSTEM
    cancellation_reason: Optional[str] = None
    rejection_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# 2. dp-status-events
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class DPStatusEvent(BaseEvent):
    dp_id: str = ""
    zone_id: str = ""
    status: str = ""                       # OFFLINE, IDLE, ASSIGNED,
                                            # EN_ROUTE_TO_RESTAURANT,
                                            # WAITING_AT_RESTAURANT, PICKED_UP,
                                            # EN_ROUTE_TO_CUSTOMER, DELIVERED
    previous_status: Optional[str] = None
    order_id: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None


# ---------------------------------------------------------------------------
# 3. gps-pings
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class GPSPingEvent(BaseEvent):
    dp_id: str = ""
    order_id: Optional[str] = None
    lat: float = 0.0
    lon: float = 0.0
    speed_kmph: float = 0.0
    heading_degrees: float = 0.0
    accuracy_meters: float = 8.0
    trip_leg: Optional[str] = None         # TO_RESTAURANT | TO_CUSTOMER | IDLE
    debug_anomaly_type: Optional[str] = None  # ground-truth label for injected
                                               # anomalies (teleport/stationary);
                                               # useful to validate a downstream
                                               # anomaly detector, NOT a field a
                                               # real GPS SDK would ever send


# ---------------------------------------------------------------------------
# 4. restaurant-status-events
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class RestaurantStatusEvent(BaseEvent):
    restaurant_id: str = ""
    zone_id: str = ""
    status: str = ""                        # ONLINE, OFFLINE, BUSY, NORMAL
    current_backlog: int = 0
    capacity_orders: int = 0
    avg_prep_minutes_observed: Optional[float] = None
    reason: Optional[str] = None


# ---------------------------------------------------------------------------
# 5. payment-events
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class PaymentEvent(BaseEvent):
    payment_id: str = ""
    order_id: str = ""
    customer_id: str = ""
    attempt_number: int = 1
    retry_of_payment_id: Optional[str] = None
    amount_inr: float = 0.0
    gateway: str = ""
    payment_method: str = ""
    status: str = ""                         # INITIATED, SUCCESS, FAILED
    failure_code: Optional[str] = None
    is_gateway_outage: bool = False


# ---------------------------------------------------------------------------
# 6. dispatch-decision-events
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class DispatchDecisionEvent(BaseEvent):
    dispatch_id: str = ""
    order_id: str = ""
    zone_id: str = ""
    assigned_dp_id: Optional[str] = None
    candidates_considered: int = 0
    assignment_score: Optional[float] = None
    is_batched: bool = False
    batch_id: Optional[str] = None
    batched_with_order_ids: list = dataclasses.field(default_factory=list)
    planned_pickup_eta_seconds: Optional[float] = None
    planned_route_distance_km: Optional[float] = None
    decision: str = ""                        # ASSIGNED | NO_DP_AVAILABLE | REASSIGNED
