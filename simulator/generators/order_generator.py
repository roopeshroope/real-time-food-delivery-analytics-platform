"""
Owns the order-lifecycle-events stream end to end.

Two entry points:
  - `spawn_order()`      : called by the engine's Poisson arrival loop to
                             create a brand-new order and drive it through
                             PLACED -> ACCEPTED/REJECTED -> PREPARING -> READY
                             (including pre-ready cancellations).
  - `transition()`        : called by dispatch_generator / gps_generator to
                             progress an order beyond READY
                             (ASSIGNED -> PICKED_UP -> DELIVERED), or to
                             cancel at any point. Centralizing the actual
                             Kafka emission here means every state change,
                             regardless of which generator triggered it,
                             produces a consistent event shape.
"""
from __future__ import annotations

import asyncio
import logging
import random
import uuid
from datetime import timedelta
from typing import Callable, Optional

from simulator.clock import SimClock
from simulator.config import AppConfig
from simulator.entities import Customer, Restaurant
from simulator.geo import haversine_km
from simulator.kafka_producer import EventPublisher
from simulator.runtime_state import RuntimeState, ActiveOrder
from simulator.schemas import OrderLifecycleEvent, iso_now
from simulator.state_machines import OrderState

log = logging.getLogger("gen.order")


class OrderLifecycleManager:
    def __init__(self, cfg: AppConfig, state: RuntimeState, clock: SimClock,
                 publisher: EventPublisher, rng: random.Random):
        self.cfg = cfg
        self.state = state
        self.clock = clock
        self.publisher = publisher
        self.rng = rng

        # wired post-construction by engine.py to avoid circular imports
        self.on_order_ready: Optional[Callable[[str], None]] = None
        self.on_payment_needed: Optional[Callable[[str, float], None]] = None

    # ------------------------------------------------------------------
    async def spawn_order(self, zone_id: str) -> Optional[str]:
        restaurant_id = self.state.restaurant_for_order(zone_id)
        if restaurant_id is None:
            return None  # no online restaurants in this zone right now
        customer_pool = self.state.customers_in_zone(zone_id)
        if not customer_pool:
            return None
        customer_id = self.rng.choice(customer_pool)

        restaurant = self.state.restaurants[restaurant_id]
        customer = self.state.customers[customer_id]

        order_id = f"ORD-{uuid.uuid4().hex[:12]}"
        item_count = self.rng.randint(*self.cfg.order.items_per_order_range)
        order_value = round(item_count * self.rng.uniform(0.6, 1.4) * self.cfg.order.avg_item_price_inr, 2)

        distance_km = haversine_km(restaurant.location, customer.home)
        travel_minutes = distance_km / self.rng.uniform(0.25, 0.45)  # ~15-27 km/h effective
        promised_eta_minutes = restaurant.avg_prep_minutes + travel_minutes + self.rng.uniform(3, 8)
        promised_eta_ts = self.clock.now() + timedelta(minutes=promised_eta_minutes)

        active = ActiveOrder(
            order_id=order_id,
            customer_id=customer_id,
            restaurant_id=restaurant_id,
            zone_id=zone_id,
            state=OrderState.PLACED,
            promised_eta_ts=iso_now(promised_eta_ts),
            pickup_location=restaurant.location,
            dropoff_location=customer.home,
        )
        self.state.active_orders[order_id] = active

        await self._emit(active, OrderState.PLACED, item_count=item_count, order_value_inr=order_value)

        if self.on_payment_needed:
            self.on_payment_needed(order_id, order_value)

        asyncio.create_task(self._drive_to_ready(active, restaurant, customer))
        return order_id

    # ------------------------------------------------------------------
    async def _drive_to_ready(self, order: ActiveOrder, restaurant: Restaurant, customer: Customer) -> None:
        ocfg = self.cfg.order
        rcfg = self.cfg.restaurant

        # -- pre-accept cancellation window --
        await asyncio.sleep(self.clock.real_seconds_for_sim_seconds(self.rng.uniform(5, 25)))
        if self.rng.random() < ocfg.cancel_before_accept_probability:
            await self.transition(order.order_id, OrderState.CANCELLED,
                                   cancelled_by="CUSTOMER", cancellation_reason="changed_mind")
            return

        # -- restaurant accept/reject decision --
        accept_delay_sec = min(rcfg.accept_sla_seconds,
                                max(5, self.rng.gauss(rcfg.accept_sla_seconds * 0.45, 15)))
        await asyncio.sleep(self.clock.real_seconds_for_sim_seconds(accept_delay_sec))

        roll = self.rng.random()
        if roll < rcfg.reject_probability:
            await self.transition(order.order_id, OrderState.REJECTED,
                                   rejection_reason=self.rng.choice(
                                       ["item_unavailable", "kitchen_overloaded", "closing_soon"]))
            return
        if roll < rcfg.reject_probability + rcfg.no_response_probability:
            # restaurant silently failed to act within SLA -> system auto-cancels
            await asyncio.sleep(self.clock.real_seconds_for_sim_seconds(rcfg.accept_sla_seconds))
            await self.transition(order.order_id, OrderState.CANCELLED,
                                   cancelled_by="SYSTEM", cancellation_reason="restaurant_no_response")
            return

        await self.transition(order.order_id, OrderState.ACCEPTED)
        restaurant.current_backlog += 1

        # -- post-accept cancellation window (during prep) --
        if self.rng.random() < ocfg.cancel_after_accept_probability:
            delay = self.rng.uniform(0.2, 0.6) * restaurant.avg_prep_minutes
            await asyncio.sleep(self.clock.real_seconds_for(delay))
            restaurant.current_backlog = max(0, restaurant.current_backlog - 1)
            await self.transition(order.order_id, OrderState.CANCELLED,
                                   cancelled_by="CUSTOMER", cancellation_reason="ordered_by_mistake")
            return

        await self.transition(order.order_id, OrderState.PREPARING)

        # -- prep time --
        prep_minutes = max(2.0, self.rng.gauss(restaurant.avg_prep_minutes, rcfg.prep_variance_minutes))
        # busier kitchens (bigger backlog) run slower -- realistic queuing effect
        prep_minutes *= 1 + 0.08 * max(0, restaurant.current_backlog - 1)
        await asyncio.sleep(self.clock.real_seconds_for(prep_minutes))

        restaurant.current_backlog = max(0, restaurant.current_backlog - 1)

        if self.rng.random() < ocfg.cancel_after_ready_probability:
            await self.transition(order.order_id, OrderState.CANCELLED,
                                   cancelled_by="CUSTOMER", cancellation_reason="took_too_long")
            return

        order.predicted_ready_ts = iso_now(self.clock.now())
        await self.transition(order.order_id, OrderState.READY, predicted_ready_ts=order.predicted_ready_ts)

        if self.on_order_ready:
            self.on_order_ready(order.order_id)

    # ------------------------------------------------------------------
    async def transition(self, order_id: str, new_state: str, **extra) -> bool:
        order = self.state.active_orders.get(order_id)
        if order is None:
            log.warning("transition() on unknown order_id=%s", order_id)
            return False

        if new_state != OrderState.CANCELLED and not OrderState.can_transition(order.state, new_state):
            log.warning("illegal transition order=%s %s -> %s", order_id, order.state, new_state)
            return False

        previous_state = order.state
        order.state = new_state
        await self._emit(order, new_state, previous_state=previous_state, **extra)

        if new_state in OrderState.TERMINAL:
            self.state.active_orders.pop(order_id, None)
        return True

    async def _emit(self, order: ActiveOrder, state: str, previous_state: Optional[str] = None,
                     **extra) -> None:
        event = OrderLifecycleEvent(
            event_time=iso_now(self.clock.now()),
            order_id=order.order_id,
            customer_id=order.customer_id,
            restaurant_id=order.restaurant_id,
            zone_id=order.zone_id,
            state=state,
            previous_state=previous_state,
            promised_eta_ts=order.promised_eta_ts,
            dp_id=order.dp_id,
            **extra,
        )
        await self.publisher.publish("order_lifecycle", key=order.order_id, event=event)
