"""
Runs the matching algorithm whenever an order becomes READY
(order_generator's on_order_ready callback fires into `assign()`).

Scoring is deliberately simple (nearest idle DP, tie-broken by rating) --
the point of this simulator is realistic *event shape and volume*, not a
state-of-the-art dispatch optimizer. Swap `_score_candidates` out freely if
you want to stress-test your pipeline against a smarter/dumber algorithm.
"""
from __future__ import annotations

import asyncio
import logging
import random
import uuid

from simulator.clock import SimClock
from simulator.config import AppConfig
from simulator.geo import haversine_km
from simulator.kafka_producer import EventPublisher
from simulator.runtime_state import RuntimeState
from simulator.schemas import DispatchDecisionEvent, iso_now
from simulator.state_machines import DPStatus

log = logging.getLogger("gen.dispatch")


class DispatchGenerator:
    def __init__(self, cfg: AppConfig, state: RuntimeState, clock: SimClock,
                 publisher: EventPublisher, rng: random.Random, order_manager, trip_runner):
        self.cfg = cfg
        self.state = state
        self.clock = clock
        self.publisher = publisher
        self.rng = rng
        self.order_manager = order_manager
        self.trip_runner = trip_runner

    def assign(self, order_id: str) -> None:
        asyncio.create_task(self._assign(order_id))

    async def _assign(self, order_id: str) -> None:
        # iterative retry loop (not recursive) so a long run of NO_DP_AVAILABLE
        # ticks during a supply shortage can't grow an unbounded call stack
        while True:
            order = self.state.active_orders.get(order_id)
            if order is None:
                return  # cancelled between READY and dispatch tick

            batch_target = self._find_batch_candidate(order)
            if batch_target is not None:
                await self._batch_onto(order, batch_target)
                return

            candidates = self.state.nearest_idle_dps(
                order.zone_id, order.pickup_location, self.cfg.dispatch.candidate_pool_size
            )
            if candidates:
                break

            await self._emit_decision(order, assigned_dp_id=None, candidates_considered=0,
                                       decision="NO_DP_AVAILABLE")
            # simple retry-after-delay -- represents dispatch engine re-polling
            await asyncio.sleep(self.clock.real_seconds_for_sim_seconds(self.rng.uniform(10, 25)))

        dp_id = candidates[0]
        dp = self.state.dps[dp_id]
        score = self._score(order, dp)

        self.state.unmark_dp_idle(dp_id)
        dp.active_order_ids = [order_id]
        order.dp_id = dp_id

        planned_distance = (
            haversine_km(dp.current_location, order.pickup_location)
            + haversine_km(order.pickup_location, order.dropoff_location)
        )

        await self._emit_decision(order, assigned_dp_id=dp_id, candidates_considered=len(candidates),
                                   decision="ASSIGNED", score=score, planned_distance=planned_distance)
        await self.order_manager.transition(order_id, "ASSIGNED")

        asyncio.create_task(self.trip_runner.run(dp_id, order_id))

    # ------------------------------------------------------------------
    def _find_batch_candidate(self, order):
        if not self.cfg.dispatch.batching_enabled:
            return None
        if self.rng.random() > self.cfg.dispatch.batching_probability_when_eligible:
            return None

        for other in self.state.active_orders.values():
            if other.order_id == order.order_id:
                continue
            if other.restaurant_id != order.restaurant_id:
                continue
            if other.dp_id is None:
                continue
            dp = self.state.dps.get(other.dp_id)
            if dp is None or dp.status != DPStatus.WAITING_AT_RESTAURANT:
                continue
            if len(dp.active_order_ids) >= 2:
                continue  # cap batch size at 2 for realism
            return dp.dp_id
        return None

    async def _batch_onto(self, order, dp_id: str) -> None:
        dp = self.state.dps[dp_id]
        batch_id = f"BATCH-{uuid.uuid4().hex[:10]}"
        dp.active_order_ids.append(order.order_id)
        order.dp_id = dp_id
        order.batch_id = batch_id
        self.state.pending_batch_dropoffs.setdefault(dp_id, []).append(order.order_id)

        planned_distance = haversine_km(dp.current_location, order.dropoff_location)
        await self._emit_decision(order, assigned_dp_id=dp_id, candidates_considered=1,
                                   decision="ASSIGNED", is_batched=True, batch_id=batch_id,
                                   planned_distance=planned_distance)
        await self.order_manager.transition(order.order_id, "ASSIGNED")
        log.info("order %s batched onto dp %s (%s)", order.order_id, dp_id, batch_id)

    def _score(self, order, dp) -> float:
        dist = haversine_km(dp.current_location, order.pickup_location)
        return round(max(0.0, 10.0 - dist) + dp.rating, 2)

    async def _emit_decision(self, order, assigned_dp_id, candidates_considered, decision,
                              score=None, planned_distance=None, is_batched=False,
                              batch_id=None) -> None:
        event = DispatchDecisionEvent(
            event_time=iso_now(self.clock.now()),
            dispatch_id=f"DISP-{uuid.uuid4().hex[:10]}",
            order_id=order.order_id,
            zone_id=order.zone_id,
            assigned_dp_id=assigned_dp_id,
            candidates_considered=candidates_considered,
            assignment_score=score,
            is_batched=is_batched,
            batch_id=batch_id,
            batched_with_order_ids=[],
            planned_pickup_eta_seconds=None,
            planned_route_distance_km=round(planned_distance, 3) if planned_distance else None,
            decision=decision,
        )
        await self.publisher.publish("dispatch_decision", key=order.order_id, event=event)
