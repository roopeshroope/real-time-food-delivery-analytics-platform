"""
Emits payment-events. Triggered per-order via `charge(order_id, amount)`
(wired from order_generator's on_payment_needed callback). Runs an
independent background loop that rolls the dice each polling interval for a
new gateway-outage window; while an outage is active, failures spike and
correlate across concurrent payment attempts (a realistic "retry storm"
rather than independent per-payment coin flips).
"""
from __future__ import annotations

import asyncio
import logging
import random
import uuid

from simulator.chaos import GatewayOutageTracker
from simulator.clock import SimClock
from simulator.config import AppConfig
from simulator.kafka_producer import EventPublisher
from simulator.runtime_state import RuntimeState
from simulator.schemas import PaymentEvent, iso_now

log = logging.getLogger("gen.payment")


class PaymentGenerator:
    def __init__(self, cfg: AppConfig, state: RuntimeState, clock: SimClock,
                 publisher: EventPublisher, rng: random.Random):
        self.cfg = cfg
        self.state = state
        self.clock = clock
        self.publisher = publisher
        self.rng = rng
        self.outage_tracker = GatewayOutageTracker(cfg.payment, rng, clock)

    async def run_outage_watcher(self, stop_event: asyncio.Event) -> None:
        poll_sim_minutes = 3.0
        while not stop_event.is_set():
            real_sleep = self.clock.real_seconds_for(poll_sim_minutes)
            await asyncio.sleep(real_sleep)
            self.outage_tracker.maybe_start_outage(real_seconds_elapsed_hint=real_sleep)
            if self.outage_tracker.is_outage_active():
                log.debug("payment gateway outage window active")

    # ------------------------------------------------------------------
    def charge(self, order_id: str, amount_inr: float) -> None:
        customer_id = None
        order = self.state.active_orders.get(order_id)
        if order:
            customer_id = order.customer_id
        asyncio.create_task(self._attempt_charge(order_id, customer_id or "UNKNOWN", amount_inr))

    async def _attempt_charge(self, order_id: str, customer_id: str, amount_inr: float) -> None:
        pcfg = self.cfg.payment
        gateway = self.rng.choice(self.state.gateways)
        method = self.rng.choices(
            self.state.payment_methods, weights=[0.55, 0.25, 0.08, 0.07, 0.05], k=1
        )[0]

        retry_of: str | None = None
        for attempt in range(1, pcfg.max_retries + 2):  # +1 initial +1 buffer
            payment_id = f"PAY-{uuid.uuid4().hex[:12]}"
            is_outage = self.outage_tracker.is_outage_active()

            await self._emit(payment_id, order_id, customer_id, attempt, retry_of,
                              amount_inr, gateway, method, "INITIATED", None, is_outage)

            await asyncio.sleep(self.clock.real_seconds_for_sim_seconds(self.rng.uniform(1, 4)))

            failure_rate = pcfg.gateway_outage_failure_rate if is_outage else pcfg.base_failure_rate
            if method == "COD":
                failure_rate = 0.0  # cash on delivery can't fail at charge time

            if self.rng.random() < failure_rate:
                failure_code = self.rng.choice(
                    ["INSUFFICIENT_FUNDS", "BANK_TIMEOUT", "GATEWAY_5XX", "OTP_EXPIRED", "CARD_DECLINED"]
                )
                await self._emit(payment_id, order_id, customer_id, attempt, retry_of,
                                  amount_inr, gateway, method, "FAILED", failure_code, is_outage)

                if attempt <= pcfg.max_retries:
                    lo, hi = pcfg.retry_backoff_seconds_range
                    await asyncio.sleep(self.clock.real_seconds_for_sim_seconds(self.rng.uniform(lo, hi)))
                    retry_of = payment_id
                    continue
                else:
                    log.info("payment permanently failed order_id=%s after %d attempts", order_id, attempt)
                    return
            else:
                await self._emit(payment_id, order_id, customer_id, attempt, retry_of,
                                  amount_inr, gateway, method, "SUCCESS", None, is_outage)
                return

    async def _emit(self, payment_id, order_id, customer_id, attempt, retry_of, amount,
                     gateway, method, status, failure_code, is_outage) -> None:
        event = PaymentEvent(
            event_time=iso_now(self.clock.now()),
            payment_id=payment_id,
            order_id=order_id,
            customer_id=customer_id,
            attempt_number=attempt,
            retry_of_payment_id=retry_of,
            amount_inr=amount,
            gateway=gateway,
            payment_method=method,
            status=status,
            failure_code=failure_code,
            is_gateway_outage=is_outage,
        )
        await self.publisher.publish("payment", key=order_id, event=event)
