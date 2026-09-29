"""
Canonical state machines. Generators are the things that actually *drive*
transitions (they own timing/probability decisions); this module is the
single source of truth for "what states exist" and "what transitions are
legal", so both the generators and any test/validation code reference the
same definitions instead of duplicating magic strings.
"""
from __future__ import annotations


class OrderState:
    PLACED = "PLACED"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    PREPARING = "PREPARING"
    READY = "READY"
    ASSIGNED = "ASSIGNED"
    PICKED_UP = "PICKED_UP"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"

    TERMINAL = {REJECTED, DELIVERED, CANCELLED}

    VALID_TRANSITIONS = {
        PLACED: {ACCEPTED, REJECTED, CANCELLED},
        ACCEPTED: {PREPARING, CANCELLED},
        PREPARING: {READY, CANCELLED},
        READY: {ASSIGNED, CANCELLED},
        ASSIGNED: {PICKED_UP, CANCELLED},
        PICKED_UP: {DELIVERED},
    }

    @classmethod
    def can_transition(cls, frm: str, to: str) -> bool:
        return to in cls.VALID_TRANSITIONS.get(frm, set())


class DPStatus:
    OFFLINE = "OFFLINE"
    IDLE = "IDLE"
    ASSIGNED = "ASSIGNED"
    EN_ROUTE_TO_RESTAURANT = "EN_ROUTE_TO_RESTAURANT"
    WAITING_AT_RESTAURANT = "WAITING_AT_RESTAURANT"
    PICKED_UP = "PICKED_UP"
    EN_ROUTE_TO_CUSTOMER = "EN_ROUTE_TO_CUSTOMER"
    DELIVERED = "DELIVERED"

    VALID_TRANSITIONS = {
        OFFLINE: {IDLE},
        IDLE: {OFFLINE, ASSIGNED},
        ASSIGNED: {EN_ROUTE_TO_RESTAURANT},
        EN_ROUTE_TO_RESTAURANT: {WAITING_AT_RESTAURANT},
        WAITING_AT_RESTAURANT: {PICKED_UP},
        PICKED_UP: {EN_ROUTE_TO_CUSTOMER},
        EN_ROUTE_TO_CUSTOMER: {DELIVERED},
        DELIVERED: {IDLE, ASSIGNED, OFFLINE},   # may pick up a batched order next
    }

    @classmethod
    def can_transition(cls, frm: str, to: str) -> bool:
        return to in cls.VALID_TRANSITIONS.get(frm, set())


class RestaurantStatus:
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"
    BUSY = "BUSY"
    NORMAL = "NORMAL"
