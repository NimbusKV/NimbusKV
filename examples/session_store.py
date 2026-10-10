# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Hemanshu Vaidya
"""session_store.py - a realistic session-store example.

Shows: TTL-based session expiry, reactive logging of session lifecycle
events (useful for auditing/security monitoring), and atomic() for a
per-session request counter that's safe even if a session is hit by
concurrent requests.

Run: python examples/session_store.py
"""
import time

from nimbuskv import NimbusKV

SESSION_TTL_SECONDS = 60 * 30  # 30 minutes


def create_session(store: NimbusKV, user_id: str) -> str:
    session_id = f"sess_{user_id}_{int(time.time())}"
    store.set(
        f"session:{session_id}",
        {"user_id": user_id, "created_at": time.time(), "requests": 0},
        ttl=SESSION_TTL_SECONDS,
    )
    return session_id


def touch_session(store: NimbusKV, session_id: str) -> None:
    """Bump the request counter for a session -- safe under concurrent
    requests for the same session, no lock held."""
    key = f"session:{session_id}"

    def _bump(session):
        if session is None:
            return session  # already expired/gone -- nothing to bump
        session = dict(session)
        session["requests"] += 1
        return session

    store.atomic(key, _bump, default=None)


def main():
    with NimbusKV() as store:
        # Log every session lifecycle event -- this is the kind of thing
        # you'd wire into real audit logging or a security dashboard.
        def audit_log(key, event, old, new):
            if event == "expire":
                print(f"[audit] session expired: {key}")
            elif event == "set" and old is None:
                print(f"[audit] session created: {key}")

        store.subscribe(audit_log, key=None)

        session_id = create_session(store, user_id="alvin")
        print("Created session:", session_id)

        for _ in range(5):
            touch_session(store, session_id)

        session = store.get(f"session:{session_id}")
        print("Session state:", session)
        assert session["requests"] == 5

        # Simulate a short-lived demo session to show expiry firing --
        # not something you'd normally do with a real 30-minute TTL.
        demo_id = "demo_session"
        store.set(f"session:{demo_id}", {"user_id": "demo"}, ttl=1)
        print("Waiting for demo session to expire...")
        time.sleep(1.5)
        print("Demo session after expiry:", store.get(f"session:{demo_id}"))


if __name__ == "__main__":
    main()
