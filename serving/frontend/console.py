"""Streamlit analyst console: HITL review (Phase 1.6) + Registry editing (Phase 2).

Run:  SERVING_URL=<serving-api> streamlit run serving/frontend/console.py

Review queue tab: reads GET /v1/hitl/pending, shows the queue as a table plus a
best-effort map, and lets a reviewer label {correct, incorrect, ambiguous} via
POST /v1/feedback.

Registry tab: edits vessels / watchlist / sanctions_flags via /v1/registry/*.
Writes land in Postgres (the system of record); the CDC pipeline propagates them
to the online store the scorer reads, which is why a flag placed here shows up
in scoring within seconds without this console ever touching DynamoDB or Redis.
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st

from frontend.hitl_client import (
    LABELS,
    HitlApi,
    feedback_payload,
    format_row,
    positions_from_rows,
)
from frontend.registry_client import (
    RegistryApi,
    format_watchlist_row,
    sanction_payload,
    watchlist_payload,
)

st.set_page_config(page_title="Harbormaster console", layout="wide")
base_url = os.environ.get("SERVING_URL", "http://localhost:8000")
api = HitlApi(base_url)
registry = RegistryApi(base_url)

st.title("Harbormaster - analyst console")
reviewer = st.sidebar.text_input("Analyst", value=os.environ.get("USER", "analyst"))

tab_review, tab_registry = st.tabs(["Review queue", "Registry"])

with tab_review:
    rows: list[dict] | None
    try:
        rows = api.pending()
    except Exception as exc:
        st.error(f"could not reach the serving API: {exc}")
        rows = None

    if rows is not None:
        st.caption(f"{len(rows)} events awaiting review")
        if not rows:
            st.success("Queue is empty.")
        else:
            points = positions_from_rows(rows)
            if points:
                st.map(pd.DataFrame(points))

            st.dataframe([format_row(r) for r in rows], use_container_width=True)

            st.subheader("Label an event")
            trace = st.selectbox("trace_id", [r.get("trace_id") for r in rows])
            label = st.radio("verdict", LABELS, horizontal=True)
            notes = st.text_input("notes (optional)")
            if st.button("Submit verdict"):
                try:
                    resp = api.feedback(feedback_payload(trace, label, reviewer, notes or None))
                    st.success(f"recorded; {resp.get('queue_position', '?')} still pending")
                except Exception as exc:
                    st.error(f"feedback failed: {exc}")

with tab_registry:
    st.subheader("Watchlist")
    try:
        wl = registry.list_watchlist()
    except Exception as exc:
        st.error(f"could not reach the serving API: {exc}")
        wl = []
    if wl:
        st.dataframe([format_watchlist_row(r) for r in wl], use_container_width=True)
    else:
        st.caption("Watchlist is empty.")

    mmsi = st.number_input("MMSI", min_value=0, max_value=999_999_999, step=1, value=0)

    col_flag, col_unflag = st.columns(2)
    with col_flag:
        reason = st.text_input("reason")
        severity = st.slider("severity", 0.0, 1.0, 0.9, 0.05)
        if st.button("Flag vessel"):
            try:
                registry.put_watchlist(
                    int(mmsi), watchlist_payload(reason, severity, added_by=reviewer)
                )
                st.success(f"{int(mmsi)} flagged")
                st.rerun()
            except Exception as exc:
                st.error(f"flag failed: {exc}")
    with col_unflag:
        if st.button("Remove from watchlist"):
            try:
                registry.delete_watchlist(int(mmsi))
                st.success(f"{int(mmsi)} removed")
                st.rerun()
            except Exception as exc:
                st.error(f"remove failed: {exc}")

    st.subheader("Sanctions flags")
    col_add, col_clear = st.columns(2)
    with col_add:
        regime = st.text_input("regime (e.g. OFAC)")
        reference = st.text_input("reference (optional)")
        if st.button("Add sanctions flag"):
            try:
                registry.put_sanction(int(mmsi), sanction_payload(regime, reference))
                st.success(f"{int(mmsi)} flagged under {regime}")
            except Exception as exc:
                st.error(f"sanctions flag failed: {exc}")
    with col_clear:
        if st.button("Clear sanctions flags"):
            try:
                resp = registry.delete_sanctions(int(mmsi))
                st.success(f"cleared {resp.get('deleted', '?')} flags for {int(mmsi)}")
            except Exception as exc:
                st.error(f"clear failed: {exc}")
