"""
Supabase integration for Google Maps Reviews Scraper.

Fetches doctors and hospitals from Supabase, and upserts doctor-matched
reviews into the Testimonials table.
"""

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger("scraper")


class SupabaseHandler:
    """
    Handles all Supabase interactions for the testimonials pipeline.

    Mirrors the pattern of MongoDBStorage: lazy-connect on first use,
    methods return empty collections on failure rather than raising.
    """

    def __init__(self, config: Dict[str, Any]):
        supabase_cfg = config.get("supabase", {})
        self.url: str = supabase_cfg.get("url", "")
        self.key: str = supabase_cfg.get("key", "")
        # sync_mode is used in upsert_testimonial to decide conflict behaviour
        self.sync_mode: str = supabase_cfg.get("sync_mode", "new_only")
        self._client = None
        self.connected: bool = False

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Initialise the supabase-py client and verify connectivity."""
        if not self.url or not self.key:
            log.error("Supabase: 'url' and 'key' must be set in config.supabase")
            return False
        try:
            from supabase import create_client
            self._client = create_client(self.url, self.key)
            # Lightweight liveness check — fetches one doctor row
            self._client.table("Doctors").select("id").limit(1).execute()
            self.connected = True
            log.info("Supabase: connected to %s", self.url)
            return True
        except Exception as exc:
            log.error("Supabase: connection failed — %s", exc)
            self.connected = False
            return False

    def _ensure_connected(self) -> bool:
        if not self.connected:
            return self.connect()
        return True

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get_doctors(self, hospital_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch doctors from the Doctors table.

        Args:
            hospital_id: When provided, only doctors belonging to that hospital
                         are returned. Pass None to fetch all doctors.

        Returns:
            List of dicts with at minimum: id, name, hospital_id.
        """
        if not self._ensure_connected():
            log.warning("Supabase: cannot fetch doctors — not connected")
            return []
        try:
            query = self._client.table("Doctors").select("id, name, hospital_id")
            if hospital_id:
                query = query.eq("hospital_id", hospital_id)
            response = query.execute()
            doctors = response.data or []
            log.info("Supabase: fetched %d doctor(s)", len(doctors))
            return doctors
        except Exception as exc:
            log.error("Supabase: failed to fetch doctors — %s", exc)
            return []

    def get_hospitals(self) -> List[Dict[str, Any]]:
        """
        Fetch hospitals from the Hospitals table.

        Requires the 'google_maps_url' column to exist on Hospitals.
        Run the prerequisite ALTER TABLE from add_google_review_id.sql first.

        Returns:
            List of dicts with at minimum: id, name, google_maps_url.
        """
        if not self._ensure_connected():
            log.warning("Supabase: cannot fetch hospitals — not connected")
            return []
        try:
            response = (
                self._client.table("Hospitals")
                .select("id, name, google_maps_url")
                .execute()
            )
            hospitals = response.data or []
            log.info("Supabase: fetched %d hospital(s)", len(hospitals))
            return hospitals
        except Exception as exc:
            log.error("Supabase: failed to fetch hospitals — %s", exc)
            return []

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def upsert_testimonial(self, data: Dict[str, Any]) -> None:
        """
        Upsert a single testimonial into the Testimonials table.

        Uses google_review_id as the unique conflict key so re-runs are
        idempotent.  When sync_mode is "new_only", existing rows are skipped
        rather than overwritten.

        Args:
            data: Column → value mapping. Must include 'google_review_id'.
        """
        if not self._ensure_connected():
            log.warning("Supabase: cannot upsert testimonial — not connected")
            return
        try:
            if self.sync_mode == "new_only":
                # Insert only; silently skip on conflict (never overwrite)
                self._client.table("Testimonials").upsert(
                    data,
                    on_conflict="google_review_id,doctor_id",
                    ignore_duplicates=True,
                ).execute()
            else:
                # update / full: insert or overwrite on conflict
                self._client.table("Testimonials").upsert(
                    data,
                    on_conflict="google_review_id,doctor_id",
                ).execute()
        except Exception as exc:
            log.error(
                "Supabase: failed to upsert testimonial (google_review_id=%s) — %s",
                data.get("google_review_id"),
                exc,
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """No-op — supabase-py uses a stateless REST client."""
        self.connected = False
