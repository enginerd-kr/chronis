"""Redis-based storage adapter with indexing."""

import json
from typing import Any

from chronis.adapters.base import JobStorageAdapter
from chronis.type_defs import JobStorageData, JobUpdateData
from chronis.utils.time import parse_iso_datetime, utc_now


class RedisStorageAdapter(JobStorageAdapter):
    """
    Redis-based job storage adapter with secondary indexes.

    Design Decisions:
    - Uses Redis Hash (HSET/HGET) for job storage
    - Sorted Set for next_run_time indexing (fast time queries)
    - Sets for status indexing (fast status queries)
    - Job data stored as JSON string in hash field

    Schema:
        # Job data
        Hash: "{prefix}job:{job_id}" -> {data: json}

        # Indexes
        Sorted Set: "{prefix}index:by_time" -> {job_id: timestamp}
        Set: "{prefix}index:status:{status}" -> {job_id}
        Set: "{prefix}index:all" -> {job_id}

    Performance Characteristics:
    - O(1) for get/create/update/delete operations
    - O(log N) for time-range queries (sorted set)
    - O(1) for status queries (set operations)
    - Metadata filters O(N) but only on filtered results

    Features:
    - Fast queries with indexes
    - Atomic operations with pipelining
    - Compatible with Redis Cluster
    - Efficient polling (ZRANGEBYSCORE)

    Example:
        >>> import redis
        >>> client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        >>> storage = RedisStorageAdapter(client)
        >>> job = storage.create_job({
        ...     "job_id": "test-123",
        ...     "name": "Test Job",
        ...     "status": "scheduled",
        ...     ...
        ... })
    """

    def __init__(self, redis_client: Any, key_prefix: str = "chronis:") -> None:
        """
        Initialize Redis storage adapter.

        Args:
            redis_client: Redis client instance with decode_responses=True
            key_prefix: Prefix for all Redis keys (default: "chronis:")

        Example:
            >>> import redis
            >>> client = redis.Redis(
            ...     host='localhost',
            ...     port=6379,
            ...     db=0,
            ...     decode_responses=True  # Important for JSON
            ... )
            >>> storage = RedisStorageAdapter(client)
        """
        self.redis = redis_client
        self.key_prefix = key_prefix

    def _make_job_key(self, job_id: str) -> str:
        """Generate Redis key for job data."""
        return f"{self.key_prefix}job:{job_id}"

    def _make_time_index_key(self) -> str:
        """Generate key for time-based index."""
        return f"{self.key_prefix}index:by_time"

    def _make_status_index_key(self, status: str) -> str:
        """Generate key for status index."""
        return f"{self.key_prefix}index:status:{status}"

    def _make_all_jobs_key(self) -> str:
        """Generate key for all jobs set."""
        return f"{self.key_prefix}index:all"

    def _serialize(self, job_data: dict[str, Any]) -> str:
        """Serialize job data to JSON string."""
        return json.dumps(job_data)

    def _deserialize(self, data: str) -> dict[str, Any]:
        """Deserialize JSON string to job data."""
        return json.loads(data)

    def _timestamp_to_score(self, timestamp: str | None) -> float:
        """Convert ISO timestamp to Redis sorted set score."""
        if timestamp is None:
            return float("inf")  # Jobs without time go to end

        try:
            dt = parse_iso_datetime(timestamp)
            return dt.timestamp()
        except (ValueError, AttributeError):
            return float("inf")

    def _update_indexes(self, job_data: JobStorageData, pipeline: Any = None) -> None:
        """
        Update secondary indexes for a job.

        Args:
            job_data: Job data
            pipeline: Optional Redis pipeline for atomic operations
        """
        job_id = job_data["job_id"]
        status = job_data.get("status", "pending")
        next_run_time = job_data.get("next_run_time")

        pipe = pipeline or self.redis

        # Add to all jobs set
        pipe.sadd(self._make_all_jobs_key(), job_id)

        # Add to time index (sorted set)
        score = self._timestamp_to_score(next_run_time)
        pipe.zadd(self._make_time_index_key(), {job_id: score})

        # Add to status index
        pipe.sadd(self._make_status_index_key(status), job_id)

        if pipeline is None:
            # No pipeline provided, changes already executed
            pass

    def _remove_from_indexes(self, job_id: str, old_status: str | None = None) -> None:
        """
        Remove job from all indexes.

        Args:
            job_id: Job ID to remove
            old_status: Previous status (for removing from status index)
        """
        pipe = self.redis.pipeline()

        # Remove from all jobs set
        pipe.srem(self._make_all_jobs_key(), job_id)

        # Remove from time index
        pipe.zrem(self._make_time_index_key(), job_id)

        # Remove from all status indexes (if old_status unknown, try common ones)
        if old_status:
            pipe.srem(self._make_status_index_key(old_status), job_id)
        else:
            # Remove from all possible status indexes
            for status in ["pending", "scheduled", "running", "paused", "failed"]:
                pipe.srem(self._make_status_index_key(status), job_id)

        pipe.execute()

    def create_job(self, job_data: JobStorageData) -> JobStorageData:
        """
        Create a new job in Redis with indexes.

        Implementation:
            Pipeline:
            1. Check exists
            2. HSET job data
            3. SADD to all jobs
            4. ZADD to time index
            5. SADD to status index

        Raises:
            ValueError: If job already exists
        """
        job_id = job_data["job_id"]
        key = self._make_job_key(job_id)

        # Atomic check-and-set using HSETNX (returns 1 if created, 0 if exists)
        created = self.redis.hsetnx(key, "data", self._serialize(dict(job_data)))
        if not created:
            raise ValueError(f"Job {job_id} already exists")

        # Update indexes (job data is already stored)
        pipe = self.redis.pipeline()
        self._update_indexes(job_data, pipeline=pipe)
        pipe.execute()

        return job_data

    def get_job(self, job_id: str) -> JobStorageData | None:
        """
        Get a job from Redis.

        Implementation:
            HGET {job_key} data
        """
        key = self._make_job_key(job_id)
        data = self.redis.hget(key, "data")

        if data is None:
            return None

        return self._deserialize(data)  # type: ignore[return-value]

    def get_jobs_batch(self, job_ids: list[str]) -> dict[str, JobStorageData]:
        """
        Get multiple jobs from Redis using pipeline (optimized).

        Implementation:
            Uses Redis pipeline to batch HGET commands into single network call.
            20x faster than sequential get_job() calls for 20 jobs.

        Args:
            job_ids: List of job IDs to retrieve

        Returns:
            Dictionary mapping job_id to job data (only includes found jobs)
        """
        if not job_ids:
            return {}

        # Use pipeline to batch all HGET commands
        pipe = self.redis.pipeline()
        for job_id in job_ids:
            key = self._make_job_key(job_id)
            pipe.hget(key, "data")

        # Execute all commands in single network round-trip
        results = pipe.execute()

        # Build result dictionary
        jobs_dict: dict[str, JobStorageData] = {}
        for job_id, data in zip(job_ids, results, strict=False):
            if data is not None:
                jobs_dict[job_id] = self._deserialize(data)  # type: ignore[assignment]

        return jobs_dict

    def update_job(self, job_id: str, updates: JobUpdateData) -> JobStorageData:
        """
        Update a job in Redis with index maintenance.

        Implementation:
            1. Get current job
            2. Merge updates
            3. Update job data and indexes atomically

        Raises:
            ValueError: If job not found
        """
        job_data = self.get_job(job_id)
        if job_data is None:
            raise ValueError(f"Job {job_id} not found")

        # Remember old values for index updates
        old_status = job_data.get("status")

        # Merge updates
        job_data.update(updates)  # type: ignore[typeddict-item]
        job_data["updated_at"] = utc_now().isoformat()  # type: ignore[typeddict-item]

        # Use pipeline for atomic updates
        pipe = self.redis.pipeline()

        # Update job data
        key = self._make_job_key(job_id)
        pipe.hset(key, "data", self._serialize(dict(job_data)))

        # Remove from old status index if status changed
        new_status = job_data.get("status")
        if old_status and old_status != new_status:
            pipe.srem(self._make_status_index_key(old_status), job_id)

        # Update indexes
        self._update_indexes(job_data, pipeline=pipe)

        pipe.execute()

        return job_data

    def compare_and_swap_job(
        self,
        job_id: str,
        expected_values: dict[str, Any],
        updates: JobUpdateData,
    ) -> tuple[bool, JobStorageData | None]:
        """
        Atomically update a job only if current values match expected values (Redis).

        Uses Redis transaction (WATCH/MULTI/EXEC) for optimistic locking.

        Args:
            job_id: Job ID to update
            expected_values: Dictionary of field-value pairs that must match
            updates: Dictionary of fields to update

        Returns:
            Tuple of (success, updated_job_data)

        Raises:
            ValueError: If job_id not found
        """
        key = self._make_job_key(job_id)

        # Check if job exists first (before WATCH)
        job_data = self.get_job(job_id)
        if job_data is None:
            raise ValueError(f"Job {job_id} not found")

        # Use Redis WATCH for optimistic locking
        with self.redis.pipeline() as pipe:
            try:
                # Watch the key for changes
                pipe.watch(key)

                # Re-read under WATCH for consistency
                raw_data = pipe.hget(key, "data")
                if raw_data is None:
                    pipe.unwatch()
                    raise ValueError(f"Job {job_id} not found")
                job_data = self._deserialize(raw_data)  # type: ignore[assignment]

                # Check if expected values match
                for field, expected_value in expected_values.items():
                    current_value = job_data.get(field)
                    if current_value != expected_value:
                        # Mismatch - return failure
                        pipe.unwatch()
                        return (False, None)

                # Remember old values for index updates
                old_status = job_data.get("status")

                # Merge updates
                updated_job_data = job_data.copy()
                updated_job_data.update(updates)  # type: ignore[typeddict-item]
                updated_job_data["updated_at"] = utc_now().isoformat()  # type: ignore[typeddict-item]

                # Start transaction
                pipe.multi()

                # Update job data
                pipe.hset(key, "data", self._serialize(dict(updated_job_data)))

                # Remove from old status index if status changed
                new_status = updated_job_data.get("status")
                if old_status and old_status != new_status:
                    pipe.srem(self._make_status_index_key(old_status), job_id)

                # Update indexes
                self._update_indexes(updated_job_data, pipeline=pipe)

                # Execute transaction
                pipe.execute()

                return (True, updated_job_data)

            except ValueError:
                raise
            except Exception:
                # Transaction failed (key was modified by another instance)
                return (False, None)

    def delete_job(self, job_id: str) -> bool:
        """
        Delete a job from Redis with index cleanup.

        Implementation:
            Pipeline:
            1. Get job (for status)
            2. DEL job data
            3. Remove from all indexes
        """
        # Get job to know its status
        job_data = self.get_job(job_id)
        old_status = job_data.get("status") if job_data else None

        # Delete job data
        key = self._make_job_key(job_id)
        result = self.redis.delete(key)

        if result > 0:
            # Remove from indexes
            self._remove_from_indexes(job_id, old_status)
            return True

        return False

    def query_jobs(
        self,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[JobStorageData]:
        """
        Query jobs with filters using indexes.

        Optimized Queries:
        - status filter: Uses Set (O(1))
        - next_run_time_lte: Uses Sorted Set (O(log N))
        - Combined: Set intersection (O(N) where N = result size)

        Metadata filters still scan results (but smaller set after filtering).

        Args:
            filters: Filter dictionary
            limit: Maximum results
            offset: Skip first N results

        Returns:
            List of matching jobs sorted by next_run_time
        """
        job_ids: set[str] | None = None

        if filters:
            # Use time index for time queries
            if "next_run_time_lte" in filters:
                max_time = filters["next_run_time_lte"]
                max_score = self._timestamp_to_score(max_time)

                # Get all matching job IDs from time index
                # (offset/limit applied later after intersection and sorting)
                time_filtered_ids = self.redis.zrangebyscore(
                    self._make_time_index_key(), min=0, max=max_score
                )
                job_ids = set(time_filtered_ids) if time_filtered_ids else set()

            # Use status index for status queries
            if "status" in filters:
                status = filters["status"]
                status_filtered_ids = self.redis.smembers(self._make_status_index_key(status))

                if job_ids is None:
                    job_ids = set(status_filtered_ids)
                else:
                    # Intersection with time filter
                    job_ids &= set(status_filtered_ids)

        # If no indexed filters used, get all job IDs
        if job_ids is None:
            job_ids = set(self.redis.smembers(self._make_all_jobs_key()))

        # Fetch job data for filtered IDs using batch operation
        job_ids_list = list(job_ids)
        jobs_dict = self.get_jobs_batch(job_ids_list)
        jobs = list(jobs_dict.values())

        # Apply client-side filters on fetched results
        if filters:
            if "updated_at_lte" in filters:
                cutoff = filters["updated_at_lte"]
                jobs = [
                    j
                    for j in jobs
                    if j.get("updated_at") is not None and j.get("updated_at") <= cutoff
                ]

            for key, value in filters.items():
                if key.startswith("metadata."):
                    metadata_key = key.replace("metadata.", "")
                    jobs = [j for j in jobs if j.get("metadata", {}).get(metadata_key) == value]

        # Sort by next_run_time
        jobs.sort(key=lambda j: j.get("next_run_time") or "")

        # Apply offset and limit (single place, after all filtering and sorting)
        if offset:
            jobs = jobs[offset:]
        if limit:
            jobs = jobs[:limit]

        return jobs

    def count_jobs(self, filters: dict[str, Any] | None = None) -> int:
        """
        Count jobs matching filters using indexes.

        Optimized:
        - No filter: O(1) - SCARD on all jobs set
        - Status filter: O(1) - SCARD on status set
        - Time filter: O(log N) - ZCOUNT on sorted set
        """
        if filters is None:
            # Fast path: count all jobs
            return self.redis.scard(self._make_all_jobs_key())

        # Use indexes for fast counting
        if "status" in filters and "next_run_time_lte" not in filters:
            # Fast path: count by status only
            return self.redis.scard(self._make_status_index_key(filters["status"]))

        # For complex filters, query and count
        return len(self.query_jobs(filters=filters))
