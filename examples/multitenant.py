"""Multi-tenant scheduler example."""

from chronis import PollingScheduler
from chronis.adapters.lock import InMemoryLockAdapter
from chronis.adapters.storage import InMemoryStorageAdapter


def send_report(tenant_name: str):
    """Send report for a tenant."""
    print(f"Sending report for tenant: {tenant_name}")


def main():
    # 1. Create adapters
    storage = InMemoryStorageAdapter()
    lock = InMemoryLockAdapter()

    # 2. Create scheduler
    scheduler = PollingScheduler(
        storage_adapter=storage,
        lock_adapter=lock,
        polling_interval_seconds=5,
    )

    # 3. Register function
    scheduler.register_job_function("send_report", send_report)

    # 4. Create jobs for different tenants
    print("Creating multi-tenant jobs...\n")

    # Tenant: ACME Corp
    scheduler.create_interval_job(
        job_id="acme-daily-report",
        name="ACME Daily Report",
        func="send_report",
        seconds=30,
        kwargs={"tenant_name": "ACME Corp"},
        metadata={"tenant_id": "acme", "plan": "enterprise"}
    )

    # Tenant: Widget Inc
    scheduler.create_interval_job(
        job_id="widget-daily-report",
        name="Widget Daily Report",
        func="send_report",
        seconds=30,
        kwargs={"tenant_name": "Widget Inc"},
        metadata={"tenant_id": "widget", "plan": "basic"}
    )

    # Tenant: TechStart
    scheduler.create_interval_job(
        job_id="techstart-hourly-sync",
        name="TechStart Hourly Sync",
        func="send_report",
        seconds=20,
        kwargs={"tenant_name": "TechStart"},
        metadata={"tenant_id": "techstart", "plan": "pro"}
    )

    # 5. Query jobs by tenant
    print("=== All Jobs ===")
    all_jobs = scheduler.query_jobs()
    for job in all_jobs:
        tenant_id = job.metadata.get("tenant_id", "N/A")
        plan = job.metadata.get("plan", "N/A")
        print(f"  {job.job_id}: tenant={tenant_id}, plan={plan}")

    print("\n=== ACME Jobs Only ===")
    acme_jobs = scheduler.query_jobs(
        filters={"metadata.tenant_id": "acme"}
    )
    for job in acme_jobs:
        print(f"  {job.job_id}: {job.name}")

    print("\n=== Enterprise Plan Jobs ===")
    enterprise_jobs = scheduler.query_jobs(
        filters={"metadata.plan": "enterprise"}
    )
    for job in enterprise_jobs:
        print(f"  {job.job_id}: {job.name}")

    # 6. Start scheduler
    print("\n=== Starting Scheduler ===")
    print("Press Ctrl+C to stop\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\nStopping scheduler...")
        scheduler.stop()
        print("Scheduler stopped")


if __name__ == "__main__":
    main()
