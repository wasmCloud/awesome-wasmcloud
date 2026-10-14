"""Workflows that call the example workload's activities.

The activities run in a wasmCloud workload, not in Python, so each one is
called by name, on the task queue the plugin polls.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

# The task queue the plugin polls, from `task-queues` in ../.wash/config.yaml.
ACTIVITY_QUEUE = "wasmcloud-example"


@workflow.defn
class Greet:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            "greet",
            name,
            task_queue=ACTIVITY_QUEUE,
            start_to_close_timeout=timedelta(seconds=10),
        )


@workflow.defn
class Fail:
    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.execute_activity(
                "fail",
                task_queue=ACTIVITY_QUEUE,
                start_to_close_timeout=timedelta(seconds=10),
            )
        except ActivityError as err:
            # The workload's failure arrives as the cause, with its message,
            # error type and retryable flag.
            cause = err.cause
            assert isinstance(cause, ApplicationError)
            return (
                f"ApplicationError: {cause.message} "
                f"(type {cause.type}, non_retryable={cause.non_retryable})"
            )
        return "the activity did not fail"


@workflow.defn
class Flaky:
    @workflow.run
    async def run(self) -> str:
        # Temporal retries a retryable failure by this policy, and each retry
        # reaches the workload again, with `attempt` one higher.
        return await workflow.execute_activity(
            "flaky",
            task_queue=ACTIVITY_QUEUE,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1)),
        )


@workflow.defn
class Slow:
    @workflow.run
    async def run(self, seconds: int) -> str:
        return await workflow.execute_activity(
            "slow",
            seconds,
            task_queue=ACTIVITY_QUEUE,
            start_to_close_timeout=timedelta(seconds=seconds + 30),
            # Temporal treats an attempt with no heartbeat for this long as
            # lost. The workload heartbeats about once a second.
            heartbeat_timeout=timedelta(seconds=5),
            # When the workflow is cancelled, wait for the workload to stop
            # and report the cancellation, rather than moving on at once.
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
