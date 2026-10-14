"""Runs one of the example workflows: python run.py greet|fail|flaky|cancel

The workflows run in a Python worker started here. The activities they call
run in the wasmCloud workload, which the plugin hands them to.
"""

import asyncio
import sys
import uuid

from temporalio.client import Client, WorkflowFailureError
from temporalio.worker import Worker

from workflows import Fail, Flaky, Greet, Slow

TEMPORAL_ADDRESS = "localhost:7233"
# The workflows' own task queue, polled by the Python worker below. The
# activities go to `wasmcloud-example`, which the plugin polls.
WORKFLOW_QUEUE = "wasmcloud-example-workflows"


async def main(scenario: str) -> None:
    client = await Client.connect(TEMPORAL_ADDRESS)
    async with Worker(
        client, task_queue=WORKFLOW_QUEUE, workflows=[Greet, Fail, Flaky, Slow]
    ):
        workflow_id = f"wasmcloud-example-{scenario}-{uuid.uuid4().hex[:8]}"
        start = {"id": workflow_id, "task_queue": WORKFLOW_QUEUE}

        if scenario == "greet":
            print(await client.execute_workflow(Greet.run, "wasmCloud", **start))
        elif scenario == "fail":
            print(await client.execute_workflow(Fail.run, **start))
        elif scenario == "flaky":
            print(await client.execute_workflow(Flaky.run, **start))
        elif scenario == "cancel":
            handle = await client.start_workflow(Slow.run, 30, **start)
            await asyncio.sleep(5)
            print("cancelling the workflow")
            await handle.cancel()
            try:
                await handle.result()
            except WorkflowFailureError as err:
                print(f"workflow ended: {err.cause!r}")
            # The workload saw the cancellation on its next heartbeat, stopped,
            # and reported it. Temporal records that in the history.
            async for event in handle.fetch_history_events():
                if event.HasField("activity_task_canceled_event_attributes"):
                    print("activity stopped: ActivityTaskCanceled")
                elif event.HasField("activity_task_completed_event_attributes"):
                    print("activity was not cancelled: it ran to the end")
        print(f"see it in the web UI: http://localhost:8233/namespaces/default/workflows/{workflow_id}")


if __name__ == "__main__":
    scenarios = ("greet", "fail", "flaky", "cancel")
    if len(sys.argv) != 2 or sys.argv[1] not in scenarios:
        sys.exit(f"usage: python run.py {'|'.join(scenarios)}")
    asyncio.run(main(sys.argv[1]))
