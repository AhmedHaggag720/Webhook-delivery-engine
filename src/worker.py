import asyncio
import logging
import signal
import sys

from src.db import db
from src.dispatcher import dispatcher
from src.redis_ops import redis_manager
from src.telemetry import setup_telemetry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker")

async def main():
    setup_telemetry("webhook-worker")
    await db.connect()
    await redis_manager.connect()

    try:
        await db.apply_migrations("migrations/001_initial_schema.sql")
    except Exception as e:
        logger.warning(f"Note applying migrations: {e}")

    await dispatcher.start()

    stop_event = asyncio.Event()

    def handle_signal():
        logger.info("Received termination signal. Initiating graceful shutdown...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_signal)

    worker_task = asyncio.create_task(dispatcher.run_loop())

    try:
        if sys.platform == "win32":
            # On Windows signal handling is simpler via polling stop_event or KeyboardInterrupt
            while not stop_event.is_set():
                await asyncio.sleep(0.5)
        else:
            await stop_event.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("KeyboardInterrupt received in worker.")
    finally:
        await dispatcher.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        await redis_manager.disconnect()
        await db.disconnect()
        logger.info("Worker process exited successfully.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker terminated by user.")

