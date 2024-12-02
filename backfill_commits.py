# don't use this for now
# it's very bad, will rewrite eventually
# runs about the same speed as the js file with the same name

from __future__ import annotations

import asyncio
import io
import json
import logging
import multiprocessing as mp
import os
import queue
import signal
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncGenerator, AsyncIterator, Dict, List, Optional, TypedDict, Callable, \
    TypeVar
from multiprocessing.connection import Connection

import aiohttp
import atproto.exceptions
from atmst.blockstore.car_file import ReadOnlyCARBlockStore
from atmst.mst.node_store import NodeStore
from atmst.mst.node_walker import NodeWalker
from cbrrr import decode_dag_cbor
from atproto import AsyncClient


# Type definitions
class BackfillLine(TypedDict):
    action: str
    timestamp: int
    uri: str
    cid: str
    record: Any


class WorkerToMasterMessage(TypedDict):
    type: str
    pds: Optional[str]
    did: Optional[str]
    until: Optional[int]


class MasterToWorkerMessage(TypedDict):
    type: str
    wait: bool
    waitTime: Optional[int]
    seen: Optional[bool]


# Set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

_T = TypeVar('_T')


# Function to get PDSes
async def get_pdses() -> List[str]:
    async with aiohttp.ClientSession() as session:
        async with session.get(
                "https://raw.githubusercontent.com/mary-ext/atproto-scraping/refs/heads/trunk/state.json"
        ) as response:
            if not response.ok:
                raise Exception("atproto-scraping state.json not ok")

            data = json.loads(await response.read())
            if not isinstance(data, dict) or "pdses" not in data:
                raise Exception("No pdses in atproto-scraping")

            return [
                pds
                for pds in data["pdses"].keys()
                if pds.startswith("https://") and "errorAt" not in data["pdses"][pds]
            ]


# Function to batch items from an async iterator
async def batch(iterable: AsyncIterator[Any], batch_size: int) -> AsyncGenerator[List[Any], None]:
    items: List[Any] = []
    async for item in iterable:
        items.append(item)
        if len(items) >= batch_size:
            yield items
            items = []

    if items:
        yield items


class XRPC:
    def __init__(self, service: str, pipe: Optional[Connection]):
        self.service = service
        self.client = AsyncClient(self.service)
        self.pipe = pipe
        self.backoffs = [1_000, 5_000, 15_000, 30_000, 60_000]

    async def process_ratelimit_headers(
            self,
            headers: Dict[str, str],
            url: str,
            on_ratelimit: callable
    ) -> None:
        remaining = headers.get('ratelimit-remaining')
        reset = headers.get('ratelimit-reset')

        if not remaining or not reset:
            return

        try:
            remaining = int(remaining)
            reset = int(reset) * 1000  # Convert to milliseconds

            if remaining <= 1:
                now = int(time.time() * 1000)
                wait_time = reset - now + 2000  # Add 2s buffer
                if wait_time > 0:
                    await on_ratelimit(wait_time)
        except ValueError:
            logger.error(f"Invalid ratelimit headers at url {url}")

    async def request(self, func: Callable[[AsyncClient], _T], attempt: int = 0) -> _T:
        url = self.service

        if self.pipe:
            msg: WorkerToMasterMessage = {
                "type": "checkCooldown",
                "pds": self.service,
                "did": None,
                "until": None,
            }
            self.pipe.send(msg)
            response: MasterToWorkerMessage = self.pipe.recv()
            if response["wait"] and response["waitTime"]:
                await asyncio.sleep((response["waitTime"] + 1000) / 1000)

        try:
            response = await func(self.client)
            return response

        except atproto.exceptions.RequestException as e:
            if e.response.status_code == 429:
                if self.pipe:
                    async def cooldown(wait_time: int):
                        msg: WorkerToMasterMessage = {
                            "type": "setCooldown",
                            "pds": self.service,
                            "did": None,
                            "until": int(time.time() * 1000) + wait_time,
                        }
                        self.pipe.send(msg)
                        await asyncio.sleep(wait_time / 1000)

                    await self.process_ratelimit_headers(
                        dict(e.response.headers),
                        self.service,
                        cooldown
                    )
                else:
                    await asyncio.sleep(
                        (self.backoffs[attempt] if attempt < len(self.backoffs) else self.backoffs[
                                                                                         -1] + 1000) / 1000
                    )
                logger.warning(f"Retrying request to {url}, on attempt {attempt}")
                return await self.request(func, attempt + 1)
            elif 400 <= e.response.status_code < 500 and func != self.client.com.atproto.sync.list_repos:
                logger.error(f"Request failed for {url}: {e.response.status_code}")
            elif attempt < len(self.backoffs) and func == self.client.com.atproto.sync.list_repos:
                await asyncio.sleep(self.backoffs[attempt] / 1000)
                logger.warning(f"Retrying request to {url}, on attempt {attempt} -- {e}")
                return await self.request(func, attempt + 1)
            else:
                logger.error(f"Request failed for {url}: {e}")
                raise

_session = None

async def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=300),
            connector=aiohttp.TCPConnector(
                ttl_dns_cache=300,
                limit_per_host=10,
                use_dns_cache=True,
            )
        )
    return _session


# Function to get repository data
async def get_repo(pds: str, did: str, pipe: Optional[Connection]) -> Optional[bytes]:
    rpc = XRPC(pds, pipe)
    try:
        response = await rpc.request(lambda client: client.com.atproto.sync.get_repo({"did": did}))
        if not response:
            logger.error(f"No data in response for getRepo {did} from {pds}")
        return response
    except Exception as e:
        logger.error(f"getRepo error for did {did} from pds {pds} --- {e}")
        return None


# Function to list repositories
async def list_repos(pds: str, pipe: Optional[Connection]) -> AsyncGenerator[str, None]:
    cursor: Optional[str] = None
    rpc = XRPC(pds, pipe)

    attempts = 0

    while True:
        try:
            logger.info(f"Listing repos for pds {pds} at cursor {cursor}")
            params = {"limit": 1000}
            if cursor:
                params["cursor"] = cursor
            response = await rpc.request(lambda client: client.com.atproto.sync.list_repos(params))

            repos = response.repos or []
            cursor = response.cursor or None

            for repo in repos:
                yield repo["did"]

            if not cursor:
                break

        except atproto.exceptions.NetworkError:
            if attempts > 5:
                logger.error(f"Network error for pds {pds} at cursor {cursor}")
                return
            await asyncio.sleep(5)
            attempts += 1

        except Exception as e:
            logger.error(f"listRepos error for pds {pds} at cursor {cursor}", exc_info=e)
            return  # Exit the generator


# Function to write lines to a file
def write_lines(data: Dict[str, Any]) -> bool:
    lines: str = data.get("lines")
    pds: str = data.get("pds")
    did: str = data.get("did")
    pipe: Connection = data.get("pipe")

    if not all([lines, pds, did, pipe]):
        logger.warning(f"Invalid write job data: {data}")
        return

    try:
        # Write the lines to file
        with open("backfill-unsorted.jsonl", "a", encoding='utf-8') as f:
            f.write(lines)
            f.flush()
            logger.info(f"Wrote {len(lines.splitlines())} lines for {did} to file")

        return True

    except Exception as e:
        logger.error(f"Error writing {did} commits: {e}", exc_info=True)
        return False


# Worker function to process repositories
def process_repo_worker(pipe: Connection, stop_event: mp.Event, repo_queue, write_queue) -> None:
    asyncio.set_event_loop(asyncio.new_event_loop())

    async def process_job():
        while not stop_event.is_set():
            try:
                job_data: Dict[str, Any] = repo_queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(1)
                continue
            except Exception as e:
                logger.error(f"Error getting job from queue: {e}")
                await asyncio.sleep(1)
                continue

            pds: str = job_data["pds"]
            did: str = job_data["did"]

            if not pds or not did:
                logger.warning(f"Invalid job data: {job_data}")
                continue

            # Check if DID is already seen
            msg: WorkerToMasterMessage = {
                "type": "checkDid",
                "pds": pds,
                "did": did,
                "until": None,
            }
            pipe.send(msg)
            response: MasterToWorkerMessage = pipe.recv()

            if response.get("seen"):
                continue  # Skip processing

            # Get the repository data
            repo_data = await get_repo(pds, did, pipe)
            if not repo_data:
                continue

            lines = []

            try:
                carfile = io.BytesIO(repo_data)
                bs = ReadOnlyCARBlockStore(carfile)
                root = decode_dag_cbor(bs.get_block(bytes(bs.car_root)))

                for path, cid in NodeWalker(NodeStore(bs), root["data"]).iter_kv():
                    key_parts = path.split('/')
                    if len(key_parts) != 2:
                        logger.warning(f"Invalid key parts for path {path}")
                        continue
                    collection = key_parts[0]
                    rkey = key_parts[1]

                    uri = f"at://{did}/{collection}/{rkey}"
                    record = decode_dag_cbor(bs.get_block(bytes(cid)), atjson_mode=True)

                    # Try to parse the timestamp from rkey or record
                    try:
                        timestamp = int(rkey.split('.')[0])
                    except:
                        if isinstance(record, dict) and "createdAt" in record:
                            try:
                                timestamp = int(
                                    datetime.fromisoformat(record["createdAt"]).timestamp() * 1000)
                            except:
                                timestamp = int(time.time() * 1000)
                        else:
                            timestamp = int(time.time() * 1000)

                    backfill_line: BackfillLine = {
                        "action": "create",
                        "timestamp": timestamp,
                        "uri": uri,
                        "cid": str(cid),
                        "record": record,
                    }

                    line = json.dumps(backfill_line) + "\n"
                    lines.append(line)

                # Only queue the write job if we actually have lines to write
                if lines:
                    # Send write job to write_queue
                    write_job_data = {
                        "lines": ''.join(lines),
                        "pds": pds,
                        "did": did,
                        "pipe": pipe,  # Pass the pipe for communication
                    }
                    write_queue.put(write_job_data)
                else:
                    logger.info(f"No lines to write for DID {did}")

            except:
                e = traceback.format_exc()
                logger.warning(f"iterateAtpRepo error for did {did} from pds {pds}\n{e}")

    try:
        asyncio.run(process_job())
    except Exception as e:
        logger.error(f"Exception in process_repo_worker: {e}", exc_info=e)


# Worker function to process write jobs
def process_write_worker(pipe: Connection, stop_event: mp.Event, write_queue) -> None:
    while not stop_event.is_set():
        try:
            job_data: Dict[str, Any] = write_queue.get_nowait()
            wrote = write_lines(job_data)
            if wrote:
                msg: WorkerToMasterMessage = {
                    "type": "markDidSeen",
                    "pds": job_data["pds"],
                    "did": job_data["did"],
                    "until": None,
                }
                pipe.send(msg)
        except queue.Empty:
            time.sleep(1)
            continue
        except Exception as e:
            logger.error(f"Error in write worker: {e}", exc_info=True)
            time.sleep(1)
            continue


# Master class to coordinate workers and manage shared state
class Master:
    def __init__(self, repo_queue, write_queue, stop_event):
        self.repo_queue = repo_queue
        self.write_queue = write_queue
        self.stop_event = stop_event
        self.num_cpus = os.cpu_count() or 4
        self.cooldowns: Dict[str, int] = {}
        self.workers: List[mp.Process] = []
        self.output_file = Path("backfill-unsorted.jsonl")
        self.output_lock = threading.Lock()
        self.pipes: List[Connection] = []

        # Load seen DIDs
        try:
            with open("seen-dids.json", "r") as f:
                self.seen_dids = json.load(f)
                logging.info(f"Loaded {len(self.seen_dids)} PDSes' seen DIDs")
        except FileNotFoundError:
            logging.warning("No seen-dids.json file found, starting with empty seen DIDs")
            self.seen_dids = {}
        except Exception as e:
            logging.error("Error loading seen-dids.json, starting with empty seen DIDs", exc_info=e)
            if os.path.exists("seen-dids.json"):
                os.rename("seen-dids.json", "seen-dids.json.bak")
            self.seen_dids = {}

    def get_cooldown(self, pds: str) -> Optional[int]:
        return self.cooldowns.get(pds)

    def set_cooldown(self, pds: str, until: int) -> None:
        self.cooldowns[pds] = until

    def mark_did_seen(self, pds: str, did: str) -> None:
        if pds not in self.seen_dids:
            self.seen_dids[pds] = {}
        self.seen_dids[pds][did] = True

    def is_did_seen(self, pds: str, did: str) -> bool:
        return bool(self.seen_dids.get(pds, {}).get(did, False))

    def save_seen_dids(self) -> None:
        to_write = json.dumps(self.seen_dids)
        if len(to_write) > os.path.getsize("seen-dids.json"):
            with open("seen-dids.json", "w") as f:
                f.write(to_write)
                did_count = sum(len(dids) for dids in self.seen_dids.values())
                logging.info(f"Saving seen DIDs, {did_count} total")

    def handle_worker_message(self, pipe: Connection) -> None:
        try:
            while not self.stop_event.is_set():
                if pipe.poll(1):
                    try:
                        msg: WorkerToMasterMessage = pipe.recv()
                        if msg["type"] == "checkCooldown":
                            cooldown_until = self.get_cooldown(msg["pds"])
                            now = int(time.time() * 1000)

                            response: MasterToWorkerMessage = {
                                "type": "cooldownResponse",
                                "wait": False,
                                "waitTime": None,
                                "seen": None
                            }
                            if cooldown_until and cooldown_until > now:
                                response["wait"] = True
                                response["waitTime"] = cooldown_until - now

                            pipe.send(response)

                        elif msg["type"] == "setCooldown":
                            if msg["until"]:
                                self.set_cooldown(msg["pds"], int(msg["until"]))

                        elif msg["type"] == "checkDid":
                            seen = self.is_did_seen(msg["pds"], msg["did"])
                            response: MasterToWorkerMessage = {
                                "type": "didResponse",
                                "wait": False,
                                "waitTime": None,
                                "seen": seen
                            }
                            pipe.send(response)

                        elif msg["type"] == "markDidSeen":
                            self.mark_did_seen(msg["pds"], msg["did"])
                    except EOFError:
                        break
        except EOFError:
            pass
        finally:
            pipe.close()
            pass

    def start_workers(self) -> None:
        # Start repo processing workers
        for _ in range(self.num_cpus - 1):
            parent_conn, child_conn = mp.Pipe()
            worker_proc = mp.Process(
                target=process_repo_worker,
                args=(child_conn, self.stop_event, self.repo_queue, self.write_queue)
            )
            worker_proc.start()
            self.workers.append(worker_proc)
            self.pipes.append(parent_conn)
            threading.Thread(target=self.handle_worker_message, args=(parent_conn,),
                             daemon=True).start()

        # Start write processing worker
        parent_conn, child_conn = mp.Pipe()
        worker_proc = mp.Process(
            target=process_write_worker,
            args=(child_conn, self.stop_event, self.write_queue)
        )
        worker_proc.start()
        self.workers.append(worker_proc)
        self.pipes.append(parent_conn)
        threading.Thread(target=self.handle_worker_message, args=(parent_conn,),
                         daemon=True).start()

    def cleanup(self) -> None:
        try:
            self.stop_event.set()
            self.save_seen_dids()
            for worker in self.workers:
                if worker.is_alive():
                    worker.terminate()
            for worker in self.workers:
                try:
                    worker.join(timeout=5)
                    if worker.is_alive():
                        worker.kill()
                except Exception as e:
                    logger.warning(f"Error while joining worker {worker.pid}: {e}")
            self.save_seen_dids()
        except Exception as e:
            logger.error(f"Exception during cleanup: {e}", exc_info=True)

    async def process_pds(self, pds: str) -> None:
        count = 0
        async for did in list_repos(pds, pipe=None):
            if not self.is_did_seen(pds, did):
                job_data = {"pds": pds, "did": did}
                self.repo_queue.put(job_data)
                count += 1

    async def run(self) -> None:
        # Start workers
        self.start_workers()

        # Periodically save seen DIDs
        async def save_loop():
            while not self.stop_event.is_set():
                self.save_seen_dids()
                await asyncio.sleep(20)

        save_task = asyncio.create_task(save_loop())

        try:
            await asyncio.gather(*[self.process_pds(pds) for pds in await get_pdses()])

            # Wait for queues to empty
            while not self.repo_queue.empty() or not self.write_queue.empty():
                await asyncio.sleep(1)

        finally:
            # Clean up
            self.stop_event.set()
            save_task.cancel()
            self.cleanup()
            if _session and not _session.closed:
                await _session.close()


if __name__ == '__main__':
    mp.set_start_method('spawn')

    repo_queue = mp.Queue()
    write_queue = mp.Queue()
    stop_event = mp.Event()

    master = Master(repo_queue, write_queue, stop_event)


    def signal_handler(signum, frame):
        logger.info(f"Received shutdown signal {signum}")
        traceback.print_stack(frame)
        master.cleanup()
        sys.exit(0)


    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(master.run())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        master.cleanup()
        sys.exit(1)
