import { AsyncResource } from "node:async_hooks";
import { EventEmitter } from "node:events";
import * as os from "node:os";
import { type TransferListItem, Worker } from "node:worker_threads";

declare module "node:worker_threads" {
	interface Worker {
		[kTaskInfo]: WorkerPoolTaskInfo<any> | null;
	}
}

type TaskCallback<T> = (err: Error | null, result: T) => void;

const kTaskInfo = Symbol("kTaskInfo");
const kWorkerFreedEvent = Symbol("kWorkerFreedEvent");

class WorkerPoolTaskInfo<Result> extends AsyncResource {
	callback: TaskCallback<Result>;

	constructor(callback: TaskCallback<Result>) {
		super("WorkerPoolTaskInfo");
		this.callback = callback;
	}

	done(err: Error | null, result: any) {
		this.runInAsyncScope(this.callback, null, err, result);
		this.emitDestroy(); // `TaskInfo`s are used only once.
	}
}

// A simple worker pool that runs tasks in parallel.
export class WorkerPool<Task, Result> extends EventEmitter {
	file: string;
	numThreads: number;
	workers: Worker[];
	freeWorkers: Worker[];
	tasks: { task: Task; callback: (err: Error | null, result: Result) => void }[];

	constructor(file: string, numThreads = os.availableParallelism()) {
		super();
		this.file = file;
		this.numThreads = numThreads;
		this.workers = [];
		this.freeWorkers = [];
		this.tasks = [];

		for (let i = 0; i < numThreads; i++) this.addNewWorker();

		// Any time the kWorkerFreedEvent is emitted, dispatch
		// the next task pending in the queue, if any.
		this.on(kWorkerFreedEvent, () => {
			if (this.tasks.length > 0) {
				const { task, callback } = this.tasks.shift()!;
				this.queueTask(task, callback);
			}
		});
	}

	queueTask(task: Task, callback: TaskCallback<Result>) {
		if (this.freeWorkers.length === 0) {
			// No free threads, wait until a worker thread becomes free.
			this.tasks.push({ task, callback });
			return;
		}

		const worker = this.freeWorkers.pop()!;
		worker[kTaskInfo] = new WorkerPoolTaskInfo(callback);
		const transferables = extractTransferables(task);
		worker.postMessage(task, transferables);
	}

	async close() {
		return Promise.all(this.workers.map((worker) => worker.terminate()));
	}

	private addNewWorker() {
		const worker = new Worker(new URL(this.file, import.meta.url));
		// The worker thread will send a message to this thread when it's done.
		worker.on("message", (result: any) => {
			worker[kTaskInfo]?.done(null, result);
			worker[kTaskInfo] = null;
			this.freeWorkers.push(worker);
			this.emit(kWorkerFreedEvent);
		});
		// If there's no callback for a task, the pool will emit any errors.
		worker.on("error", (err: Error) => {
			if (worker[kTaskInfo]) worker[kTaskInfo].done(err, null);
			else this.emit("error", err);

			this.workers.splice(this.workers.indexOf(worker), 1);
			this.addNewWorker();
		});
		this.workers.push(worker);
		this.freeWorkers.push(worker);
		this.emit(kWorkerFreedEvent);
	}
}

function extractTransferables(
	obj: any,
	transferables: Array<TransferListItem> = [],
): Array<TransferListItem> {
	if (typeof obj !== "object") return transferables;
	for (const key of Object.keys(obj)) {
		const value = obj[key];
		if (value instanceof ArrayBuffer) {
			transferables.push(value);
		} else if (ArrayBuffer.isView(value) && value.buffer instanceof ArrayBuffer) {
			transferables.push(value.buffer);
		} else if (value && typeof value === "object" && Object.hasOwn(obj, key)) {
			extractTransferables(value, transferables);
		}
	}
	return transferables;
}
