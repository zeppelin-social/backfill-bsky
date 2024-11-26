import { sortFile } from 'large-sort'
import type { BackfillLine } from './shared.js'

await sortFile<BackfillLine>(
	"backfill-unsorted.jsonl",
	"backfill-sorted.jsonl",
	(str) => JSON.parse(str),
	(line) => JSON.stringify(line),
	(a, b) => a.timestamp > b.timestamp ? 1 : -1,
);

console.log(`Done sorting backfill data!
  
  Ensure the AppView is running, then run backfill-commits.ts to backfill the AppView.
  Keep buffer-relay.ts running to receive live events until this is done. Then run backfill-buffer.ts to fill in the buffer, then restart the AppView to begin from cursor: 0.`);
