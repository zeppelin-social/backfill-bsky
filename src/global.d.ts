declare module "shared-node-buffer" {
	export default class SharedNodeBuffer extends Buffer {
		constructor(key: string, size: number);
	}
}
