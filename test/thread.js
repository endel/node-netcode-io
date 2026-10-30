// The thread WebSockets and plain HTTP run on for this run of the suite:
// `npm test` runs it once with each.
export const thread = process.env.NETCODE_IO_THREAD ?? 'node';
