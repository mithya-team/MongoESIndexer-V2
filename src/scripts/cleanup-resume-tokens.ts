/**
 * scripts/cleanup-resume-tokens.ts
 *
 * One-off script to prune the resume_tokens index down to a single most-recent
 * row per (collection, index) pair. Required because earlier versions of
 * acknowledgeChangeEvent minted a fresh row per event, producing millions of
 * leftover rows.
 *
 * Usage:
 *   node dist/scripts/cleanup-resume-tokens.js               # dry run
 *   node dist/scripts/cleanup-resume-tokens.js --apply       # delete extras
 *
 * Environment variables:
 *   ELASTICSEARCH_NODE      Required.
 *   ELASTICSEARCH_CA_CERT   Optional. Path to CA cert if cluster uses self-signed TLS.
 */

import { config as loadEnv } from 'dotenv';
loadEnv();

import { Client } from '@elastic/elasticsearch';
import * as fs from 'fs';

const INDEX = 'resume_tokens';

async function main() {
	const apply = process.argv.includes('--apply');

	const node = process.env.ELASTICSEARCH_NODE;
	if (!node) {
		throw new Error('ELASTICSEARCH_NODE must be set');
	}
	const tlsOptions: any = {};
	if (process.env.ELASTICSEARCH_CA_CERT) {
		tlsOptions.ca = fs.readFileSync(process.env.ELASTICSEARCH_CA_CERT);
	}

	const client = new Client({ node, tls: tlsOptions });

	const totalBefore = (await client.count({ index: INDEX })).count;
	console.log(`Total rows in ${INDEX} before cleanup: ${totalBefore}`);

	// Find all distinct (collection, index) pairs via composite aggregation.
	const pairs: Array<{ collection: string; index: string }> = [];
	let afterKey: any = undefined;
	for (;;) {
		const aggResp: any = await client.search({
			index: INDEX,
			size: 0,
			aggs: {
				pairs: {
					composite: {
						size: 1000,
						sources: [{ collection: { terms: { field: 'collection' } } }, { index: { terms: { field: 'index' } } }],
						...(afterKey ? { after: afterKey } : {}),
					},
				},
			},
		});
		const buckets = aggResp.aggregations?.pairs?.buckets ?? [];
		for (const b of buckets) pairs.push(b.key);
		if (!aggResp.aggregations?.pairs?.after_key || buckets.length === 0) break;
		afterKey = aggResp.aggregations.pairs.after_key;
	}
	console.log(`Found ${pairs.length} distinct (collection, index) pair(s)`);

	let totalDeleted = 0;
	for (const pair of pairs) {
		// Find the most-recent row to keep.
		const keepResp = await client.search({
			index: INDEX,
			size: 1,
			sort: [
				{ created: 'desc' },
				{ _id: 'desc' }, // deterministic tiebreaker
			],
			query: {
				bool: {
					filter: [{ term: { collection: pair.collection } }, { term: { index: pair.index } }],
				},
			},
		});
		const keep = keepResp.hits.hits[0];
		if (!keep) continue;

		// Count how many would be deleted.
		const countResp = await client.count({
			index: INDEX,
			query: {
				bool: {
					filter: [{ term: { collection: pair.collection } }, { term: { index: pair.index } }],
					must_not: [{ term: { _id: keep._id } }],
				},
			},
		});

		console.log(
			`  ${pair.collection} / ${pair.index} → keep ${keep._id} (created ${(keep._source as any)?.created}), delete ${countResp.count} other(s)`,
		);

		if (apply && countResp.count > 0) {
			const del = await client.deleteByQuery({
				index: INDEX,
				refresh: true,
				query: {
					bool: {
						filter: [{ term: { collection: pair.collection } }, { term: { index: pair.index } }],
						must_not: [{ term: { _id: keep._id } }],
					},
				},
			});
			totalDeleted += del.deleted ?? 0;
		}
	}

	const totalAfter = (await client.count({ index: INDEX })).count;
	console.log('---');
	console.log(`Rows before: ${totalBefore}`);
	console.log(`Rows deleted: ${totalDeleted}${apply ? '' : ' (DRY RUN — pass --apply to actually delete)'}`);
	console.log(`Rows after: ${totalAfter}`);

	await client.close();
}

main().catch((err) => {
	console.error(err);
	process.exit(1);
});
