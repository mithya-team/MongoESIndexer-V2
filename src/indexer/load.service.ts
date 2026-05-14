import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { Client } from '@elastic/elasticsearch';
import { IndicesCreateRequest, IndicesPutMappingRequest } from '@elastic/elasticsearch/lib/api/types';
import * as fs from 'fs/promises';
import path from 'path';
import { ExtractService } from './extract.service';
import { Configuration, ConfigurationSchema } from '../configuration';
import Bottleneck from 'bottleneck';
import cliProgress from 'cli-progress';
import { ObjectId } from 'mongodb';
import humanizeDuration from 'humanize-duration';
import { hasOnlyIndexingFields } from '@/utils/array-utils';
import { TransformService } from './transform.service';
import cron from 'node-cron';

/**
 * Service responsible for loading and managing data synchronization between MongoDB and Elasticsearch.
 * Handles index creation, document indexing, change stream processing, and bulk operations.
 * Implements OnModuleInit to initialize configurations and start indexing on application startup.
 */
@Injectable()
export class LoadService implements OnModuleInit {
	configs: Configuration[] = [];
	constructor(
		private readonly extractService: ExtractService,
		private readonly transformService: TransformService,
		@Inject('ESClient') private readonly esClient: Client,
	) {}

	/**
	 * Lifecycle hook that initializes the service by loading configurations from the configs directory.
	 * Called automatically when the module is initialized.
	 */
	onModuleInit() {
		this.init(process.env.CONFIGS_DIR || path.join(__dirname, '../../configs'));
	}

	/**
	 * Initializes the service by loading and processing configuration files.
	 * Creates necessary Elasticsearch indices and starts the indexing process.
	 *
	 * Implementation:
	 * 1. Reads all configuration files from the specified directory
	 * 2. Creates a resume_tokens index for tracking change streams
	 * 3. Processes each configuration file
	 * 4. Handles force delete if specified
	 * 5. Creates or updates Elasticsearch indices
	 * 6. Initiates the indexing process
	 *
	 * @param configDir - Directory path containing configuration files
	 */
	async init(configDir: string) {
		const configFiles = await fs.readdir(configDir);

		await this.upsertIndex({
			index: 'resume_tokens',
			mappings: {
				properties: {
					token: { type: 'keyword' },
					collection: { type: 'keyword' },
					created: {
						type: 'date',
					},
					index: {
						type: 'keyword',
					},
				},
			},
		});
		for (const configFile of configFiles) {
			const config = ConfigurationSchema.parse(
				JSON.parse(await fs.readFile(configDir + '/' + configFile, 'utf8')),
			);
			this.configs.push(config);
			if (config.force_delete) {
				console.log(`deleteIndex: ${config.index_name}`);
				await this.deleteIndex(config.index_name);
				await this.extractService.updateMany(
					config.collection,
					{},
					{ lastESIndexedAt: null, lastESIndexResponse: null },
				);
			}

			await this.upsertIndex({ ...config.index_params, index: config.index_name } as IndicesCreateRequest);

			// run every minute
			cron.schedule('* * * * *', () => this.handleNewDocuments(config.collection, config.index_name));
			if (config.update_field) {
				cron.schedule('* * * * *', () =>
					this.handleUpdatedDocuments(config.collection, config.index_name, config.update_field),
				);
			}
		}
		await this.indexAll();
	}

	/**
	 * Initiates the indexing process for all configured collections.
	 * Creates progress bars for monitoring and handles concurrent indexing.
	 *
	 * Implementation:
	 * 1. Creates a multi-progress bar for visual feedback
	 * 2. Sets up a rate limiter for concurrent operations
	 * 3. Processes each configuration in parallel
	 * 4. Starts change stream handlers for real-time updates
	 */
	async indexAll() {
		const multiBar = new cliProgress.MultiBar(
			{
				clearOnComplete: false,
				hideCursor: true,
				format: `[{bar}] {index_name} Progress: {percentage}% | {value}/{total} | ETA: {humanized_eta} | Skipped: {skipped}, Total: {total_documents}`,
			},
			cliProgress.Presets.shades_grey,
		);
		const limiter = new Bottleneck({
			maxConcurrent: 3,
		});

		await Promise.all(
			this.configs.map(async (config) => {
				if (config.index_on_start) {
					try {
						await limiter.schedule(async () => {
							const bar = multiBar.create(200, 0, {
								index_name: config.index_name,
								humanized_eta: 0,
								total_documents: 0,
								skipped: 0,
							});
							await this.indexCollection(config, bar);
						});
					} catch (err: any) {
						// Don't let an indexCollection failure prevent the change-stream
						// supervisor from opening — the cron path can backfill anything
						// that didn't get picked up during the initial pass, but the
						// supervisor must be running so real-time events flow.
						console.error(
							`indexAll: indexCollection failed for ${config.collection}: ${err?.message || err} — change stream will still be opened`,
						);
					}
				}
				this.handleChangeStream(config.collection, config.index_name, config.exclude_fields || []).catch(
					(err) =>
						console.error(
							`handleChangeStream supervisor exited unexpectedly for ${config.collection}: ${err?.message || err}`,
						),
				);
			}),
		);
		multiBar.stop();
	}

	/**
	 * Retrieves the most recent resume token for a specific collection and index.
	 * Used to resume change streams from the last processed point.
	 *
	 * @param collectionName - Name of the MongoDB collection
	 * @param index - Name of the Elasticsearch index
	 * @returns The most recent resume token document or undefined if none exists
	 */
	async getResumeToken(collectionName: string, index: string) {
		const response = await this.esClient.search({
			index: 'resume_tokens',
			query: {
				bool: {
					filter: [
						{
							term: {
								collection: collectionName,
							},
						},
						{
							term: {
								index,
							},
						},
					],
				},
			},
			// Sort by created desc only. A secondary sort on `_id` was attempted
			// but ES 7+ disables fielddata on `_id` by default and rejects such
			// queries with `illegal_argument_exception`. Once F8's cleanup script
			// runs there is exactly one row per (collection, index) so the
			// tiebreaker is moot anyway.
			sort: [{ created: 'desc' }],
			size: 1,
		});
		const token = response.hits.hits[0];
		return token;
	}

	/**
	 * Lists all existing Elasticsearch indices.
	 * @returns List of all indices in the Elasticsearch cluster
	 */
	async listAllIndices() {
		return this.esClient.cat.indices({ format: 'json' });
	}

	/**
	 * Creates a new Elasticsearch index with specified parameters.
	 * @param params - Index creation parameters including mappings and settings
	 * @returns Response from Elasticsearch index creation operation
	 */
	async createIndex(params: IndicesCreateRequest) {
		return this.esClient.indices.create(params);
	}

	/**
	 * Deletes an Elasticsearch index if it exists.
	 * Gracefully handles cases where the index doesn't exist.
	 * @param index - Name of the index to delete
	 */
	async deleteIndex(index: string) {
		try {
			await this.esClient.indices.delete({ index });
		} catch {
			console.warn(`Error deleting index ${index}`);
		}
	}

	/**
	 * Updates the mapping of an existing Elasticsearch index.
	 * @param params - Index mapping parameters
	 */
	async updateMapping(params: IndicesCreateRequest) {
		const mappingRequest: IndicesPutMappingRequest = {
			...params.mappings,
			index: params.index,
		};
		await this.esClient.indices.putMapping(mappingRequest);
	}

	/**
	 * Creates or updates an Elasticsearch index.
	 * If the index exists, updates its mapping; if not, creates a new index.
	 *
	 * Implementation:
	 * 1. Checks if the index exists
	 * 2. Updates mapping if index exists
	 * 3. Creates new index if it doesn't exist
	 *
	 * @param params - Index creation/update parameters
	 */
	async upsertIndex(params: IndicesCreateRequest) {
		try {
			const exists = await this.esClient.indices.exists({
				index: params.index,
			});
			if (exists) {
				console.log(`upsertIndex: index ${params.index} already exists, updating mapping`);
				return this.updateMapping(params);
			}
			console.log(`upsertIndex: index ${params.index} does not exist, creating`);
			return this.esClient.indices.create(params);
		} catch (error) {
			console.error(`upsertIndex: index ${params.index} error ${error}`);
		}
	}

	/**
	 * Performs bulk indexing of documents into Elasticsearch.
	 * Transforms documents using TransformService before indexing.
	 *
	 * @param index - Target Elasticsearch index name
	 * @param documents - Array of documents to index
	 * @returns Response from Elasticsearch bulk operation
	 */
	async bulkIndexDocuments(index: string, documents: any[]) {
		const bulkBody = await this.transformService.getBulkIndexBody(index, documents);
		const response = await this.esClient.bulk({
			index,
			body: bulkBody,
		});
		if (response.errors) {
			const failures = response.items
				.map((item: any) => item.index || item.create || item.update || item.delete)
				.filter((op: any) => op?.error)
				.slice(0, 5);
			console.error(
				`bulkIndexDocuments: ${index} had ${failures.length}+ item-level error(s) (showing up to 5): ${JSON.stringify(failures)}`,
			);
		}
		return response;
	}

	/**
	 * Indexes a single document from MongoDB to Elasticsearch.
	 * Updates the document's lastESIndexedAt timestamp in MongoDB.
	 *
	 * Implementation:
	 * 1. Retrieves the document from MongoDB
	 * 2. Indexes it in Elasticsearch
	 * 3. Updates indexing metadata in MongoDB
	 *
	 * @param collection - MongoDB collection name
	 * @param id - Document ID to index
	 */
	async indexOne(collection: string, id: string) {
		const configs = this.configs.filter((config) => config.collection === collection);
		if (configs.length === 0) {
			throw new Error(`indexOne: config for ${collection} not found`);
		}
		for (const config of configs) {
			try {
				const { pipeline, separateLookups } = this.extractService.processSeparateLookups(
					config.aggregation_pipeline,
				);
				const [document] = await this.extractService.getDocumentsWithNestedPagination(
					config.collection,
					[
						{
							$match: {
								_id: new ObjectId(id),
							},
						},
						...pipeline,
					],
					separateLookups.map((index) => index + 1),
				);
				if (!document) {
					throw new Error(`indexOne: document for ${collection} with id ${id} not found`);
				}
				const response = await this.bulkIndexDocuments(config.index_name, [document]);
				await this.extractService.updateOne(collection, id, {
					lastESIndexedAt: new Date(),
					lastESIndexResponse: response.items.find((item) => item.index._id === id.toString())?.index?.result,
				});
			} catch (error) {
				console.error(`indexOne: error indexing document for ${collection} with id ${id}: ${error}`);
				console.error(error);
			}
		}
	}

	/**
	 * Deletes a document from all relevant Elasticsearch indices.
	 * @param collection - MongoDB collection name
	 * @param id - Document ID to delete
	 */
	async deleteOne(collection: string, id: string) {
		const configs = this.configs.filter((config) => config.collection === collection);
		if (configs.length === 0) {
			throw new Error(`deleteOne: config for ${collection} not found`);
		}
		for (const config of configs) {
			await this.esClient.delete({
				index: config.index_name,
				id,
			});
		}
	}

	/**
	 * Indexes all documents from a collection based on configuration.
	 * Handles batch processing with progress tracking and error recovery.
	 *
	 * Implementation:
	 * 1. Calculates total documents and skipped count
	 * 2. Processes documents in batches
	 * 3. Handles BSON size limitations
	 * 4. Updates progress bar with ETA
	 * 5. Updates indexing metadata in MongoDB
	 *
	 * @param config - Configuration for the collection
	 * @param bar - Progress bar instance for visual feedback
	 */
	async indexCollection(config: Configuration, bar: cliProgress.SingleBar) {
		const skippedCount = await this.extractService.getIndexSkipCount(config);
		const totalDocuments = await this.extractService.getIndexCounts(config);
		const skippedPipeline = this.extractService.getSkippedPipeline(config);
		const { pipeline, separateLookups } = this.extractService.processSeparateLookups(skippedPipeline);
		let documents = [];

		bar.start(totalDocuments, 0, {
			humanized_eta: 0,
			index_name: config.index_name,
			total_documents: totalDocuments + skippedCount,
			skipped: skippedCount,
		});
		console.log(`indexCollection: ${config.index_name}`);

		let batchSize = config.batch_size;
		const startTime = new Date();

		for (let done = 0, completed = false; done < totalDocuments && !completed; ) {
			try {
				documents = await this.extractService.getDocumentsWithNestedPagination(
					config.collection,
					pipeline,
					separateLookups,
					batchSize,
				);
			} catch (error: any) {
				if (error.codeName === 'BSONObjectTooLarge') {
					batchSize = Math.floor(batchSize / 2);

					if (batchSize < 1) {
						console.log(`indexCollection: BSONObjectTooLarge, batch size is too small, skipping documents`);
						batchSize = 1;
						done += batchSize;
						bar.update(done);
						continue;
					}
					continue;
				}
				console.error(`indexCollection: error getting documents: ${error}`);
				done += config.batch_size;
				bar.update(done);
				continue;
			}
			if (batchSize < config.batch_size) {
				console.log(`indexCollection: resetting batch size to ${config.batch_size}`);
				batchSize = config.batch_size;
			}
			if (documents.length === 0) {
				console.log(`indexCollection: No more documents to index`);
				completed = true;
				continue;
			}
			const response = await this.bulkIndexDocuments(config.index_name, documents);
			await this.extractService.bulkUpdate(
				config.collection,
				documents.map((doc) => ({
					filter: { _id: new ObjectId((doc._id || doc.id) as string) },
					update: {
						lastESIndexedAt: new Date(),
						lastESIndexResponse: response.items.find(
							(item) => item.index._id === (doc._id || doc.id).toString(),
						)?.index?.result,
					},
				})),
			);
			done += documents.length;
			const timeElapsed = new Date().getTime() - startTime.getTime();
			const eta = (totalDocuments - done) * (timeElapsed / done);
			console.log(
				`indexCollection: ${config.index_name} done: ${done} eta: ${humanizeDuration(eta, { round: true })}`,
			);
			bar.update(done, { humanized_eta: humanizeDuration(eta, { round: true }) });
		}
		console.log(`indexCollection: ${config.index_name} doneAll totalDocuments: ${totalDocuments}`);
		bar.stop();
	}

	/**
	 * Records a change stream event in the resume_tokens index.
	 * Used to maintain the position in the change stream.
	 *
	 * @param collectionName - MongoDB collection name
	 * @param index - Elasticsearch index name
	 * @param oldToken - Previous resume token
	 * @param newToken - New resume token
	 */
	async acknowledgeChangeEvent(collectionName: string, index: string, oldToken: any, newToken: any) {
		// Stable _id per (collection, index): if oldToken came from getResumeToken
		// it has a real ES doc _id; otherwise mint one. After the first call we
		// stash the minted id back onto oldToken so subsequent calls keep writing
		// to the same row rather than minting a fresh row per event (the cause of
		// the 3.1M leftover resume_tokens rows accumulated under the old code).
		const tokenId = oldToken?._id ?? new ObjectId().toString();
		await this.bulkIndexDocuments('resume_tokens', [
			{
				_id: tokenId,
				token: newToken._id._data,
				collection: collectionName,
				index,
				created: new Date(),
			},
		]);
		if (oldToken && !oldToken._id) {
			oldToken._id = tokenId;
		}
	}

	/**
	 * Handles MongoDB change stream events for real-time synchronization.
	 * Processes insert, update, and delete operations.
	 *
	 * Implementation:
	 * 1. Retrieves the last resume token
	 * 2. Creates a change stream
	 * 3. Processes each change event
	 * 4. Skips events that only update indexing fields
	 * 5. Updates Elasticsearch accordingly
	 * 6. Records the new resume token
	 *
	 * @param collectionName - MongoDB collection name
	 * @param index - Elasticsearch index name
	 */
	/**
	 * Supervises a change-stream consumer for one collection. Reconnects on any
	 * error, including ChangeStreamHistoryLost (in which case the stale resume
	 * token is deleted so the next stream starts fresh from "now"). The cron
	 * fallback (`handleUpdatedDocuments`) covers any inserts/updates that occur
	 * during the gap; deletes during the gap are not recovered (documented in
	 * the rollout plan).
	 */
	async handleChangeStream(collectionName: string, index: string, excludeFields: string[] = []) {
		const BACKOFF_MS = 5000;
		// Loop forever — graceful shutdown is handled by the process lifecycle (pm2/SIGTERM).
		for (;;) {
			let changeStream: Awaited<ReturnType<ExtractService['getChangeStream']>> | undefined;
			try {
				const fetched = await this.getResumeToken(collectionName, index);
				const token = fetched?._source?.['token'];
				// Always pass a mutable holder so acknowledgeChangeEvent can stash a
				// minted _id back onto it and subsequent acknowledges reuse the same
				// resume_tokens row (avoids minting a fresh row per event).
				const resumeToken: { _id?: string } = fetched ? { _id: fetched._id as string } : {};
				console.log(
					`handleChangeStream: ${collectionName} ${index} starting (token: ${token ? 'present' : 'none'})`,
				);
				changeStream = await this.extractService.getChangeStream(collectionName, token);

				for await (const change of changeStream) {
					try {
						await this.processChangeEvent(collectionName, index, excludeFields, change, resumeToken);
					} catch (eventErr: any) {
						// Per-event failure must NOT kill the stream.
						console.error(
							`handleChangeStream event error (${collectionName} ${change?.operationType} ${change?.documentKey?._id}): ${eventErr?.message || eventErr}`,
						);
					}
				}
				// for-await ended without error (stream closed gracefully). Reconnect after backoff.
				console.warn(`handleChangeStream: ${collectionName} stream ended; reconnecting in ${BACKOFF_MS}ms`);
			} catch (streamErr: any) {
				const message = streamErr?.message || String(streamErr);
				const isHistoryLost =
					streamErr?.code === 286 || /ChangeStreamHistoryLost|resume of change stream/i.test(message);
				console.error(`handleChangeStream: ${collectionName} stream crashed: ${message}`);
				if (isHistoryLost) {
					console.error(
						`handleChangeStream: ${collectionName} history lost — discarding stale resume token and restarting from "now"`,
					);
					try {
						await this.discardResumeToken(collectionName, index);
					} catch (discardErr: any) {
						console.error(
							`handleChangeStream: ${collectionName} failed to discard resume token: ${discardErr?.message || discardErr}`,
						);
					}
				}
			} finally {
				if (changeStream) {
					try {
						await changeStream.close();
					} catch {
						/* ignore */
					}
				}
			}
			await new Promise((r) => setTimeout(r, BACKOFF_MS));
		}
	}

	/**
	 * Processes a single change-stream event. Extracted from handleChangeStream
	 * so the per-event error handling stays compact.
	 */
	private async processChangeEvent(
		collectionName: string,
		index: string,
		excludeFields: string[],
		change: any,
		resumeToken: any,
	) {
		const updatedFields = Object.keys(change?.updateDescription?.updatedFields || {});
		if (hasOnlyIndexingFields(updatedFields, excludeFields) && change.operationType === 'update') {
			await this.acknowledgeChangeEvent(collectionName, index, resumeToken, change);
			return;
		}

		console.log(`handleChangeStream: ${collectionName} ${index} ${change.operationType} ${change.documentKey._id}`);
		switch (change.operationType) {
			case 'insert':
			case 'update':
				await this.indexOne(collectionName, change.documentKey._id);
				break;
			case 'delete':
				await this.deleteOne(collectionName, change.documentKey._id);
				break;
		}
		await this.acknowledgeChangeEvent(collectionName, index, resumeToken, change);
	}

	/**
	 * Deletes all resume_tokens rows for a given (collection, index). Used to
	 * recover from ChangeStreamHistoryLost — the next stream open will then
	 * start from "now" rather than trying to resume a position that no longer
	 * exists in the oplog.
	 */
	async discardResumeToken(collectionName: string, index: string) {
		await this.esClient.deleteByQuery({
			index: 'resume_tokens',
			query: {
				bool: {
					filter: [{ term: { collection: collectionName } }, { term: { index } }],
				},
			},
			refresh: true,
		});
	}

	/**
	 * Handles new documents in a collection.
	 * Retrieves documents that have not been indexed yet and indexes them.
	 *
	 * @param collectionName - MongoDB collection name
	 * @param index - Elasticsearch index name
	 */

	async handleNewDocuments(collectionName: string, index: string) {
		const config = this.configs.find((c) => c.collection === collectionName);
		const batchSize = config?.new_batch_size ?? 100;
		const concurrency = config?.index_concurrency ?? 20;
		console.log(`handleNewDocuments: ${collectionName} ${index}`);
		try {
			const documents = await this.extractService.getDocuments(
				collectionName,
				[
					{
						$match: {
							lastESIndexedAt: { $exists: false },
						},
					},
					{
						$project: {
							_id: 1,
						},
					},
					// Newest-first: brand-new docs are the most likely to be user-visible
					// (someone just created them), so they index ahead of any old unindexed
					// stragglers that have been waiting around.
					{
						$sort: {
							_id: -1,
						},
					},
				],
				batchSize,
			);
			if (documents.length === 0) {
				console.log(`handleNewDocuments: ${collectionName} ${index} no documents to index`);
				return;
			}
			console.log(`handleNewDocuments: ${collectionName} ${index} ${documents.length} documents`);
			const limiter = new Bottleneck({ maxConcurrent: concurrency });
			await Promise.all(documents.map((doc) => limiter.schedule(() => this.indexOne(collectionName, doc._id))));
		} catch (error) {
			console.error(`handleNewDocuments: ${collectionName} ${index} ${error}`);
			console.error(error);
		}
	}

	async handleUpdatedDocuments(collectionName: string, index: string, updateField: string = 'updated') {
		const config = this.configs.find((c) => c.collection === collectionName);
		const batchSize = config?.update_batch_size ?? 500;
		const concurrency = config?.index_concurrency ?? 20;
		console.log(`handleUpdatedDocuments: ${collectionName} ${index}`);
		try {
			const documents = await this.extractService.getDocuments(
				collectionName,
				[
					{
						$match: {
							$expr: {
								$gte: [`$${updateField}`, '$lastESIndexedAt'],
							},
						},
					},
					{
						$project: {
							_id: 1,
						},
					},
					// FIFO over staleness: oldest stale first. The previous sort by _id
					// ASC caused newest-doc starvation — a re-bumped 2019 doc (small _id)
					// would jump in front of a freshly-published 2026 doc (large _id).
					// Sorting by the update field instead makes queue position depend on
					// when the doc became stale, not when it was originally created.
					{
						$sort: {
							[updateField]: 1,
						},
					},
				],
				batchSize,
			);
			if (documents.length === 0) {
				console.log(`handleUpdatedDocuments: ${collectionName} ${index} no documents to index`);
				return;
			}
			console.log(`handleUpdatedDocuments: ${collectionName} ${index} ${documents.length} documents`);
			const limiter = new Bottleneck({ maxConcurrent: concurrency });
			await Promise.all(documents.map((doc) => limiter.schedule(() => this.indexOne(collectionName, doc._id))));
		} catch (error) {
			console.error(`handleUpdatedDocuments: ${collectionName} ${index} ${error}`);
			console.error(error);
		}
	}
}
