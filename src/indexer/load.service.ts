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
					{ lastIndexedAt: null, lastIndexedResponse: null },
				);
			}

			await this.upsertIndex({ ...config.index_params, index: config.index_name } as IndicesCreateRequest);
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
					await limiter.schedule(async () => {
						const bar = multiBar.create(200, 0, {
							index_name: config.index_name,
							humanized_eta: 0,
							total_documents: 0,
							skipped: 0,
						});
						await this.indexCollection(config, bar);
					});
				}
				this.handleChangeStream(config.collection, config.index_name);
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
			sort: {
				created: 'desc',
			},
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
		return response;
	}

	/**
	 * Indexes a single document from MongoDB to Elasticsearch.
	 * Updates the document's lastIndexedAt timestamp in MongoDB.
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
			const [document] = await this.extractService.getDocuments(config.collection, [
				{
					$match: {
						_id: new ObjectId(id),
					},
				},
				...config.aggregation_pipeline,
			]);
			if (!document) {
				throw new Error(`indexOne: document for ${collection} with id ${id} not found`);
			}
			const response = await this.bulkIndexDocuments(config.index_name, [document]);
			await this.extractService.updateOne(collection, id, {
				lastIndexedAt: new Date(),
				lastIndexedResponse: response.items.find((item) => item.index._id === id.toString())?.index?.result,
			});
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
		const { pipeline, separateLookups } = this.extractService.processSeparateLookups(config);
		let documents = [];

		bar.start(totalDocuments, 0, {
			humanized_eta: 0,
			index_name: config.index_name,
			total_documents: totalDocuments + skippedCount,
			skipped: skippedCount,
		});
		let batchSize = config.batch_size;
		const startTime = new Date();

		for (let done = 0, completed = false; done < totalDocuments && !completed; ) {
			try {
				documents =
					batchSize <= 5
						? await this.extractService.getDocumentsWithNestedPagination(
								config.collection,
								pipeline,
								separateLookups,
							)
						: await this.extractService.getDocuments(config.collection, pipeline, batchSize);
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
			// Preserve document IDs before they are mutated by fixIds
			const documentIds = documents.map((doc) => (doc._id || doc.id).toString());
			const response = await this.bulkIndexDocuments(config.index_name, documents);
			await this.extractService.bulkUpdate(
				config.collection,
				documentIds.map((id) => ({
					filter: { _id: new ObjectId(id) },
					update: {
						lastIndexedAt: new Date(),
						lastIndexedResponse: response.items.find(
							(item) => item.index._id === id,
						)?.index?.result,
					},
				})),
			);
			done += documents.length;
			const timeElapsed = new Date().getTime() - startTime.getTime();
			const eta = (totalDocuments - done) * (timeElapsed / done);
			bar.update(done, { humanized_eta: humanizeDuration(eta, { round: true }) });
		}
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
		await this.bulkIndexDocuments('resume_tokens', [
			{
				_id: oldToken ? oldToken._id : new ObjectId(),
				token: newToken._id._data,
				collection: collectionName,
				index,
				created: new Date(),
			},
		]);
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
	async handleChangeStream(collectionName: string, index: string) {
		const resumeToken = await this.getResumeToken(collectionName, index);
		const changeStream = await this.extractService.getChangeStream(collectionName, resumeToken?._source?.['token']);
		for await (const change of changeStream) {
			const updatedFields = Object.keys(change?.updateDescription?.updatedFields || {});
			if (hasOnlyIndexingFields(updatedFields) && change.operationType !== 'delete') {
				await this.acknowledgeChangeEvent(collectionName, index, resumeToken, change);
				continue;
			}

			console.log(`handleChangeStream: ${change.operationType} ${change.documentKey._id}`);
			switch (change.operationType) {
				case 'insert':
					await this.indexOne(collectionName, change.documentKey._id);
					break;
				case 'update':
					await this.indexOne(collectionName, change.documentKey._id);
					break;
				case 'delete':
					await this.deleteOne(collectionName, change.documentKey._id);
					break;
			}
			await this.acknowledgeChangeEvent(collectionName, index, resumeToken, change);
		}
	}
}
