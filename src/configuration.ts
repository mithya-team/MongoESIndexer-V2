import { z } from 'zod';

export const ConfigurationSchema = z.object({
	collection: z.string().describe('The collection to index'),
	index_name: z.string().describe('The index to use'),
	batch_size: z.number().describe('The batch size to use'),
	index_on_start: z.boolean().describe('Whether to index the collection on start'),
	force_delete: z.boolean().describe('Whether to delete the index before indexing'),
	skip_after_seconds: z
		.number()
		.default(0)
		.describe(
			'The number of seconds to skip after indexing. Default is 0. ' +
				'This is used to avoid indexing the same documents twice. ' +
				'This is the time after which if the document is indexed, it will not be indexed again.',
		),
	aggregation_pipeline: z.array(z.any()).describe('The aggregation pipeline to use'),
	index_params: z.object({
		settings: z.record(z.any()).describe('The settings to use'),
		mappings: z.record(z.any()).describe('The mappings to use'),
	}),
	update_field: z.string().optional().describe('The date field to use for updated documents'),
	exclude_fields: z.array(z.string()).optional().describe('The fields to exclude from indexing'),
	new_batch_size: z
		.number()
		.default(100)
		.describe(
			'Number of brand-new documents (no lastESIndexedAt) to process per cron tick. ' +
				'Was hard-coded to 20.',
		),
	update_batch_size: z
		.number()
		.default(500)
		.describe(
			'Number of stale documents (updated >= lastESIndexedAt) to process per cron tick. ' +
				'Was hard-coded to 50.',
		),
	index_concurrency: z
		.number()
		.default(20)
		.describe(
			'Maximum concurrent indexOne() calls per cron tick. Caps fan-out into Mongo aggregation ' +
				'and ES bulk pipelines so larger batch sizes do not overwhelm downstream services.',
		),
});

export type Configuration = z.infer<typeof ConfigurationSchema>;
