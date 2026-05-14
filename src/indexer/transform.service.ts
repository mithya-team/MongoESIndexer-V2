import { Injectable } from '@nestjs/common';
import { ObjectId } from 'mongodb';

/**
 * Service responsible for transforming MongoDB documents into Elasticsearch bulk index format.
 * Handles document ID normalization and prepares documents for bulk indexing operations.
 * Provides utilities for document transformation and Elasticsearch bulk indexing preparation.
 */
@Injectable()
export class TransformService {
	/**
	 * Recursively processes a document to normalize IDs and convert ObjectIds to strings.
	 *
	 * Implementation:
	 * 1. Handles null/undefined values
	 * 2. Processes arrays by recursively fixing each element
	 * 3. Converts ObjectId instances to strings
	 * 4. Normalizes _id field to id field
	 * 5. Recursively processes nested objects
	 *
	 * NOTE: this still mutates the input doc (deletes _id, replaces ObjectIds
	 * with their string form in place). Callers must capture any _id they need
	 * to read BEFORE invoking fixIds (see getBulkIndexBody below).
	 *
	 * @param doc - The document or value to process
	 * @returns The processed document with normalized IDs and string ObjectIds
	 */
	fixIds(doc: any) {
		if (!doc) return doc;
		if (typeof doc !== 'object') return doc;
		if (Array.isArray(doc)) {
			doc.forEach((item) => this.fixIds(item));
			return doc;
		}
		if (doc instanceof ObjectId) {
			return doc.toString();
		}
		if (doc._id) {
			doc.id = doc._id;
			delete doc._id;
		}
		for (const key in doc) {
			doc[key] = this.fixIds(doc[key]);
		}
		return doc;
	}

	/**
	 * Transforms an array of MongoDB documents into Elasticsearch bulk index format.
	 *
	 * Implementation:
	 * 1. Snapshots each document's _id/id BEFORE fixIds runs (fixIds mutates the
	 *    input and deletes _id, so reading document._id afterwards is unsafe).
	 * 2. Processes each document using fixIds to normalize IDs and convert ObjectIds.
	 * 3. Creates Elasticsearch bulk index format with:
	 *    - Index metadata line containing index name and the snapshotted document ID
	 *    - Document data line containing the processed document
	 *
	 * @param index - The Elasticsearch index name
	 * @param documents - Array of MongoDB documents to transform
	 * @returns Array of objects in Elasticsearch bulk index format, alternating between metadata and document lines
	 */
	async getBulkIndexBody(index: string, documents: any[]) {
		const ids = documents.map((doc) => (doc?._id ?? doc?.id)?.toString());
		documents.forEach((doc) => this.fixIds(doc));
		return documents.flatMap((document, i) => [
			{
				index: {
					_index: index,
					_id: ids[i],
				},
			},
			document,
		]);
	}
}
