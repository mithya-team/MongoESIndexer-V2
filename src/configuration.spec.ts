import * as fs from 'fs';
import * as path from 'path';

describe('LtdDoc index configuration', () => {
	it('includes Lexsitus references in search result sources', () => {
		const configPath = path.resolve(process.cwd(), 'configs/ltd_doc.settings.json');
		const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
		const projection = [...config.aggregation_pipeline].reverse().find((stage) => stage.$project)?.$project;

		expect(projection?.lexsitusReferences).toBe(1);
		expect(config.index_params.mappings.properties.lexsitusReferences).toEqual({
			type: 'object',
			enabled: false,
		});
	});
});
