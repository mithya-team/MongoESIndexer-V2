import * as fs from 'fs';
import * as path from 'path';

describe('LtdDoc index configuration', () => {
	it('indicates whether Lexsitus references exist without exposing them', () => {
		const configPath = path.resolve(process.cwd(), 'configs/ltd_doc.settings.json');
		const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
		const projection = [...config.aggregation_pipeline].reverse().find((stage) => stage.$project)?.$project;

		expect(projection?.lexsitusReferences).toBeUndefined();
		expect(projection?.hasLexsitusReferences).toEqual({
			$cond: [{ $isArray: '$lexsitusReferences' }, { $gt: [{ $size: '$lexsitusReferences' }, 0] }, false],
		});
		expect(config.index_params.mappings.properties.lexsitusReferences).toBeUndefined();
		expect(config.index_params.mappings.properties.hasLexsitusReferences).toEqual({
			type: 'boolean',
		});
	});
});
