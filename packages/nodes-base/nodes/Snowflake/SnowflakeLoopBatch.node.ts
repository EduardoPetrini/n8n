/* eslint-disable n8n-nodes-base/node-filename-against-convention */
import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IPairedItemData,
} from 'n8n-workflow';
import { deepCopy } from 'n8n-workflow';

import snowflake from 'snowflake-sdk';

import { connect, getGeneratorBatch } from './GenericFunctions';

export class SnowflakeLoopBatch implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Snowflake LoopBatch',
		name: 'snowflakeLoopBatch',
		icon: 'file:snowflake.svg',
		group: ['input'],
		version: 1,
		description: 'Split Snowflake data into batches and iterate over each batch',
		defaults: {
			name: 'Snowflake LoopBatch',
		},
		inputs: ['main'],
		// eslint-disable-next-line n8n-nodes-base/node-class-description-outputs-wrong
		outputs: ['main', 'main'],
		outputNames: ['row', 'done'],
		credentials: [
			{
				name: 'snowflake',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Execute Query',
						value: 'executeQuery',
						description: 'Execute an SQL query',
						action: 'Execute a SQL query',
					},
				],
				default: 'executeQuery',
			},
			{
				displayName: 'Batch Size',
				name: 'batchSize',
				type: 'number',
				noDataExpression: true,
				default: 1000,
				required: true,
				description: 'The number of rows to read and keep cached',
			},
			{
				displayName: 'Query',
				name: 'query',
				type: 'string',
				noDataExpression: true,
				typeOptions: {
					editor: 'sqlEditor',
				},
				default: '',
				placeholder: 'SELECT id, name FROM product WHERE id < 40',
				required: true,
				description: 'The SQL query to execute',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][] | null> {
		const credentials = (await this.getCredentials(
			'snowflake',
		)) as unknown as snowflake.ConnectionOptions;

		const query = this.getNodeParameter('query', 0) as string;
		const batchSize = this.getNodeParameter('batchSize', 0) as number;

		const nodeContext = this.getContext('node');

		const returnItems: INodeExecutionData[] = [];
		let completed = false;

		if (nodeContext.isNotFirst === undefined) {
			// Is the first time the node runs
			const sourceData = this.getInputSourceData();

			nodeContext.currentRunIndex = 0;
			nodeContext.maxRunIndex = 1;
			nodeContext.sourceData = deepCopy(sourceData);

			const connection = snowflake.createConnection(credentials);
			await connect(connection);

			const generator = getGeneratorBatch(connection, query, batchSize);

			// Get the items which should be returned
			const itemNext = await generator.next();
			nodeContext.batchItems = itemNext.value.reverse();
			// returnItems.push.apply(returnItems, [value]);

			// Save the incoming items to be able to return them for later runs
			nodeContext.isNotFirst = true;

			// Reset processedItems as they get only added starting from the first iteration
			nodeContext.generator = generator;
		}

		if (nodeContext.batchItems.length === 0) {
			// The node has been called before. So return the next batch of items.
			nodeContext.currentRunIndex += 1;

			const itemNext = await nodeContext.generator.next();
			completed = true;
			if (!itemNext.done && itemNext.value) {
				completed = false;
				nodeContext.batchItems = itemNext.value.reverse();
			}
		}

		const addSourceOverwrite = (pairedItem: IPairedItemData | number): IPairedItemData => {
			if (typeof pairedItem === 'number') {
				return {
					item: pairedItem,
					sourceOverwrite: nodeContext.sourceData,
				};
			}

			return {
				...pairedItem,
				sourceOverwrite: nodeContext.sourceData,
			};
		};

		function getPairedItemInformation(
			item: INodeExecutionData,
		): IPairedItemData | IPairedItemData[] {
			if (item.pairedItem === undefined) {
				return {
					item: 0,
					sourceOverwrite: nodeContext.sourceData,
				};
			}

			if (Array.isArray(item.pairedItem)) {
				return item.pairedItem.map(addSourceOverwrite);
			}

			return addSourceOverwrite(item.pairedItem);
		}

		returnItems.map((item) => {
			item.pairedItem = getPairedItemInformation(item);
		});
		// }

		const value = nodeContext.batchItems.pop();
		returnItems.push.apply(returnItems, [value]);

		if (returnItems.length === 0 || completed) {
			this.logger.info('Snowflake loop batch completed!');
			nodeContext.done = true;
			return [[], []];
		}

		nodeContext.done = false;

		return [returnItems, []];
	}
}
