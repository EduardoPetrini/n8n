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

import { connect, getGenerator, getNextItem } from './GenericFunctions';

export class SnowflakeLoop implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Snowflake Loop',
		name: 'snowflakeLoop',
		icon: 'file:snowflake.svg',
		group: ['input'],
		version: 1,
		description: 'Split Snowflake data into batches and iterate over each batch',
		defaults: {
			name: 'Snowflake Loop',
		},
		inputs: ['main'],
		outputs: ['main'],
		outputNames: ['row'],
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

			// ----------------------------------
			//         executeQuery
			// ----------------------------------
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
		const items = [{}] as unknown as INodeExecutionData[];

		const nodeContext = this.getContext('node');

		const returnItems: INodeExecutionData[] = [];

		if (nodeContext.items === undefined) {
			// Is the first time the node runs
			const sourceData = this.getInputSourceData();

			nodeContext.currentRunIndex = 0;
			nodeContext.maxRunIndex = 1;
			nodeContext.sourceData = deepCopy(sourceData);

			const connection = snowflake.createConnection(credentials);
			await connect(connection);

      const generator = getGenerator(connection, query);

			// Get the items which should be returned
			const newItem = await generator.next();
      const value = newItem.value;

			returnItems.push.apply(returnItems, [value]);

			// Save the incoming items to be able to return them for later runs
			nodeContext.items = [value];

			// Reset processedItems as they get only added starting from the first iteration
			nodeContext.processedItems = [value];
      nodeContext.generator = generator;
		} else {
			// The node has been called before. So return the next batch of items.
			nodeContext.currentRunIndex += 1;

			const newItem = await nodeContext.generator.next();
      const value = newItem.value;

			returnItems.push.apply(returnItems, [value]);

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

			const sourceOverwrite = this.getInputSourceData();

			const newItems = items.map((item, index) => {
				return {
					...item,
					pairedItem: {
						sourceOverwrite,
						item: index,
					},
				};
			});

			nodeContext.processedItems = [...nodeContext.processedItems, ...newItems];

			returnItems.map((item) => {
				item.pairedItem = getPairedItemInformation(item);
			});
		}

		nodeContext.noItemsLeft = nodeContext.items.length === 0;

		if (returnItems.length === 0) {
			nodeContext.done = true;
			return [nodeContext.processedItems];
		}

		nodeContext.done = false;

		return [returnItems];
	}
}
