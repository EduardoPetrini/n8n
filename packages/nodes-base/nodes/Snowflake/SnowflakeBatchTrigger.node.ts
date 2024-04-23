import EventEmitter from 'events';
import type {
	IDataObject,
	INodeType,
	INodeTypeDescription,
	ITriggerFunctions,
	ITriggerResponse,
	IRun,
} from 'n8n-workflow';

import { createDeferredPromise, NodeOperationError } from 'n8n-workflow';

import snowflake from 'snowflake-sdk';
import { connect, destroy, execute } from './GenericFunctions';

export class SnowflakeBatchTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'SnowflakeBatch Trigger',
		name: 'snowflakeBatchTrigger',
		icon: 'file:snowflake.svg',
		group: ['trigger'],
		version: 1,
		description: 'Stream data from Snowflake',
		defaults: {
			name: 'SnowflakeBatch Trigger',
		},
		inputs: [],
		outputs: ['main'],
		parameterPane: 'wide',
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
				displayOptions: {
					show: {
						operation: ['executeQuery'],
					},
				},
				default: '',
				placeholder: 'SELECT id, name FROM product WHERE id < 40',
				required: true,
				description: 'The SQL query to execute',
			},
			{
				displayName: 'Main Table',
				name: 'table',
				type: 'string',
				noDataExpression: true,
				displayOptions: {
					show: {
						operation: ['executeQuery'],
					},
				},
				default: 'table_1',
				placeholder: 'The main table name',
				required: true,
				description: 'The SQL query main to count',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				noDataExpression: true,
				displayOptions: {
					show: {
						operation: ['executeQuery'],
					},
				},
				default: 1000,
				placeholder: '1000',
				required: true,
				description: 'The SQL query limit to execute',
			},
		],
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse | undefined> {
		const credentials = (await this.getCredentials(
			'snowflake',
		)) as unknown as snowflake.ConnectionOptions;

		const query = this.getNodeParameter('query') as string;
		const limit = this.getNodeParameter('limit') as number;
		const table = this.getNodeParameter('table') as number;

		const connection = snowflake.createConnection(credentials);
		await connect(connection);
		const event = new EventEmitter();
		const startTrigger = async () => {
			const offset = 0;

			const countQuery = `SELECT COUNT(*) as count FROM ${table};`;

			const result = await execute(connection, countQuery, []);
			const count = result?.[0].COUNT;
			console.log('rows count', count);

			event.on('queryAndPush', async (newOffset) => {
				const limitQuery = `${query} LIMIT ${limit} OFFSET ${newOffset}`;
				console.log(limitQuery);

				const rows = await execute(connection, limitQuery, []);
				console.log('result', rows?.length);

				const responsePromise = await createDeferredPromise<IRun>();
				const jsonArray = this.helpers.returnJsonArray(rows as IDataObject[]);

				this.emit([this.helpers.returnJsonArray(jsonArray)], undefined, responsePromise);

				event.emit('increment', newOffset);
			});

			event.on('increment', (newOffset) => {
				console.log('Incrementing...', newOffset, '+', limit);
				newOffset = newOffset + limit;

				if (newOffset >= count) {
					return event.emit('done', 'done');
				}

				event.emit('queryAndPush', newOffset);
			});

			event.on('done', (msg) => {
				console.log('Events loop completed', msg);
			});

			event.emit('queryAndPush', offset);
		};

		await startTrigger();

		const closeFunction = async () => {
      event.emit('done', 'killed')
			event.removeAllListeners();
			await destroy(connection);
		};

		const manualTriggerFunction = async () => {
			await startTrigger();
		};

		return {
			closeFunction,
			manualTriggerFunction,
		};
	}
}
