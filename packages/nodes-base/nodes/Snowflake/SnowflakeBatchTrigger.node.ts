import EventEmitter from 'events';
import type {
	IDataObject,
	INodeType,
	INodeTypeDescription,
	ITriggerFunctions,
	ITriggerResponse,
	IRun,
} from 'n8n-workflow';

import { createDeferredPromise } from 'n8n-workflow';

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
				default: '',
				placeholder: 'SELECT id, name FROM product WHERE id < 40',
				required: true,
				description: 'The SQL query to execute',
			},
			{
				displayName: 'Main Table',
				name: 'table',
				type: 'string',
				default: 'schema.table_1',
				placeholder: 'The main table name',
				required: true,
				description: 'The SQL query main to count',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				typeOptions: {
					minValue: 1,
				},
				default: 1000,
				placeholder: '1000',
				required: true,
				description: 'Max number of results to return',
			},
			{
				displayName: 'Execution Interval in Minutes',
				name: 'minutes',
				type: 'number',
				default: 60,
				typeOptions: {
					minValue: 1,
				},
				placeholder: '60',
				required: true,
				description: 'The SQL query interval to execute',
			},
			{
				displayName: 'Interval between batches in seconds',
				name: 'batchInterval',
				type: 'number',
				default: 60,
				typeOptions: {
					minValue: 1,
				},
				placeholder: '60',
				required: true,
				description:
					'The SQL query interval to execute between batches to work around the backpressure',
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
		const minutes = this.getNodeParameter('minutes') as number;
		const batchInterval = this.getNodeParameter('batchInterval') as number;

		const connection = snowflake.createConnection(credentials);
		await connect(connection);
		const event = new EventEmitter();
		let isRunning = false;

		const startTrigger = async () => {
			console.log('Starting trigger', new Date().toLocaleString());
			if (isRunning) {
				console.log('It is already running, what should I do?', isRunning);
			}

			const offset = 0;
			isRunning = true;
			const countQuery = `SELECT COUNT(*) as count FROM ${table};`;

			const result = await execute(connection, countQuery, []);
			const count = result?.[0].COUNT;
			console.log('rows count', count);
			this.logger.info(`rows count: ${count}`);
			if (!count) {
				this.logger.warn(`No data found in the source table: ${table}, count: ${count}`);
				return;
			}

			event.on('queryAndPush', async (newOffset) => {
				const limitQuery = `${query} LIMIT ${limit} OFFSET ${newOffset}`;
				console.log(limitQuery, ' | is running', isRunning);
        this.logger.info(`${limitQuery}`);

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

				const interval = 1000 * batchInterval;
				setTimeout(() => event.emit('queryAndPush', newOffset), interval);
			});

			event.on('done', (msg) => {
				isRunning = false;
				console.log('Events loop completed', msg);
			});

			event.emit('queryAndPush', offset);
		};

		await startTrigger();

		const interval = 60 * 1000 * minutes;
		setInterval(async () => {
			await startTrigger();
		}, interval);

		const closeFunction = async () => {
			event.emit('done', 'killed');
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
