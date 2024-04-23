import type {
	INodeType,
	INodeTypeDescription,
	ITriggerFunctions,
	ITriggerResponse,
} from 'n8n-workflow';

import snowflake from 'snowflake-sdk';
import { connect, destroy, getStream } from './GenericFunctions';

export class SnowflakeTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Snowflake Trigger',
		name: 'snowflakeTrigger',
		icon: 'file:snowflake.svg',
		group: ['trigger'],
		version: 1,
		description: 'Stream data from Snowflake',
		defaults: {
			name: 'Snowflake Trigger',
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
		],
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse | undefined> {
		const credentials = (await this.getCredentials(
			'snowflake',
		)) as unknown as snowflake.ConnectionOptions;

		const query = this.getNodeParameter('query') as string;

		const connection = snowflake.createConnection(credentials);
		await connect(connection);
		const startTrigger = async () => {
			const statement = await getStream(connection, query, []);

			const dataStream = statement?.streamRows();

			dataStream?.on('error', (err: Error) => {
				throw err;
			});

			dataStream?.on('finish', () => {
				console.log('Finished stream');
			});

			dataStream?.on('data', (row) => {
				console.log('row', row.C_CUSTKEY);
				this.emit([this.helpers.returnJsonArray([row])]);
			});
		};

		await startTrigger();

		const closeFunction = async () => {
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
