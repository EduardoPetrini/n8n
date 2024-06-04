import { INodeExecutionData } from 'n8n-workflow';
import type snowflake from 'snowflake-sdk';

export async function connect(conn: snowflake.Connection) {
	return await new Promise<void>((resolve, reject) => {
		conn.connect((error) => (error ? reject(error) : resolve()));
	});
}

export async function destroy(conn: snowflake.Connection) {
	return await new Promise<void>((resolve, reject) => {
		conn.destroy((error) => (error ? reject(error) : resolve()));
	});
}

export async function execute(
	conn: snowflake.Connection,
	sqlText: string,
	binds: snowflake.InsertBinds,
) {
	return await new Promise<any[] | undefined>((resolve, reject) => {
		conn.execute({
			sqlText,
			binds,
			complete: (error, _, rows) => (error ? reject(error) : resolve(rows)),
		});
	});
}

export async function getStream(
	conn: snowflake.Connection,
	sqlText: string,
	binds: snowflake.InsertBinds,
) {
	return await new Promise<snowflake.Statement>((resolve, reject) => {
		conn.execute({
			sqlText,
			binds,
			streamResult: true,
			complete: (error, stmt) => (error ? reject(error) : resolve(stmt)),
		});
	});
}

export async function getNextItem(): Promise<INodeExecutionData> {
	await new Promise((resolve) => setTimeout(resolve, 200));

	const num = Math.round(Math.random() * 1000);

	const row = {
		json: { id: num, name: `My name is ${num}`, date: new Date() },
		pairedItem: {
			item: 0,
			sourceOverwrite: undefined,
		},
	} as unknown as INodeExecutionData;

	return row;
}

export async function* getGenerator(
	connection: snowflake.Connection,
	query: string,
): AsyncGenerator<INodeExecutionData, any, unknown> {
	const statement: snowflake.Statement = await new Promise((resolve, reject) => {
		connection.execute({
			sqlText: query,
			streamResult: true,
			complete: (err, stmt) => (err ? reject(err) : resolve(stmt)),
		});
	});

	const dataStream = statement.streamRows();

	let done = false;
	const finish = new Promise((resolve) =>
		dataStream.once('end', () => {
			const returnData = {
				json: null,
				pairedItem: {
					item: 0,
					sourceOverwrite: undefined,
				},
			} as unknown as INodeExecutionData;
			resolve(returnData);
			done = true;
		}),
	);

	while (!done) {
		const promise = new Promise((resolve) => {
			dataStream.once('data', (row) => {
				const returnData = {
					json: row,
					pairedItem: {
						item: 0,
						sourceOverwrite: undefined,
					},
				} as unknown as INodeExecutionData;
				resolve(returnData);
				dataStream.pause();
			});
			dataStream.resume();
		});

		yield (await Promise.race([promise, finish])) as unknown as INodeExecutionData;
	}
}

export async function* getGeneratorBatch(
	connection: snowflake.Connection,
	query: string,
): AsyncGenerator<INodeExecutionData, any, unknown> {
	const statement: snowflake.Statement = await new Promise((resolve, reject) => {
		connection.execute({
			sqlText: query,
			streamResult: true,
			complete: (err, stmt) => (err ? reject(err) : resolve(stmt)),
		});
	});

	const totalRows = statement.getNumRows();

	let start = 0;
	let end = start + 1;
	while (end < totalRows) {
		const dataStream = statement.streamRows({
			start,
			end,
		});

		const promise = new Promise((resolve) => {
			dataStream.once('data', (row) => {
				const returnData = {
					json: row,
					pairedItem: {
						item: 0,
						sourceOverwrite: undefined,
					},
				} as unknown as INodeExecutionData;
				resolve(returnData);
				dataStream.pause();
			});
			dataStream.resume();
		});

		start = end;
		end = end + 1;
		yield (await Promise.resolve(promise)) as INodeExecutionData;

    // if (end >= totalRows) {
    //   const returnData = {
    //     json: null,
    //     pairedItem: {
    //       item: 0,
    //       sourceOverwrite: undefined,
    //     },
    //   } as unknown as INodeExecutionData;

    //   yield returnData
    // }
	}
}
