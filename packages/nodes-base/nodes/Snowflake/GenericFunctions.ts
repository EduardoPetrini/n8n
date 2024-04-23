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
			complete: (error, stmt, rows) => (error ? reject(error) : resolve(rows)),
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
