
const { BaseDriver } = require("@cubejs-backend/query-orchestrator")
const genericPool = require('generic-pool');
const { promisify } = require('util');
const mysql = require('mysql');
const MyQuery = require("./MyQuery");
class MyDriver extends BaseDriver {
    constructor(config) {
        super();
        const { pool, ...restConfig } = config || {};
        this.config = {
            host: process.env.CUBEJS_DB_HOST,
            database: process.env.CUBEJS_DB_NAME,
            port: process.env.CUBEJS_DB_PORT,
            user: process.env.CUBEJS_DB_USER,
            password: process.env.CUBEJS_DB_PASS,
            socketPath: process.env.CUBEJS_DB_SOCKET_PATH,
            timezone: 'Z',
            ssl: this.getSslOptions(),
            dateStrings: true,
            ...restConfig,
        };

        this.pool = genericPool.createPool({
            create: async () => {
                const conn = mysql.createConnection(this.config);
                const connect = promisify(conn.connect.bind(conn));

                if (conn.on) {
                    conn.on('error', () => {
                        conn.destroy();
                    });
                }
                conn.execute = promisify(conn.query.bind(conn));

                await connect();

                return conn;
            },
            validate: async (connection) => {
                try {
                    await connection.execute('SELECT 1');
                } catch (e) {
                    this.databasePoolError(e);
                    return false;
                }
                return true;
            },
            destroy: (connection) => promisify(connection.end.bind(connection))(),
        }, {
            min: 0,
            max: process.env.CUBEJS_DB_MAX_POOL && parseInt(process.env.CUBEJS_DB_MAX_POOL, 10) || 8,
            evictionRunIntervalMillis: 10000,
            softIdleTimeoutMillis: 30000,
            idleTimeoutMillis: 30000,
            testOnBorrow: true,
            acquireTimeoutMillis: 20000,
            ...pool
        });
    }
    static dialectClass() {
        return MyQuery;
    }
    withConnection(fn) {
        const self = this;
        const connectionPromise = this.pool.acquire();

        let cancelled = false;
        const cancelObj = {};
        const promise = connectionPromise.then(async conn => {
            const [{ connectionId }] = await conn.execute('select connection_id() as connectionId');
            cancelObj.cancel = async () => {
                cancelled = true;
                await self.withConnection(async processConnection => {
                    await processConnection.execute(`KILL ${connectionId}`);
                });
            };
            return fn(conn)
                .then(res => this.pool.release(conn).then(() => {
                    if (cancelled) {
                        throw new Error('Query cancelled');
                    }
                    return res;
                }))
                .catch((err) => this.pool.release(conn).then(() => {
                    if (cancelled) {
                        throw new Error('Query cancelled');
                    }
                    throw err;
                }));
        });
        promise.cancel = () => cancelObj.cancel();
        return promise;
    }
    async testConnection() {
        const conn = await this.pool._factory.create();
        try {
            return await conn.execute('SELECT 1');
        } finally {
            await this.pool._factory.destroy(conn);
        }
    }
    query(query, values) {
        return this.withConnection(db => this.setTimeZone(db)
            .then(() => db.execute(query, values))
            .then(res => res));
    }
    setTimeZone(db) {
        return db.execute(`SET time_zone = '${this.config.storeTimezone || '+00:00'}'`, []);
    }

}
module.exports = MyDriver