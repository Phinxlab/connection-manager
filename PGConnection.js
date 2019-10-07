const Global = require('@phinxlab/config-manager');
const Log = new (require('@phinxlab/log-manager'))('PGConnection');
const {Client, Pool} = require('pg');

class PGConnection {

    static registerConfig(cfg) {
        Global.addConfig("PG_CONNECTION", cfg)
    }

    constructor() {
        this.adujstTypes();
    }

    adujstTypes() {
        // 0700,701,1021,1022,1231,1700
        var types = require('pg').types;

        function toNumber(val) {
            return parseFloat(val);
        };

        types.setTypeParser(700, toNumber);
        types.setTypeParser(701, toNumber);
        types.setTypeParser(1021, toNumber);
        types.setTypeParser(1022, toNumber);
        types.setTypeParser(1231, toNumber);
        types.setTypeParser(1700, toNumber);
        types.setTypeParser(20, function(val) {
            return parseInt(val)
        })

    }

    async tx(callback) {
        const pool = new Pool(Global.PG_CONNECTION);
        const client = await pool.connect();
        try {
            Log.info('Connection to database');
            Log.info('Begining a new transaction');
            await client.query('BEGIN');
            try {
                Log.info('Waiting for the operations to be done');
                client.execute=client.query.bind(client);
                const response = await callback(client);
                Log.info('Commiting the operations: ', response);
                await client.query('COMMIT');
                return response;
            } catch (e) {
                Log.error('Failed to execute query');
                Log.error(e);
                await client.query('ROLLBACK');
                const message = e.message?e.message:e.toString();
                throw new Error(message,message);
            }
        } catch (e) {
            Log.error('Failed to begin transactiom');
            Log.error(e);
            const message = e.message?e.message:e.toString();
            throw new Error(message,message);
        } finally {
            Log.info('Killing connection tx');
            client.release();
        }
    }

    async execute(query, params = []) {
        const client = new Client(Global.PG_CONNECTION);
        try {
            await client.connect();
            const res = await client.query(query, params);
            return res;
        } catch (e) {
            Log.error(e.message);
            throw new Error('Failed to execute query',e.message);
        } finally {
            await client.end();
        }
    }

}
module.exports = PGConnection;