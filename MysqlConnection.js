const Global = require('@phinxlab/config-manager');
const Log = new (require('@phinxlab/log-manager'))('MysqlConnection');
const { createConnection, createPool } = require('mysql');

class MysqlConnection {

  static registerConfig(cfg) {
    Global.addConfig("MYSQL_CONNECTION", cfg)
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

  async tx(callback, pk, valuePk) {
    const client = createPool(Global.MYSQL_CONNECTION);
    try {
      await MysqlConnection.execQuery(client,'BEGIN');
      try {
        client.execute = (query) => MysqlConnection.execQuery(client, query, null, pk, valuePk);
        const response = await callback(client);
        await MysqlConnection.execQuery(client,'COMMIT');
        return response;
      } catch (e) {
        await MysqlConnection.execQuery(client,'ROLLBACK');
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
    }
  }

  static execQuery (client, query, params, pk, valuePk) {
    return new Promise ((res, rej) => {
      client.query(query, params, (error, result, fields) => {
        if (error) {
          rej(error)
        } else {
          //This is the postgres' structure, we need to simulate how the result come from the postgres
          res({
            command: '',
            rowCount: 0,
            oid: null,
            rows: Array.isArray(result)? result : [{
              ...result,
              [pk]: valuePk || result.insertId,
            }],
            fields,
          });
        }
      });
    })
  };

  async execute(query, params = []) {
    const client = createConnection(Global.MYSQL_CONNECTION);
    try {
      await client.connect();
      return await MysqlConnection.execQuery(client, query, params);
    } catch (e) {
      Log.error(e.message);
      throw new Error('Failed to execute query',e.message);
    } finally {
      await client.end();
    }
  }

}
module.exports = MysqlConnection;