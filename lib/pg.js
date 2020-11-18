var pg = require('pg');
var named = require('node-postgres-named');
var Cursor = require('pg-cursor');

module.exports=function(RED){

	function PostgresDatabaseNode(n) {
		RED.nodes.createNode(this, n);
		this.hostname = n.hostname;
		this.port = n.port;
		this.db = n.db;
		this.ssl = n.ssl;

		var credentials = this.credentials;
		if (credentials) {
			this.user = credentials.user;
			this.password = credentials.password;
		}
	}
	
	RED.nodes.registerType("postgresdb", PostgresDatabaseNode, {
		credentials: {
			user: {type: "text"},
			password: {type: "password"}
		}
	});


	function PostgresNode(n) {
		RED.nodes.createNode(this, n);
	
		var node = this;
	
		node.topic = n.topic;
		node.postgresdb = n.postgresdb;
		node.postgresConfig = RED.nodes.getNode(this.postgresdb);
		node.sqlquery = n.sqlquery;
		node.output = n.output;
		node.perrow = n.perrow;
		node.rowspermsg = n.rowspermsg;
	
		if (node.postgresConfig) {
	
			var connectionConfig = {
				user: node.postgresConfig.user,
				password: node.postgresConfig.password,
				host: node.postgresConfig.hostname,
				port: node.postgresConfig.port,
				database: node.postgresConfig.db,
				ssl: node.postgresConfig.ssl
			};
			node.pgpool=new pg.Pool(connectionConfig);
	
			var handleError = function(err, msg) {
				msg.error=err;
				node.error(err,msg);
				console.log(err);
				console.log(msg.payload);
				console.log(msg.queryParameters);
			};
				
			node.on('input', function(msg) {
				node.pgpool.connect((err, client, done)=>{
					if (err) {
						handleError(err, msg);
					} else {
						
							named.patch(client);
						
						if (!!!msg.queryParameters)
							msg.queryParameters = [];
						
						if (!node.perrow){
							try{
								var q=client.query(
									msg.payload,
									msg.queryParameters,
									function(err, results) {
										done();
										if (err) {
											handleError(err, msg);
										} else {
											if (node.output && !node.perrow) {
												msg.payload = results.rows;
												node.send(msg);
											}
										}
									}
								);
							} catch(err){
								node.error("Some query parameters are missing", msg);
							}
						}
						else {
							var cur=client.query(new Cursor(msg.payload,msg.queryParameters));
							var sndrow=(err,rows)=>{
								if (!!err) {
									handleError(err,msg);
									done();
								}
								else {
									if (rows.length>0){
										node.send(Object.assign(Object.assign({},msg),{payload:((node.rowspermsg||1)>1)?rows:rows[0]}));
										cur.read(node.rowspermsg||1,sndrow);
									}
									else done();									
								}
							}
							cur.read(node.rowspermsg||1,sndrow);
						}
					
					}
				});
			});
		} else {
			this.error("missing postgres configuration");
		}

		this.on("close", function() {
			if (node.clientdb) node.clientdb.end();
		});
	}

	RED.nodes.registerType("postgres", PostgresNode);
}
