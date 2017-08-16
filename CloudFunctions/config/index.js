/*
 *
 *
 * Cloud function for subscribing to manifold-configuration topic
 *
 *
 */

var projectid = process.env.GCLOUD_PROJECT;
var mysql = require('mysql');

// [START functions_pubsub_subscribe]
/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {object} event The Cloud Functions event.
 * @param {object} event.data The Cloud Pub/Sub Message object.
 * @param {string} event.data.data The "data" property of the Cloud Pub/Sub Message.
 * @param {function} The callback function.
 */
exports.subscribe = function subscribe (event, callback) {
    const pubsubMessage = event.data;

    // We're just going to log the message to prove that it worked!
    const message = Buffer.from(pubsubMessage.data, 'base64').toString();

    console.log(message);

    var jsonData = JSON.parse(message);

    if (!jsonData) {
        throw new Error('message-payload is empty!');
    }
    //parsing the message for updating/creating the 'Valve' kind entity
    createEntity(jsonData);

    // Don't forget to call the callback!
    callback();
};
// [END functions_pubsub_subscribe]

// [START createEntity]
function createEntity (jsonData) {
    var connection = mysql.createConnection({
        socketPath: '/cloudsql/' + projectid + ':us-central1:nexmatixmvd',
        user: 'cfuser',
        database: 'smartdevice_repo'
    });

    connection.connect();

    for(var i = 0; i < jsonData.stations.length; i++){

        console.log("Config: iteration-"+i);

	var station = jsonData.stations[i];

	var entity = {
            manifold_sn: jsonData.manifold_sn,
            timestamp: new Date(jsonData.timestamp),

            fab_date: station.fab_date,
	    ship_date: station.ship_date,
	    sku: station.sku,
	    station_num: station.station_num,
            valve_sn: station.valve_sn,
            ccl: station.ccl
	};

        connection.query('INSERT INTO valve SET ? on duplicate key update ?', [entity, entity], function(error, results, fields) {
            if (error) console.log(error);
            else console.log("inserted successfully");
        });
    }

    connection.end();
}
//[END createEntity]

/*
  gcloud beta functions deploy manifold-configuration-subscriber --entry-point subscribe --stage-bucket nexmatix-staging-bucket --trigger-topic manifold-configuration

*/
