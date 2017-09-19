/*
 *
 *
 * Cloud function for subscribing to manifold-configuration topic
 *
 *
 */


// [START functions_pubsub_subscribe]
/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {object} event The Cloud Functions event.
 * @param {object} event.data The Cloud Pub/Sub Message object.
 * @param {string} event.data.data The "data" property of the Cloud Pub/Sub Message.
 * @param {function} The callback function.
 */

const request = require('request-promise');

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
    createMessage(jsonData);

    // Don't forget to call the callback!
    callback();
};
// [END functions_pubsub_subscribe]

// [START createMessage]
function createMessage (jsonData) {
    const elasticSearchHost = 'http://35.202.211.96/elasticsearch';
    const configUri = '/valve_config/valve';
    const elasticSearchMethod = 'POST';

    var station_index;
    for(station_index in jsonData.stations) {
        console.log("Config: iteration-" + i);

        var station = jsonData.stations[station_index];

        var entity = {
            manifold_sn: jsonData.manifold_sn,
            timestamp: new Date(),
            fab_date: new Date(station.fab_date),
            ship_date: new Date(station.ship_date),
            sku: station.sku,
            station_num: station.station_num,
            valve_sn: station.valve_sn,
            cycle_count_limit: station.ccl,
            event_ts: new Date()
        };

        var elasticSearchRequest = {
            method: elasticSearchMethod,
            uri: elasticSearchHost + configUri,
            auth: {
                username: "user",
                password: "VQY4sxqG"
            },
            body: entity,
            json: true
        };

        console.log("The elasticSearchRequest is ", elasticSearchRequest);

        request(elasticSearchRequest).then(response => {
            console.log("Elasticsearch response", response);
        });
    }
};
//[END createMessage]

/*
  gcloud beta functions deploy elasticsearch-manifold-configuration-subscriber --entry-point subscribe --stage-bucket nexmatix-staging-bucket --trigger-topic manifold-configuration

*/
