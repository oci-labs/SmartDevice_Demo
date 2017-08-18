/*
 *
 *
 * Cloud function for subscribing to manifold-state topic for status and send new indexed document to the elasticsearch instance
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
        throw new Error('State: message is empty!');
    }
    //parsing the message for updating/creating the Manifold kind entity
    createMessage(jsonData);

    // Don't forget to call the callback!
    callback();
}
// [END functions_pubsub_subscribe]

// [START createMessage]
function createMessage(jsonData) {
    const elasticSearchHost = 'http://35.193.249.208/elasticsearch';
    const stateUri = '/manifold_state/state';
    const elasticSearchMethod = 'POST';
    console.log("State: Entered createEntity...");
    var manifold_sn = jsonData.manifold_sn;
    var timestamp = new Date(jsonData.timestamp);
    console.log("Timestamp ", timestamp);

    console.log("station_count:" + jsonData.stations.length);

    var station_index;
    for (station_index in jsonData.stations) {
        station = jsonData.stations[station_index];
        const station_num = station.station_num;
        const valve_sn = station.valve_sn;

        console.log("state: station_index:" + station_index);
        console.log("State: station-" + JSON.stringify(station));
        console.log("State: station_num-" + station_num);
        console.log("State: valve_sn-" + valve_sn);

        var entity = {
            valve_sn: station.valve_sn,
            manifold_sn: manifold_sn,
            station_num: station.station_num,
            cycle_count: station.cc,
            cycle_count_limit: station.ccl,
            update_ts: timestamp,
            input: station.input,
            pressure_point: station.pp,
            pressure_fault: station.p_fault,
            leak: station.leak,
            event_ts: timestamp
        };
        console.log("State: Creating ValveStatusEntity..." + JSON.stringify(entity));

        var elasticSearchRequest = {
            method: elasticSearchMethod,
            uri: elasticSearchHost + stateUri,
            auth: {
                username: "user",
                password: "O2Xzg7yk"
            },
            body: entity,
            json: true
        };

        console.log("The elasticSearchRequest is ", elasticSearchRequest);

        return request(elasticSearchRequest).then(response => {
            console.log("Elasticsearch response", response);
        });

    }
}
//[END createMessage]

/*

 gcloud beta functions deploy elasticsearch-manifold-state-subscriber --entry-point subscribe --stage-bucket nexmatix-staging-bucket --trigger-topic manifold-state

 */
