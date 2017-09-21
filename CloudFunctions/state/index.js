/*
 *
 *
 * Cloud function for subscribing to manifold-state topic for status
 *
 *
 */

const request = require('request-promise');

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
        throw new Error('State: message is empty!');
    }
    //parsing the message for updating/creating the Manifold kind entity
    createEntity(jsonData);

    // Don't forget to call the callback!
    callback();
}
// [END functions_pubsub_subscribe]

// [START createEntity]
function createEntity(jsonData){
    console.log("State: Entered createEntity...");
    var manifold_sn = jsonData.manifold_sn;
    var timestamp = new Date();

    const pressureFault = "pressure";
    const cycleCountFault = "cycle count";
    const leakFault = "leak";

    const alertQuery = "SELECT count(*) as count FROM valve_alert WHERE valve_sn = ? and needs_notification = true";
    const updateAlertQuery = "UPDATE valve_alert SET needs_notification = false WHERE valve_sn = ? and needs_notification = true";

    console.log("stations_count:"+jsonData.stations.length);

    var connection = mysql.createConnection({
        socketPath: '/cloudsql/' + projectid + ':us-central1:nexmatixmvd',
        user: 'cfuser',
        database: 'smartdevice_repo'
    });

    connection.connect();

    var station_index;
    for(station_index in jsonData.stations){
        console.log("The station index is: ", station_index);

        station = jsonData.stations[station_index];
        const station_num = station.station_num;
        const valve_sn = station.valve_sn;
        if(valve_sn !== null && valve_sn !== undefined) {

            const default_ccl = 20000000;

            console.log("state: station_index:" + station_index);
            console.log("State: station-" + JSON.stringify(station));
            console.log("State: station_num-" + station_num);
            console.log("State: valve_sn-" + valve_sn);

            var entity = {
                valve_sn: station.valve_sn,
                manifold_sn: manifold_sn,
                station_num: station.station_num,
                cc: station.cc,
                ccl: station.ccl !== null ? station.ccl : default_ccl,
                timestamp: timestamp,
                input: station.input,
                pp: station.pp,
                p_fault: station.p_fault,
                leak: station.leak
            };
            console.log("State: Creating ValveStatusEntity..." + JSON.stringify(entity));

            connection.query('INSERT INTO valve_status SET ? on duplicate key update ?', [entity, entity], function (error, results, fields) {
                if (error) console.log(error);
                else console.log("inserted successfully");
            });

            var count = 0;

            connection.query(alertQuery, [valve_sn], function (error, results) {
                if (error) console.log(error);
                else {
                    console.log("Total new results after insert are: ", results);
                    for (var i in results) {
                        count = results[i].count;
                        if(count > 0) {
                            console.log("The count is greater than 0, sending email.");
                            sendEmailRequest(valve_sn);
                        }
                    }
                }
            });

            console.log("Count is: ", count);

            connection.query(updateAlertQuery, [valve_sn], function (error, results) {
                    if (error) console.log(error);
                    else {
                        console.log("Updated valve_alerts to not need notification for valve: ", valve_sn);
                    }
            });
        }
    }

    connection.end();
}
//[END createEntity]

function sendEmailRequest(valveSN) {
    console.log("Send email request for valveSN", valveSN);

    const baseUrl = 'https://' + projectid + '.appspot.com';
    const emailUri = '/notification/';
    const method = 'POST';

    console.log("The baseURL is: " + baseUrl);

    var emailRequest = {
        method: method,
        uri: baseUrl + emailUri + valveSN
    };

    request(emailRequest).then(response => {
        console.log("The email response is: ", response);
    });
}

/*

  gcloud beta functions deploy manifold-state-subscriber --entry-point subscribe --stage-bucket nexmatix-staging-bucket --trigger-topic manifold-state

*/
