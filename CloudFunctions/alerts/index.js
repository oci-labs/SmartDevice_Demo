/*
*
*
*Cloud function for subscribing to manifold-state topic for ValveAlerts
*
*
*/

// Specifying datastore requirement in GCP project
const Datastore = require('@google-cloud/datastore');

// Instantiates a client
const datastore = Datastore();

//enums
const KIND_VALVE_ALERT = "ValveAlert"
const PRESSURE_FAULTS = ['H', 'L'];
const LEAKS = ['C', 'P'];
const TYPE_PRESSURE_FAULT = "p_fault";
const TYPE_LEAK = "leak";
const TYPE_C_THRESH_FAULT = "c_thresh";


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
        throw new Error('message is empty!');
    }
    //parsing the message for updating/creating the Manifold kind entity
    createEntity(jsonData);

    // Don't forget to call the callback!
    callback();
}
// [END functions_pubsub_subscribe]

function createEntity(jsonData){
console.log("Alert: Entered createEntity...");

    var station_index;
    for(station_index in jsonData.stations){
        //new transaction started for i'th station
        const transaction = datastore.transaction();
        station = jsonData.stations[station_index];
        const valve_sn = station.valve_sn;

        var isCCLExceeded = (station.cc > station.ccl)? true:false;
        var isPressureFault_notReported = (station.p_fault == 'N')?true:false;
        var isLeak_notReported = (station.leak=='N')?true:false;

        var valveAlertKey_pFault = valve_sn + "." + TYPE_PRESSURE_FAULT;
        var valveAlertKey_leak = valve_sn + "." + TYPE_LEAK;
        var valveAlertKey_CCL = valve_sn + "." + TYPE_C_THRESH_FAULT;

        var retrieved_key_alert_p_fault = datastore.key([KIND_VALVE_ALERT, valveAlertKey_pFault]);
        var retrieved_key_alert_leak = datastore.key([KIND_VALVE_ALERT, valveAlertKey_leak]);
        var retrieved_key_alert_c_thresh = datastore.key([KIND_VALVE_ALERT, valveAlertKey_CCL]);

        console.log("Alert: station_index:"+station_index);
        console.log("Alert: station-"+JSON.stringify(station));
        console.log("Alert: valve_sn-"+valve_sn);
        console.log("Alert: valveAlertKey_pFault-"+valveAlertKey_pFault);
        console.log("Alert: valveAlertKey_leak-"+valveAlertKey_leak);
        console.log("Alert: valveAlertKey_CCL-"+valveAlertKey_CCL);

        return transaction.run()
        // fetch valveAlert for pressure fault
        .then (()=>transaction.get(retrieved_key_alert_p_fault))
        .then((results)=> {
            const retrieved_entity = results[0];
            //case1: if valve_sn.alert_type exists && p_fault =='N'
            if(retrieved_entity && isPressureFault_notReported){
                return transaction.delete(retrieved_key_alert_p_fault);
            }
            //case2: if valve_sn.alert_type does not exist && p_fault = 'H' or 'L'
            else if(!retrieved_entity && !isPressureFault_notReported){

                message_time = new Date().getTime();
                var entity = createValveAlertEntity(station, TYPE_PRESSURE_FAULT, retrieved_key_alert_p_fault, message_time);
                return transaction.save(entity);
            }
            //Case3: if valve_sn.alert_type exists && p_fault = 'H' or 'L'
            else if(retrieved_entity && !isPressureFault_notReported){

                //update function
                message_time = retrieved_entity.detection_time;
                var entity = createValveAlertEntity(station, TYPE_PRESSURE_FAULT, retrieved_key_alert_p_fault, message_time);
                return transaction.save(entity);
            }
            //Case4: if valve_sn.alert_type does not exist && p_fault == 'N'
            else{
                //Do Nothing
            }
        })

        // fetch valveAlert for leak
        .then (()=>transaction.get(retrieved_key_alert_leak))
        .then((results)=> {
            const retrieved_entity = results[0];
            //case1: if valve_sn.alert_type exists && leak =='N'
            if(retrieved_entity && isLeak_notReported){
                return transaction.delete(retrieved_key_alert_leak);
            }
            //case2: if valve_sn.alert_type does not exist && leak = 'C' or 'Persistent'
            else if(!retrieved_entity && !isLeak_notReported){

                message_time = new Date().getTime();
                var entity = createValveAlertEntity(station, TYPE_LEAK, retrieved_key_alert_leak, message_time);
                return transaction.save(entity);
            }
            //Case3: if valve_sn.alert_type exists && leak = 'C' or 'Persistent'
            else if(retrieved_entity && !isLeak_notReported){

                //update function
                message_time = retrieved_entity.detection_time;
                var entity = createValveAlertEntity(station, TYPE_LEAK, retrieved_key_alert_leak, message_time);
                return transaction.save(entity);
            }
            //Case4: if valve_sn.alert_type does not exist && leak == 'N'
            else{
                //Do Nothing
            }
        })

             // fetch valveAlert for cc_Threshold
            .then (()=>transaction.get(retrieved_key_alert_c_thresh))
            .then((results)=> {
                const retrieved_entity = results[0];
                //case1: if cc>ccl && valve_sn.alert_type does not exists
                if(isCCLExceeded && !retrieved_entity){
                   console.log("Alert: inserting c_thresh...");
                   message_time = new Date().getTime();
                   var entity = createValveAlertEntity(station, TYPE_C_THRESH_FAULT, retrieved_key_alert_c_thresh, message_time);
                   return transaction.save(entity);
                }
                //case2: if cc>ccl && valve_sn.alert_type exists
                else if(isCCLExceeded && retrieved_entity){
                    console.log("Alert: updating c_thresh...");
                    message_time = retrieved_entity.detection_time;
                    var entity = createValveAlertEntity(station, TYPE_C_THRESH_FAULT, retrieved_key_alert_c_thresh, message_time);
                    return transaction.save(entity);
                }
                //Case3: if cc<=ccl
                else{
                     //Do Nothing
                }
            })

            .then(()=> {
               transaction.commit();
               console.log("Transaction Committed");
            })
            .catch((exception)=> {
                transaction.rollback();
                console.log("Transaction Rolledback:"+exception);
            });
        }
    }

function createValveAlertEntity(jsonData, alert_type, key, message_time){
    console.log("Creating ValveAlertEntity...")
    if(alert_type==TYPE_PRESSURE_FAULT){
        description = ((jsonData.p_fault == 'H') ? "High":"Low") + " pressure fault detected";
    }else if(alert_type==TYPE_LEAK){
        description = ((jsonData.leak == 'P') ? "Persistent":"\"C\"") + " leak detected";
    }else if(alert_type==TYPE_C_THRESH_FAULT){
        description = "Cycle Count exceeded the the threshold (ccl) by " + (jsonData.cc-jsonData.ccl);
    }else{
        console.log("Error:Wrong alert type inputted");
    }
    const entity = {
        key: key,
        data: [
            {
                name: 'valve_sn',
                value: jsonData.valve_sn
            },
            {
                name: 'detection_time',
                value: message_time //do a query for old time stamp
            },
            {
                name: 'alert_type',
                value: alert_type
            },
            {
                name: 'description',
                value: description
            }
        ]
    };
    return entity;
}

/*

gcloud beta functions deploy manifold-alert-subscriber --entry-point subscribe --stage-bucket nexmatix-staging-bucket --trigger-topic manifold-state

 */
