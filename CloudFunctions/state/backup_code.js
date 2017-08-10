/*
*
*
*Cloud function for subscribing to manifold-state topic
*
*
*/

// Specifying datastore requirement in GCP project
const Datastore = require('@google-cloud/datastore');

// Instantiates a client
const datastore = Datastore();

//enums
const KIND_VALVE_STATUS = "ValveStatus"
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
console.log("Entered createEntity...");
    var manifold_sn = jsonData.manifold_sn;
    for(var i = 0; i < jsonData.stations.length; i++){
            const transaction = datastore.transaction();
            console.log("Iteration: "+i);
            var station = jsonData.stations[i];

            const station_num = station.station_num;
            const valve_sn = station.valve_sn;

            var isCCLExceeded = (station.cc > station.ccl)? true:false;
            var isPressureFault_notReported = (station.p_fault == 'N')?true:false;
            var isLeak_notReported = (station.leak=='N')?true:false;

            var valveStatusKey = manifold_sn + "." + station_num + "." + valve_sn;
            var valveAlertKey_pFault = valve_sn + "." + TYPE_PRESSURE_FAULT;
            var valveAlertKey_leak = valve_sn + "." + TYPE_LEAK;
            var valveAlertKey_CCL = valve_sn + "." + TYPE_C_THRESH_FAULT;

            var retrieved_key_status = datastore.key([KIND_VALVE_STATUS, valveStatusKey]);
            var retrieved_key_alert_p_fault = datastore.key([KIND_VALVE_ALERT, valveAlertKey_pFault]);
            var retrieved_key_alert_leak = datastore.key([KIND_VALVE_ALERT, valveAlertKey_leak]);
            var retrieved_key_alert_c_thresh = datastore.key([KIND_VALVE_ALERT, valveAlertKey_CCL]);

            return transaction.run()
                // fetch valve status entity
                .then (()=>transaction.get(retrieved_key_status))
                .then((results)=> {
                    const retrieved_entity = results[0];
                    // If valve status entity exists
                    if(retrieved_entity){
                        //update
                        console.log("Updated a valve Status Entity with key:"+retrieved_key_status.name);
                        var entity = createValveStatusEntity(station, manifold_sn, retrieved_key_status);
                        return transaction.save(entity);
                    }else{
                        // Insert valve status
                        console.log("VStatusKey_retrieved:"+retrieved_key_status.name);
                        //console.log("Created a valve Status Entity with key:"+retrieved_key.name);
                        var entity = createValveStatusEntity(station, manifold_sn, retrieved_key_status);
                        return transaction.save(entity);
                    }
                })

                // fetch valveAlert for pressure fault
              /*  .then (()=>transaction.get(retrieved_key_alert_p_fault))
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
                        message_time = retrieved_entity.update_time;
                        var entity = createValveAlertEntity(station, TYPE_PRESSURE_FAULT, retrieved_key_alert_p_fault, message_time);
                        return transaction.save(entity);
                    }
                    //Case4: if valve_sn.alert_type does not exist && p_fault == 'N'
                    else{
                        //Do Nothing
                    }
                })*/

                 // fetch valveAlert for cc_Threshold
                .then (()=>transaction.get(retrieved_key_alert_c_thresh))
                .then((results)=> {
                    const retrieved_entity = results[0];
                    //case1: if cc>ccl && valve_sn.alert_type does not exists
                    if(isCCLExceeded && !retrieved_entity){
                       message_time = new Date().getTime();
                       var entity = createValveAlertEntity(station, TYPE_C_THRESH_FAULT, retrieved_key_alert_c_thresh, message_time);
                       return transaction.save(entity);
                    }
                    //case2: if cc>ccl && valve_sn.alert_type exists
                    else if(isCCLExceeded && retrieved_entity){
                        message_time = retrieved_entity.update_time;
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

function createValveStatusEntity(jsonData, manifold_sn, key){
    message_time = new Date().getTime();
    entity = {
        key: key,
        data: [
           {
                name:"valve_sn", value:jsonData.valve_sn
           },
           {
                name:"manifold_sn", value:manifold_sn
           },
           {
                name:"station_num", value:jsonData.station_num
           },
           {
                name:"cc", value:jsonData.cc
           },
           {
                name:"ccl", value:jsonData.ccl
           },
           {
                name:"update_time", value:message_time
           },
           {
                name:"input", value:jsonData.input
           },
           {
                name:"pp", value:jsonData.pp
           },
           {
                name:"p_fault", value:jsonData.p_fault
           },
           {
                name:"leak", value:jsonData.leak
           }
        ]
    };
    console.log("Creating ValveStatusEntity..." + JSON.stringify(entity));
    return entity;
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



// [START update_entity]
function updateEntity (transaction, valve_sn, alert_type, description) {
    const retrieved_key = transaction.key([
        KIND_VALVE_ALERT,
        valve_sn + '.' + alert_type
    ]);
    console.log("retrieved_keyID: "+retrieved_key.name);
    transaction.run()
        .then(() => transaction.get(retrieved_key))
        .then((results) => {
            const retrieved_entity = results[0];
            retrieved_entity.description = description;
            console.log("retrieved_entity.description:"+retrieved_entity.description);
            console.log("description:"+description);
            transaction.save({
            key: retrieved_key,
            data: retrieved_entity
            });
        }
    );
}
// [END update_entity]


 /*
    const transaction = datastore.transaction();

    var entities = [];
    console.log("State: jsonData.stations.length:"+jsonData.stations.length);
    var station;
    for(station in jsonData.stations){
        console.log("State: station-"+JSON.stringify(station));
        const station_num = jsonData.stations[station].station_num;
        const valve_sn = jsonData.stations[station].valve_sn;
        var valveStatusKey = manifold_sn + "." + station_num + "." + valve_sn;
        var retrieved_key_status = datastore.key([KIND_VALVE_STATUS, valveStatusKey]);
        var entity = createValveStatusEntity(station, manifold_sn, retrieved_key_status);
        entities.push(entity);
    }

    return transaction.run()
        .then(()=> {
             return transaction.save(entities);
        })
        .then(()=> {
           transaction.commit();
           console.log("State: Transaction Committed");
        })
        .catch((exception)=> {
            transaction.rollback();
            console.log("State: Transaction Rolledback:"+exception);
        });
*/