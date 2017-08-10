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
console.log("Entered createEntity");
    var manifold_key = jsonData.manifold_sn;
    const transaction = datastore.transaction();
      for(var i = 0; i < jsonData.stations.length; i++){
            console.log("Iteration: "+i);
            var station = jsonData.stations[i];

            const station_num = station.station_num;
            const valve_sn = station.valve_sn;

            var valveStatusKey = manifold_key + "." + station_num + "." + valve_sn;
            var valveAlertKey_pFault = valve_sn + "." + TYPE_PRESSURE_FAULT;
            var valveAlertKey_leak = valve_sn + "." + TYPE_LEAK;
            var valveAlertKey_CCL = valve_sn + "." + TYPE_C_THRESH_FAULT;
            var cclExceeded = (jsonData.cc > jsonData.ccl)? true:false;
            var isPressureFault_notReported = (jsonData.p_fault == 'N')?true:false;
            var isLeak_notReported = (jsonData.leak=='N')?true:false;

            return transaction.run()
                // fetch valve status entity
                .then (()=>datastore.key([KIND_VALVE_STATUS, valveStatusKey]))
                .then((results)=> {
                    const retrieved_key = results[0];
                    // If valve status entity exists
                    if(retrieved_key){
                        //update
                        console.log("Updated a valve Status Entity with key:"+retrieved_key.name);
                    }else{
                        // Insert valve status
                        console.log("VStatusKey_retrieved:"+retrieved_key.name);
                        //console.log("Created a valve Status Entity with key:"+retrieved_key.name);
                        return transaction.save(createValveStatusEntity(retrieved_key, station));
                    }
                })

                // fetch valveAlert for pressure fault
                .then (()=>datastore.key([KIND_VALVE_ALERT,valveAlert_pFault]))
                .then((results)=> {
                    const retrieved_key = results[0];
                    //case1: if valve_sn.alert_type exists && p_fault =='N'
                    if(retrieved_key && isPressureFault_notReported){
                        return transaction.delete(retrieved_key);
                    }
                    //case2: if valve_sn.alert_type does not exist && p_fault = 'H' or 'L'
                    else if(!retrieved_key && !isPressureFault_notReported){
                        return transaction.save(createValveStatusEntity(station,TYPE_PRESSURE_FAULT));
                    }
                    //Case3: if valve_sn.alert_type exists && p_fault = 'H' or 'L'
                    else if(retrieved_key && !isPressureFault_notReported){
                        //update function

                    }
                    //Case4: if valve_sn.alert_type does not exist && p_fault == 'N'
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

function createValveAlertEntity(jsonData, alert_type){
    console.log("Creating ValveAlertEntity...")
    if(alert_type==TYPE_PRESSURE_FAULT){
        key = valveAlertKey_pFault;
        description = ((jsonData.p_fault == 'H') ? "High":"Low") + " pressure fault detected";
    }else if(alert_type==TYPE_LEAK){
        key = valveAlertKey_leak;
        description = ((jsonData.leak == 'P') ? "Persistent":"\"C\"") + " leak detected";
    }else if(alert_type==TYPE_C_THRESH_FAULT){
        key = valveAlertKey_leak;
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
                value: new Date().getTime() //do a query for old time stamp
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

function createValveStatusEntity(jsonData, key){
    entity = {
        key: key,
        data: [
            {
                name: 'valve_sn',
                value: jsonData.valve_sn
            },
            {
                name: 'manifold_sn',
                value: jsonData.manifold_key
            },
            {
                name: 'station_num',
                value: jsonData.station_num
            },
            {
                name: 'update_time',
                value: new Date().getTime()
            },
            {
                name: 'input',
                value: jsonData.input
            },
            {
                name: 'cc',
                value: jsonData.cc
            },
            {
                name: 'pp',
                value: jsonData.pp
            },
            {
                name: 'ccl',
                value: jsonData.ccl
            },
            {
                name: 'p_fault',
                value: jsonData.p_fault
            },
            {
                name: 'leak',
                value: jsonData.leak
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

// [START delete_entity]
function deleteAlert (transaction, valve_sn, alert_type) {
    const retrieved_key_del = transaction.key(
        [
            KIND_VALVE_ALERT,
            valve_sn + '.' + alert_type
        ]
    );
    transaction.delete(retrieved_key_del)
}
// [END delete_entity]

// [START addEntity]
function addEntity (transaction, entity) {
    transaction.save(entity);
}
// [END add_entity]

/**
 * Gets a Datastore key from the kind/key pair in the request.
 *
 * @param {object} requestData Cloud Function request data.
 * @param {string} requestData.key Datastore key string.
 * @param {string} requestData.kind Datastore kind.
 * @returns {object} Datastore key object.
 */
function getKeyFromRequestData (requestData) {
  if (!requestData.key) {
    throw new Error('Key not provided. Make sure you have a "key" property in your request');
  }

  if (!requestData.kind) {
    throw new Error('Kind not provided. Make sure you have a "kind" property in your request');
  }

  return transaction.key([requestData.kind, requestData.key]);
}

/**
 * Creates and/or updates a record.
 *
 * @example
 * gcloud alpha functions call set --data '{"kind":"Task","key":"sampletask1","value":{"description": "Buy milk"}}'
 *
 * @param {object} req Cloud Function request context.
 * @param {object} req.body The request body.
 * @param {string} req.body.kind The Datastore kind of the data to save, e.g. "Task".
 * @param {string} req.body.key Key at which to save the data, e.g. "sampletask1".
 * @param {object} req.body.value Value to save to Cloud Datastore, e.g. {"description":"Buy milk"}
 * @param {object} res Cloud Function response context.
 */
exports.set = function set (req, res) {
    // The value contains a JSON document representing the entity we want to save
    if (!req.body.value) {
        throw new Error('Value not provided. Make sure you have a "value" property in your request');
    }

    const key = getKeyFromRequestData(req.body);
    const entity = {
        key: key,
        data: req.body.value
    };

    return datastore.save(entity)
        .then(() => res.status(200).send(`Entity ${key.path.join('/')} saved.`))
        .catch((err) => {
            console.error(err);
            res.status(500).send(err);
            return Promise.reject(err);
        }
    );
};