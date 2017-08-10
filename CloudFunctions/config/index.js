/*
*
*
*Cloud function for subscribing to manifold-configuration topic
*
*
*/


// Specifying datastore requirement in GCP project
const Datastore = require('@google-cloud/datastore');

// Instantiates a client
const datastore = Datastore();


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
  var manifold_key = jsonData.manifold_sn;
  
  for(var i = 0; i < jsonData.stations.length; i++){

      console.log("Config: iteration-"+i);

	  var station = jsonData.stations[i];
	  
	  var kind = "Valve";
	  var entityKey = station.valve_sn;
	  var request_for_key = JSON.parse("{\"kind\":\"".concat(kind).concat("\", \"key\":\"").concat(entityKey).concat("\"}"));
	  const key = getKeyFromRequestData(request_for_key);

	  var entity = {
		key: key,
		data: [
			{
				name: 'station_num',
				value: station.station_num
			},
			{
				name: 'update_time',
				value: station.update_time
			},
			{
				name: 'sku',
				value: station.sku
			},
			{
				name: 'fab_date',
				value: station.fab_date
			},
			{
				name: 'ship_date',
				value: station.ship_date
			}
		]
		};

		//function to add entities
		addEntity(entity);
	}
  }
  
//[END createEntity]

// [START addEntity]
function addEntity (entity) {
  datastore.save(entity)
    .then(() => {
      console.log(`an Entity ${entity.key.id} created successfully.`);
    })
    .catch((err) => {
      console.error('ERROR:', err);
    });
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

  return datastore.key([requestData.kind, requestData.key]);
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
    });
};

/*
gcloud beta functions deploy manifold-configuration-subscriber --entry-point subscribe --stage-bucket nexmatix-staging-bucket --trigger-topic manifold-configuration

*/