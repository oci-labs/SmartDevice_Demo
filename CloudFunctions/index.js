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
  
  var message_object = JSON.parse(message);
  
  
  if (!message_object) {
    throw new Error('message is empty!');
  }
  //parsing the message for updating/creating the Manifold kind entity
  createEntity_ManifoldKind(message_object);

  // Don't forget to call the callback!
  callback();
};
// [END functions_pubsub_subscribe]

// [START createEntity_ManifoldKind]
function createEntity_ManifoldKind (message_object) {
  var manifold_key = message_object.manifold_sn;
  var kind = "Manifold";
  var request_for_key = JSON.parse("{\"kind\":\"".concat(kind).concat("\", \"key\":\"").concat(manifold_key).concat("\"}"));
  //var request = '{"kind":"Task","key":"sampletask1"}';
   
  console.log(manifold_key);
  console.log(request_for_key);
   
  const key = getKeyFromRequestData(request_for_key);
  
  var entity = {
    key: key,
	data: [
      {
        name: 'last_updated',
        value: new Date().toJSON()
      }
    ]
  };

  //function to add entities
  addEntity(entity);
}
//[END createEntity_ManifoldKind]

// [START addEntity]
function addEntity (entity) {
  datastore.save(entity)
    .then(() => {
      console.log(`Task ${key.id} created successfully.`);
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