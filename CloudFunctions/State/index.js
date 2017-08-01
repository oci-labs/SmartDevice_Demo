// [START functions_pubsub_subscribe]
/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {object} event The Cloud Functions event.
 * @param {object} event.data The Cloud Pub/Sub Message object.
 * @param {string} event.data.data The "data" property of the Cloud Pub/Sub Message.
 * @param {function} The callback function.
 */
exports.subscribe = function manifold-state-subscriber (event, callback) {
  const pubsubMessage = event.data;

  // We're just going to log the message to prove that it worked!
  console.log(Buffer.from(pubsubMessage.data, 'base64').toString());

  // Don't forget to call the callback!
  callback();
};
// [END functions_pubsub_subscribe]