'use strict';
declareUpdate();
var input;
var endpointConstants = fn.head(xdmp.fromJSON(endpointConstants));

if (fn.count(input) == 1) {
  input = Sequence.from([input]);
}

if (endpointConstants.simpleBulkService === true) {
  for (var content of input) {
    xdmp.documentInsert(
      "/simple/" + sem.uuidString() + ".json",
      content,
      [xdmp.permission("rest-reader", "read"), xdmp.permission("rest-writer", "update")],
      ["data", "simpleBulkService"]
    );
  }
} else {
  // Send two objects for each document - a metadata JSON object, then the content object
  // Note that this requires the batchSize to be an even value.
  const inputArray = input.toArray();
  for (var i = 0; i < inputArray.length; i += 2) {
    const metadata = inputArray[i].toObject();
    const content = inputArray[i + 1];
    xdmp.documentInsert(
      "/complex/" + sem.uuidString() + ".json",
      content,
      metadata.permissions.map(perm => xdmp.permission(xdmp.role(perm.roleName), perm.capability)),
      metadata.collections
    )
  }
}
