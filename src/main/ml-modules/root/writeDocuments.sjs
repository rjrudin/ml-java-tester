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
      "/example/" + sem.uuidString() + ".json",
      content,
      [xdmp.permission("rest-reader", "read"), xdmp.permission("rest-writer", "update")],
      ["data", "simpleBulkService"]
    );
  }
} else {
  for (var content of input) {
    content = content.toObject();
    xdmp.documentInsert(
      "/example/" + content.uri,
      content.content,
      content.permissions.map(perm => xdmp.permission(xdmp.role(perm.roleName), perm.capability)),
      content.collections
    );
  }
}
