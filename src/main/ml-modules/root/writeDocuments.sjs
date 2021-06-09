'use strict';
declareUpdate();
var input;

if (fn.count(input) == 1) {
  input = Sequence.from([input]);
}

for (var content of input) {
  content = content.toObject();
  xdmp.documentInsert(
    "/example/" + content.uri,
    content.content,
    content.permissions.map(perm => xdmp.permission(xdmp.role(perm.roleName), perm.capability)),
    content.collections
  );
}
