'use strict';
// Recover the installed generated bundle without executing plugin code or reading user settings.
const fs = require('fs');
const path = require('path');
const root = path.join(__dirname, '..');
const bundle = fs.readFileSync(path.join(root, 'main.js'), 'utf8');
const markers = [...bundle.matchAll(/^__defs\["([A-Za-z][A-Za-z0-9]*)"\] = function \(module, exports, require\) \{\r?\n/gm)];
if (!markers.length) throw new Error('No generated modules found');
const modules = markers.map((m, i) => {
  const end = i + 1 < markers.length ? markers[i + 1].index : bundle.lastIndexOf("module.exports = __load('plugin');");
  const body = bundle.slice(m.index + m[0].length, end).replace(/\r?\n\};\r?\n$/, '');
  if (body === bundle.slice(m.index + m[0].length, end)) throw new Error('Invalid module boundary: ' + m[1]);
  new Function('module', 'exports', 'require', body); // Validate every module before writing any sources.
  return { name: m[1], code: "'use strict';\n" + body };
});
for (const m of modules) fs.writeFileSync(path.join(root, 'src', m.name + '.js'), m.code);
console.log('Recovered', modules.length, 'modules from installed main.js');
