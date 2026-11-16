'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { formatResult, STYLES } = require('../src/notify');
const { makeT } = require('../src/i18n');
test('backend-blocked folders are not presented as successful sync or review-approved deletions', () => {
  for (const language of ['en', 'ar']) for (const style of STYLES) {
    const t = makeT(language);
    const result = formatResult(style, t, { status: 'blocked', uploaded: 1, skipped: 13, errors: 0, failures: [], blockedFolders: [{ path: 'blocked-folder', code: 'UPLOAD_IN_PROGRESS', message: 'The API rejects the completed revision' }] });
    assert.match(result.text, /API/);
    assert.match(result.text, /blocked-folder/);
    assert.ok(!result.text.includes(t('notify.done')));
  }
});
