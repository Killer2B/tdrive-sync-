# تقرير الاختبارات — Tdrive Sync 1.0.0

## ما نُفِّذ فعليًا
تشغيل `npm test` (Node v22) مقابل **خادم Tdrive وهمي** في `test/mockServer.js` يطبّق قائمة المسارات الموثقة بأشكال JSON **مفترضة**، مع حقن أعطال حقيقية (قطع الاتصال، 429، 500). النتيجة: **36 نجاح، 0 فشل**.

```
PASS  1a auth success
PASS  1b auth failure: 401, not retried, key never in message
PASS  1c client refuses http:// unless explicitly allowed, and empty keys
PASS  1d key only goes to the configured origin and is redacted from logs
PASS  4 retries after HTTP 429 honoring Retry-After
PASS  4b 5xx retried with exponential backoff, then gives up with a retryable error
PASS  2 network drop during resumable upload recovers via retry; bytes identical
PASS  13 interrupted upload resumes: only missing parts are re-sent
PASS  2b a single accepted part is not treated as a complete upload
PASS  3 network drop during ranged download recovers from the offset
PASS  12 empty file round-trips through client
PASS  built main.js loads, registers commands, tests connection, syncs both ways
PASS  6 new local file is uploaded (folders created, remote bytes identical)
PASS  7 new remote file is downloaded
PASS  8 modifications on either side propagate (each direction), for all duplicate-name policies
PASS  9 edited on both sides -> conflict: nothing overwritten, copy kept, path frozen
PASS  9b resolve "both": local stays, remote copy kept, converges
PASS  9c resolve "remote": local version preserved as backup, then remote applied
PASS  10a local deletion is propagated to the Tdrive trash only (recoverable); not re-created
PASS  10b remote deletion moves local file to trash; later local edit of a remotely-deleted file re-uploads
PASS  10c mass deletion is blocked (limit) and empty-vault guard blocks deletions
PASS  10d deletion mode "off" never deletes
PASS  10e lost/old ledger never deletes healthy files and does not duplicate
PASS  11 Arabic names, spaces, similar names: round-trip to a second device
PASS  12 empty and large (resumable) files sync both ways
PASS  5 partial failure: other files still sync, status is not "ok", failed file retried next run
PASS  2c repeated network failures abort the run as "offline" without touching data
PASS  1e bad key aborts the whole run before any change
PASS  14 an existing local file is never overwritten without a ledger entry
PASS  14b local edit that lands during a download is not overwritten
PASS  15 running sync twice with no changes performs no writes and no re-upload
PASS  exclusions: hidden folders and patterns are never uploaded
PASS  upload-only mode never downloads or deletes
PASS  pause stops the run between files and leaves the ledger consistent
PASS  decide(): decision table
PASS  corrupt ledger resets safely
# tests 36
# pass 36
# fail 0
```

مغطى من قائمة المتطلبات: 1 ✓، 2 ✓، 3 ✓، 4 ✓، 5 ✓، 6 ✓، 7 ✓، 8 ✓، 9 ✓، 10 ✓، 11 ✓، 12 ✓، 13 ✓، 14 ✓، 15 ✓ — **جميعها ضد الخادم الوهمي فقط**.

## ما لم يُنفَّذ (بصراحة)
- **اختبارات التكامل مع Tdrive الحقيقي**: لم تُنفَّذ (لا مفتاح اختبار، ولا وصول شبكة للأداة). لذلك أشكال JSON وبعض السلوكيات (انظر `API-CONTRACT.md` ← "افتراضات") **غير مؤكدة**.
- **التشغيل داخل Obsidian فعليًا على Android وWindows**: لم يُجرَّب. ما اختُبر: تحميل `main.js` المبني مقابل واجهة Obsidian **مُحاكاة** (تسجيل الأوامر، بناء الإعدادات، المزامنة عبر محوّل الملفات). عرض `SyncStatusView` وLocalFS على أجهزة حقيقية لم يُختبرا.
- المزامنة التلقائية عند الخلفية في أندرويد: Obsidian يوقف JS في الخلفية؛ الفاصل الزمني يعمل أثناء فتح التطبيق فقط.
- أداء الحجوم الكبيرة جدًا (آلاف الملفات / ملفات بمئات الميغابايت): لم يُقَس. الرفع الكبير يُحمَّل كاملًا في الذاكرة (قيد requestUrl)، فقد يتعثر على أجهزة بذاكرة قليلة.
