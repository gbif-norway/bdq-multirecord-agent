// Set this once: Project Settings → Script properties → SHARED_SECRET=your_long_random_secret
const SECRET_PROP = 'SHARED_SECRET';

// Verify HMAC-SHA256 over raw body with header X-Signature: sha256=<hex>
function verify_(raw, sigHeader) {
  if (!sigHeader || !sigHeader.startsWith('sha256=')) return false;
  const secret = PropertiesService.getScriptProperties().getProperty(SECRET_PROP);
  const hmac = Utilities.computeHmacSha256Signature(raw, secret);
  const hex = hmac.map(b => ('0' + (b & 0xff).toString(16)).slice(-2)).join('');
  return Utilities.safeCompare(hex, sigHeader.slice(7));
}

// Expected JSON body:
// {
//   "threadId": "GMAIL_THREAD_ID",
//   "htmlBody": "<p>...</p>",
//   "attachments": [{ "filename": "a.csv", "mimeType": "text/csv", "contentBase64": "..." }]
// }
function doPost(e) {
  const raw = e.postData?.contents || '';
  const ok = verify_(raw, e.parameter['X-Signature'] || e.headers['X-Signature']);
  if (!ok) return ContentService.createTextOutput('bad sig').setMimeType(ContentService.MimeType.TEXT).setResponseCode(401);

  const data = JSON.parse(raw);
  const thread = GmailApp.getThreadById(String(data.threadId));
  if (!thread) return ContentService.createTextOutput('no thread').setResponseCode(404);

  const opts = { htmlBody: data.htmlBody || 'Done.' };
  if (Array.isArray(data.attachments) && data.attachments.length) {
    opts.attachments = data.attachments.map(a =>
      Utilities.newBlob(Utilities.base64Decode(a.contentBase64), a.mimeType || 'application/octet-stream', a.filename || 'file.bin')
    );
  }

  thread.reply('', opts);
  return ContentService.createTextOutput('ok');
}
