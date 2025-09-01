// Set this once: Project Settings → Script properties → SHARED_SECRET=your_long_random_secret
const SECRET_PROP = 'SHARED_SECRET';

// Verify HMAC-SHA256 over raw body with header X-Signature: sha256=<hex>
function verify_(raw, sigHeader) {
  if (!sigHeader || !sigHeader.startsWith('sha256=')) return false;
  const secret = PropertiesService.getScriptProperties().getProperty(SECRET_PROP);
  if (!secret) return false;
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
  try {
    const raw = e.postData?.contents || '';
    const sig = e.parameter['X-Signature'] || e.parameter['signature'] || (e.headers && e.headers['X-Signature']) || '';
    if (!verify_(raw, sig)) {
      return ContentService.createTextOutput('bad sig').setMimeType(ContentService.MimeType.TEXT).setResponseCode(401);
    }

    const data = JSON.parse(raw);
    if (!data || !data.threadId) {
      return ContentService.createTextOutput('bad request').setMimeType(ContentService.MimeType.TEXT).setResponseCode(400);
    }

    const thread = GmailApp.getThreadById(String(data.threadId));
    if (!thread) return ContentService.createTextOutput('no thread').setMimeType(ContentService.MimeType.TEXT).setResponseCode(404);

    const opts = { htmlBody: String(data.htmlBody || 'Done.') };
    if (Array.isArray(data.attachments) && data.attachments.length) {
      try {
        opts.attachments = data.attachments.map(a =>
          Utilities.newBlob(Utilities.base64Decode(String(a.contentBase64 || '')), String(a.mimeType || 'application/octet-stream'), String(a.filename || 'file.bin'))
        );
      } catch (blobErr) {
        return ContentService.createTextOutput('bad attachment').setMimeType(ContentService.MimeType.TEXT).setResponseCode(400);
      }
    }

    thread.reply('', opts);
    return ContentService.createTextOutput('ok').setMimeType(ContentService.MimeType.TEXT).setResponseCode(200);
  } catch (err) {
    return ContentService.createTextOutput('error').setMimeType(ContentService.MimeType.TEXT).setResponseCode(500);
  }
}

function doGet() {
  return ContentService.createTextOutput('ok').setMimeType(ContentService.MimeType.TEXT).setResponseCode(200);
}
