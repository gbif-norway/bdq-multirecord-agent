// Set this once: Project Settings → Script properties → SHARED_SECRET=your_long_random_secret
const SECRET_PROP = 'SHARED_SECRET';

// Verify HMAC-SHA256 over raw body with header X-Signature: sha256=<hex>
function verify_(raw, sigHeader) {
  if (!sigHeader || !sigHeader.startsWith('sha256=')) return false;
  const secret = PropertiesService.getScriptProperties().getProperty(SECRET_PROP);
  if (!secret) return false;
  const hmac = Utilities.computeHmacSha256Signature(raw, secret);
  const hex = hmac.map(b => ('0' + (b & 0xff).toString(16)).slice(-2)).join('');
  return timingSafeEqual(hex, String(sigHeader.slice(7)));
}

function timingSafeEqual(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') return false;
  var lenA = a.length, lenB = b.length;
  var len = Math.max(lenA, lenB);
  var result = 0;
  for (var i = 0; i < len; i++) {
    var ca = i < lenA ? a.charCodeAt(i) : 0;
    var cb = i < lenB ? b.charCodeAt(i) : 0;
    result |= (ca ^ cb);
  }
  return result === 0 && lenA === lenB;
}

// Expected JSON body:
// {
//   "threadId": "GMAIL_THREAD_ID",
//   "htmlBody": "<p>...</p>",
//   "attachments": [{ "filename": "a.csv", "mimeType": "text/csv", "contentBase64": "..." }]
// }
function doPost(e) {
  try {
    // Avoid optional chaining for Rhino compatibility in some Apps Script projects
    const raw = (e && e.postData && e.postData.contents) ? e.postData.contents : '';
    const sig = e.parameter['X-Signature'] || e.parameter['signature'] || (e.headers && e.headers['X-Signature']) || '';
    if (!verify_(raw, sig)) {
      return ContentService.createTextOutput('bad sig').setMimeType(ContentService.MimeType.TEXT);
    }

    const data = JSON.parse(raw);
    try { console.log('doPost: payload meta', { hasThreadId: !!(data && data.threadId), atts: Array.isArray(data && data.attachments) ? data.attachments.length : 0, rawLen: String(raw || '').length }); } catch (eLog) {}
    if (!data || !data.threadId) {
      return ContentService.createTextOutput('bad request').setMimeType(ContentService.MimeType.TEXT);
    }

    const thread = GmailApp.getThreadById(String(data.threadId));
    if (!thread) {
      try { console.log('doPost: no thread for', String(data.threadId)); } catch (eLog2) {}
      return ContentService.createTextOutput('no thread').setMimeType(ContentService.MimeType.TEXT);
    }

    const opts = { htmlBody: String(data.htmlBody || 'Done.') };
    if (Array.isArray(data.attachments) && data.attachments.length) {
      try {
        opts.attachments = data.attachments.map(a =>
          Utilities.newBlob(Utilities.base64Decode(String(a.contentBase64 || '')), String(a.mimeType || 'application/octet-stream'), String(a.filename || 'file.bin'))
        );
      } catch (blobErr) {
        return ContentService.createTextOutput('bad attachment').setMimeType(ContentService.MimeType.TEXT);
      }
    }

    try {
      thread.reply('', opts);
    } catch (replyErr) {
      console.log('reply error', String(replyErr && replyErr.message || replyErr));
      return ContentService.createTextOutput('reply error: ' + String(replyErr && replyErr.message || replyErr))
        .setMimeType(ContentService.MimeType.TEXT);
    }
    return ContentService.createTextOutput('ok').setMimeType(ContentService.MimeType.TEXT);
  } catch (err) {
    console.log('doPost error', String(err && err.message || err));
    return ContentService.createTextOutput('error: ' + String(err && err.message || err)).setMimeType(ContentService.MimeType.TEXT);
  }
}

function doGet() {
  // Touch Gmail scope so Test deployments prompts for authorization
  try {
    var unread = GmailApp.getInboxUnreadCount();
    return ContentService.createTextOutput('ok auth, unread=' + unread).setMimeType(ContentService.MimeType.TEXT);
  } catch (e) {
    return ContentService.createTextOutput('auth error: ' + String(e && e.message || e)).setMimeType(ContentService.MimeType.TEXT);
  }
}
