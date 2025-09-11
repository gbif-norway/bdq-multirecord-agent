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
    const raw = e.postData?.contents || '';
    const sig =
      e.parameter['X-Signature'] ||
      e.parameter['signature'] ||
      (e.headers && e.headers['X-Signature']) ||
      '';

    if (!verify_(raw, sig)) {
      return ContentService.createTextOutput('bad sig')
        .setMimeType(ContentService.MimeType.TEXT);
    }

    let data;
    try {
      data = JSON.parse(raw);
    } catch (_) {
      return ContentService.createTextOutput('bad json')
        .setMimeType(ContentService.MimeType.TEXT);
    }
    if (!data) {
      return ContentService.createTextOutput('bad request')
        .setMimeType(ContentService.MimeType.TEXT);
    }

    const opts = { htmlBody: String(data.htmlBody || 'Done.') };

    if (Array.isArray(data.attachments) && data.attachments.length) {
      try {
        opts.attachments = data.attachments.map(a =>
          Utilities.newBlob(
            Utilities.base64Decode(String(a.contentBase64 || '')),
            String(a.mimeType || 'application/octet-stream'),
            String(a.filename || 'file.bin')
          )
        );
      } catch (blobErr) {
        return ContentService.createTextOutput('bad attachment')
          .setMimeType(ContentService.MimeType.TEXT);
      }
    }

    const threadId = data.threadId ? String(data.threadId) : '';
    if (threadId) {
      try {
        const thread = GmailApp.getThreadById(threadId);
        if (thread) {
          thread.reply('See details below.', opts); // body must be non-empty
          return ContentService.createTextOutput('ok')
            .setMimeType(ContentService.MimeType.TEXT);
        }
      } catch (getErr) {
        console.log('getThreadById error', String(getErr && getErr.message || getErr));
        // fall through
      }
    }

    if (data.to) {
      try {
        const to = String(data.to);
        const subject = String(data.subject || 'BDQ service message');
        GmailApp.sendEmail(to, subject, 'See HTML version.', opts);
        return ContentService.createTextOutput('ok')
          .setMimeType(ContentService.MimeType.TEXT);
      } catch (sendErr) {
        console.log('sendEmail error', String(sendErr && sendErr.message || sendErr));
        return ContentService.createTextOutput('send error: ' + String(sendErr && sendErr.message || sendErr))
          .setMimeType(ContentService.MimeType.TEXT);
      }
    }

    return ContentService.createTextOutput('no thread or to')
      .setMimeType(ContentService.MimeType.TEXT);
  } catch (err) {
    console.log('doPost error', String(err && err.message || err));
    return ContentService.createTextOutput('error: ' + String(err && err.message || err))
      .setMimeType(ContentService.MimeType.TEXT);
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
