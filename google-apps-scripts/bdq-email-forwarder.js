/**
 * Gmail → HTTPS forwarder (polls unprocessed top-level emails and POSTs JSON to your endpoint).
 *
 * Setup:
 * 1) In Apps Script editor: Services → enable "Gmail API" (Advanced Gmail Service).
 * 2) Edit ENDPOINT_URL and SHARED_SECRET below.
 * 3) Triggers → Add Trigger: choose poll, Event source: Time-driven, Every minute.
 * 4) First run: execute init() once to ensure the processed label exists.
 */

const ENDPOINT_URL = 'https://bdq-multirecord-agent-638241344017.europe-west1.run.app/email/incoming';
const SHARED_SECRET = ''; // optional HMAC; set '' to disable
const LABEL_PROCESSED = 'bdq/processed';
const MAX_PER_RUN = 50; // safety cap per minute

function log() {
  try {
    const parts = Array.prototype.slice.call(arguments).map(a =>
      typeof a === 'string' ? a : JSON.stringify(a)
    );
    console.log(parts.join(' '));
  } catch (e) {}
}

function init() {
  ensureLabel(LABEL_PROCESSED);
  log('init:processed label ensured');
}

function poll() {
  log('poll:start', new Date().toISOString());
  const ids = findUnprocessedIds_();
  log('poll:found', ids.length, 'unprocessed emails');
  if (ids.length) processMessageIds_(ids);
  log('poll:end');
}

/* ---------------- helpers ---------------- */

function replyNoAttachment_(message, headers) {
  const thread = GmailApp.getThreadById(message.threadId);
  const note =
    "Hi, thanks for emailing in!\n\n" +
    "This service is for running BDQ tests (see https://github.com/tdwg/bdq) against Darwin Core datasets." +
    "\nThis is an automated message as no attachment was found in your previous email. " +
    "\nPlease send a NEW email with your data file attached (a Darwin Core occurrence or taxon TXT/CSV/TSV).\n" +
    "Replies without an attachment won’t be processed.\n";
  thread.reply(note);
}

function processMessageIds_(ids) {
  const labelIdProcessed = ensureLabel(LABEL_PROCESSED);
  log('process:start count', ids.length);

  for (const id of ids) {
    try {
      const meta = Gmail.Users.Messages.get('me', id, { format: 'minimal' });
      if ((meta.labelIds || []).includes(labelIdProcessed)) {
        log('process:skip already processed', id);
        continue;
      }

      const full = Gmail.Users.Messages.get('me', id, { format: 'full' });

      if (!isTopLevel_(full)) {
        log('process:skip reply/non-top-level', id);
        continue;
      }

      const payload = buildPayload_(full);
      const hdr = payload.headers || {};
      const attCount = (payload.attachments || []).length;
      log('process:msg', { id, subj: hdr.subject || '', from: hdr.from || '', attCount });

      let ok = true;
      if (attCount === 0) {
        log('process:reply no attachment', id);
        replyNoAttachment_(full, hdr);
      } else {
        ok = postToEndpoint_(payload);
      }

      if (ok) {
        Gmail.Users.Messages.modify({ addLabelIds: [labelIdProcessed] }, 'me', id);
        log('process:labeled processed', id);
      }
    } catch (e) {
      log('process:error', { id, error: String(e && e.message || e) });
    }
  }
  log('process:end');
}

function findUnprocessedIds_() {
  const q = `-label:"${LABEL_PROCESSED}" -in:spam -in:trash`;
  const out = [];
  let pageToken = null;

  do {
    const res = Gmail.Users.Messages.list('me', { q, maxResults: 100, pageToken });
    if (res.messages) out.push(...res.messages.map(m => m.id));
    pageToken = res.nextPageToken || null;
  } while (pageToken && out.length < MAX_PER_RUN);

  return out.slice(0, MAX_PER_RUN);
}

function isTopLevel_(full) {
  try {
    const headers = indexHeaders_(full.payload && full.payload.headers || []);
    if (headers['In-Reply-To'] || headers['References']) return false;

    const thr = Gmail.Users.Threads.get('me', full.threadId);
    const msgs = thr.messages || [];
    if (!msgs.length) return true;

    let first = msgs[0];
    for (const m of msgs) {
      if (Number(m.internalDate) < Number(first.internalDate)) first = m;
    }
    return full.id === first.id;
  } catch (e) {
    const headers = indexHeaders_(full.payload && full.payload.headers || []);
    return !(headers['In-Reply-To'] || headers['References']);
  }
}

function ensureLabel(name) {
  const labelsRes = Gmail.Users.Labels.list('me');
  const found = (labelsRes.labels || []).find(l => l.name === name);
  if (found) return found.id;
  const created = Gmail.Users.Labels.create({
    name,
    labelListVisibility: 'labelShow',
    messageListVisibility: 'show'
  }, 'me');
  return created.id;
}

function buildPayload_(message) {
  const headers = indexHeaders_(message.payload.headers || []);
  const parts = flattenParts_(message.payload);
  const { text, html } = extractBodies_(parts, message.id);

  const attachments = [];
  for (const p of parts) {
    if (p.filename && p.body && p.body.attachmentId) {
      const att = Gmail.Users.Messages.Attachments.get('me', message.id, p.body.attachmentId);
      let dataStr = (att && typeof att.data === 'string') ? att.data : '';
      let usedFallback = false;
      let fallbackSize = 0;
      if (!dataStr) {
        try {
          const msg = GmailApp.getMessageById(String(message.id));
          const blobs = msg.getAttachments({ includeInlineImages: true, includeAttachments: true });
          const match = blobs.find(b => {
            try {
              return b && b.getName && b.getName() === p.filename;
            } catch (e) { return false; }
          });
          if (match) {
            dataStr = Utilities.base64Encode(match.getBytes());
            fallbackSize = match.getBytes().length;
            usedFallback = true;
          }
        } catch (e) {
          log('attach:fallback error', String(e && e.message || e));
        }
      }
      log('attach:get', { filename: p.filename, size: (att && att.size) || fallbackSize || 0, hasData: !!dataStr, fallback: usedFallback });
      attachments.push({
        filename: p.filename,
        mimeType: p.mimeType || 'application/octet-stream',
        size: (att && att.size) || fallbackSize || 0,
        contentBase64: dataStr
      });
    }
  }

  return {
    receivedAt: new Date().toISOString(),
    messageId: message.id,
    threadId: message.threadId,
    historyId: message.historyId,
    labelIds: message.labelIds || [],
    snippet: message.snippet || '',
    headers: {
      subject: headers['Subject'] || '',
      from: headers['From'] || '',
      to: headers['To'] || '',
      cc: headers['Cc'] || '',
      date: headers['Date'] || '',
      messageId: headers['Message-Id'] || '',
      inReplyTo: headers['In-Reply-To'] || '',
      references: headers['References'] || ''
    },
    body: { text, html },
    attachments
  };
}

function indexHeaders_(arr) {
  const map = {};
  for (const h of arr) map[h.name] = h.value;
  return map;
}

function flattenParts_(node) {
  const out = [];
  function walk(p) {
    if (!p) return;
    if (p.parts && p.parts.length) p.parts.forEach(walk);
    else out.push(p);
  }
  walk(node);
  return out;
}

function extractBodies_(parts, messageId) {
  let text = '', html = '';
  for (const p of parts) {
    const mime = (p.mimeType || '').toLowerCase();
    const isText = mime.startsWith('text/plain');
    const isHtml = mime.startsWith('text/html');
    if (!isText && !isHtml) continue;

    let dataStr = (p.body && typeof p.body.data === 'string') ? p.body.data : null;
    if (!dataStr && p.body && p.body.attachmentId) {
      const att = Gmail.Users.Messages.Attachments.get('me', messageId, p.body.attachmentId);
      if (att && typeof att.data === 'string') dataStr = att.data;
    }
    if (!dataStr) continue;
    const decoded = decodeBase64Url_(dataStr);
    if (isText) text = decoded;
    if (isHtml) html = decoded;
  }
  return { text, html };
}

function decodeBase64Url_(data) {
  if (typeof data !== 'string' || !data) return '';
  let s = data.replace(/-/g, '+').replace(/_/g, '/');
  const pad = s.length % 4;
  if (pad) s += '='.repeat(4 - pad);
  const bytes = Utilities.base64Decode(s);
  return Utilities.newBlob(bytes).getDataAsString('UTF-8');
}

function postToEndpoint_(payload) {
  const json = JSON.stringify(payload);
  const headers = {
    'Content-Type': 'application/json',
    'X-Gmail-Message-Id': payload.messageId || '',
    'X-Gmail-Thread-Id': payload.threadId || '',
    'X-From': (payload.headers && payload.headers.from) || '',
    'X-Subject': (payload.headers && payload.headers.subject) || ''
  };
  if (SHARED_SECRET) {
    const sigBytes = Utilities.computeHmacSha256Signature(json, SHARED_SECRET);
    headers['X-Signature'] = Utilities.base64Encode(sigBytes);
  }
  try {
    const res = UrlFetchApp.fetch(ENDPOINT_URL, {
      method: 'post',
      muteHttpExceptions: true,
      payload: json,
      headers,
      followRedirects: true,
      contentType: 'application/json'
    });
    const code = res.getResponseCode();
    if (code < 200 || code >= 300) {
      const text = (res.getContentText() || '').slice(0, 500);
      log('post:non2xx', code, text);
    } else {
      log('post:ok', code);
    }
    return code >= 200 && code < 300;
  } catch (e) {
    log('post:error', String(e && e.message || e));
    return false;
  }
}
