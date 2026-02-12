// scripts/addBookUids.ts
// Usage:
//   npx tsx scripts/addBookUids.ts --path "C:\Users\szecs\Documents\marble_app\book_data\books.json"
//   npx tsx scripts/addBookUids.ts -p ./book_data/books.json

import { promises as fs } from 'fs';
import path from 'path';

/* --------- Standalone UID helpers (no app imports needed) --------- */

type AnyBook = Record<string, any>;
const UID_RE = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i;

export function isBookUID(v: unknown): v is string {
  return typeof v === 'string' && UID_RE.test(v);
}

export function ensureBookUID(b: AnyBook): string {
  if (isBookUID(b?.uid)) return b.uid as string;
  // Node 16+ has crypto.randomUUID
  const uid = (globalThis as any).crypto?.randomUUID?.() ?? require('crypto').randomUUID();
  b.uid = uid;
  return uid;
}

/* ------------------------------ CLI utils ------------------------------ */

function parseArgs() {
  const args = process.argv.slice(2);
  let p = '';
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--path' || a === '-p') {
      p = args[i + 1] ?? '';
      i++;
    } else if (!a.startsWith('-') && !p) {
      // allow positional path as well
      p = a;
    }
  }
  return { path: p };
}

async function backupFile(filePath: string) {
  const dir = path.dirname(filePath);
  const base = path.basename(filePath);
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const backup = path.join(dir, `${base}.bak.${stamp}.json`);
  await fs.copyFile(filePath, backup);
  return backup;
}

/* --------------------------------- MAIN --------------------------------- */

async function run() {
  const { path: cliPath } = parseArgs();
  const DEFAULT_PATH = path.resolve('data/books.json');
  const FILE_PATH = path.resolve(cliPath || DEFAULT_PATH);

  console.log(`[addBookUids] Using file: ${FILE_PATH}`);

  try {
    await fs.access(FILE_PATH);
  } catch {
    console.error(
      `[addBookUids] File not found.\n` +
      `  Pass the correct path via --path (or -p).\n` +
      `  Example:\n` +
      `  npx tsx scripts/addBookUids.ts --path "C:\\Users\\szecs\\Documents\\marble_app\\book_data\\books.json"`
    );
    process.exit(1);
  }

  const raw = await fs.readFile(FILE_PATH, 'utf8');
  let data: any;
  try {
    data = JSON.parse(raw);
  } catch {
    console.error(`[addBookUids] Invalid JSON in ${FILE_PATH}`);
    process.exit(1);
  }

  // Support either an array of books or { books: [...] }
  const books: AnyBook[] = Array.isArray(data) ? data : Array.isArray(data?.books) ? data.books : [];
  if (!Array.isArray(books) || books.length === 0) {
    console.log('[addBookUids] No books found to process.');
    process.exit(0);
  }

  let added = 0;
  for (const b of books) {
    if (!isBookUID(b?.uid)) {
      ensureBookUID(b);
      added++;
    }
  }

  const backup = await backupFile(FILE_PATH);
  const out = Array.isArray(data) ? books : { ...data, books };
  await fs.writeFile(FILE_PATH, JSON.stringify(out, null, 2) + '\n', 'utf8');

  console.log(`[addBookUids] Backup written: ${backup}`);
  console.log(`[addBookUids] Done. Added UIDs to ${added} of ${books.length} book(s).`);
}

run().catch((e) => {
  console.error('[addBookUids] Unexpected error:', e);
  process.exit(1);
});
