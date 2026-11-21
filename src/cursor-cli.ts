/*
 * Offline cursor backup, restore, history, and consistency-check command. It
 * never contacts Stellar or Telegram; restore uses the normal instance lock to
 * exclude a running poller. `history` and `check` are read-only.
 */

import { randomUUID } from "node:crypto";
import { access, link, mkdir, readFile, realpath, rename, stat, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { config as loadDotenv } from "dotenv";
import { pathToFileURL } from "node:url";

import {
  checkCursorConsistency,
  formatCursorConsistencyReport,
  type CursorConsistencyReport,
} from "./cursor-consistency.js";
import { acquireInstanceLock } from "./instanceLock.js";
import { readArchive } from "./stellar/archive.js";
import {
  appendCursorRecoveryHistory,
  parseAndMigrateCursorFile,
  snapshotCursorPositions,
  type CursorRecoveryPosition,
  type CursorRecoveryRecord,
} from "./poller.js";

export interface BackupCursorOptions {
  cursorFile: string;
  backupFile: string;
  force?: boolean;
}

export interface RestoreCursorOptions extends BackupCursorOptions {
  lockFile: string;
  now?: () => Date;
}

async function exists(filePath: string): Promise<boolean> {
  try {
    await access(filePath);
    return true;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw err;
  }
}

function validateCursor(raw: string): ReturnType<typeof parseAndMigrateCursorFile> {
  try {
    return parseAndMigrateCursorFile(raw);
  } catch {
    throw new Error("cursor file is malformed or uses an unsupported schema");
  }
}

async function writeAtomic(filePath: string, contents: string, replace: boolean): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
  const temporaryPath = `${filePath}.tmp-${process.pid}-${randomUUID()}`;

  try {
    await writeFile(temporaryPath, contents, { encoding: "utf8", flag: "wx" });
    if (replace) {
      await rename(temporaryPath, filePath);
    } else {
      try {
        await link(temporaryPath, filePath);
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code === "EEXIST") {
          throw new Error(`destination already exists at ${filePath}; pass --force to overwrite`);
        }
        throw err;
      }
    }
  } finally {
    await unlink(temporaryPath).catch(() => undefined);
  }
}

/** Copy a validated supported cursor snapshot without exposing cursor values. */
export async function backupCursor(options: BackupCursorOptions): Promise<void> {
  await assertDifferentPaths(options.cursorFile, options.backupFile);
  const raw = await readFile(options.cursorFile, "utf8");
  const parsed = validateCursor(raw);
  if (!options.force && (await exists(options.backupFile))) {
    throw new Error(`backup already exists at ${options.backupFile}; pass --force to overwrite`);
  }
  await writeAtomic(options.backupFile, raw, options.force === true);
  console.log(
    `[cursor] backed up schema=v` + `${parsed.file.version} targets=${Object.keys(parsed.file.targets).length} ` +
      `from ${options.cursorFile} to ${options.backupFile}`,
  );
}

/** Restore a compatible cursor while holding the poller's exclusive lock. */
export async function restoreCursor(options: RestoreCursorOptions): Promise<void> {
  await assertDifferentPaths(options.cursorFile, options.backupFile);
  const lock = await acquireInstanceLock(options.lockFile);
  try {
    const raw = await readFile(options.backupFile, "utf8");
    const parsed = validateCursor(raw);
    const liveExists = await exists(options.cursorFile);
    if (!options.force && liveExists) {
      throw new Error(`cursor already exists at ${options.cursorFile}; pass --force to replace it`);
    }

    let before: Record<string, CursorRecoveryPosition> = {};
    let priorHistory: CursorRecoveryRecord[] = parsed.file.recoveryHistory ?? [];
    if (liveExists) {
      try {
        const liveRaw = await readFile(options.cursorFile, "utf8");
        const liveParsed = parseAndMigrateCursorFile(liveRaw);
        before = snapshotCursorPositions(liveParsed.file.targets);
        priorHistory = liveParsed.file.recoveryHistory ?? [];
      } catch {
        // Unreadable or corrupt live cursor: record empty before-positions
        // without copying raw bytes or secrets into recovery history.
        before = {};
        priorHistory = [];
      }
    }

    const after = snapshotCursorPositions(parsed.file.targets);
    const recoveryHistory = appendCursorRecoveryHistory(priorHistory, {
      at: (options.now ?? (() => new Date()))().toISOString(),
      reason: "restore",
      before,
      after,
    });
    const nextFile = {
      ...parsed.file,
      recoveryHistory,
    };

    const normalized = `${JSON.stringify(nextFile, null, 2)}\n`;
    await writeAtomic(options.cursorFile, normalized, options.force === true);
    console.log(
      `[cursor] restored schema=v${nextFile.version} targets=${Object.keys(nextFile.targets).length} ` +
        `from ${options.backupFile} to ${options.cursorFile}`,
    );
  } finally {
    await lock.release();
  }
}

function summarizePositions(positions: Record<string, CursorRecoveryPosition>): string {
  const entries = Object.entries(positions);
  if (entries.length === 0) return "none";
  return entries
    .map(([source, pos]) => {
      const ledger =
        typeof pos.rewindFromLedger === "number"
          ? `rewind@${pos.rewindFromLedger}`
          : pos.lastEventLedger !== null
            ? `ledger=${pos.lastEventLedger}`
            : pos.cursor !== null
              ? "cursor=set"
              : "none";
      return `${source}:${ledger}`;
    })
    .join(",");
}

/** Read and print the bounded recovery history from a validated cursor file. */
export async function readCursorRecoveryHistory(cursorFile: string): Promise<CursorRecoveryRecord[]> {
  const raw = await readFile(cursorFile, "utf8");
  const parsed = validateCursor(raw);
  return parsed.file.recoveryHistory ?? [];
}

/**
 * Load scan-archive records without modifying them. A directory is read as an
 * event archive (`events-*.jsonl`, see `createEventArchive`); a file is read as
 * a `npm run scan -- --jsonl` export, skipping its `{ "type": "summary" }` line.
 */
export async function readScanArchiveRecords(archivePath: string): Promise<unknown[]> {
  let info;
  try {
    info = await stat(archivePath);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(`archive not found at ${archivePath}`);
    }
    throw err;
  }

  const records: unknown[] = [];
  if (info.isDirectory()) {
    for await (const event of readArchive(archivePath)) records.push(event);
    return records;
  }

  const raw = await readFile(archivePath, "utf8");
  const lines = raw.split(/\r?\n/);
  for (let index = 0; index < lines.length; index += 1) {
    const trimmed = lines[index]!.trim();
    if (!trimmed) continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      throw new Error(`malformed archive record at line ${index + 1}`);
    }
    if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
      const { type, ...rest } = parsed as Record<string, unknown>;
      if (type === "summary") continue;
      records.push(type === undefined || type === "event" ? rest : parsed);
      continue;
    }
    records.push(parsed);
  }
  return records;
}

export interface CheckCursorOptions {
  cursorFile: string;
  archivePath: string;
  /** Contract id each source is expected to belong to (e.g. `{ market: MARKET_CONTRACT_ID }`). */
  expectedContracts?: Record<string, string | undefined>;
}

/**
 * Read-only consistency check of a cursor file against a scan archive. Takes
 * no instance lock and writes nothing: neither the cursor nor the archive is
 * modified, whatever the result.
 */
export async function checkCursor(options: CheckCursorOptions): Promise<CursorConsistencyReport> {
  const raw = await readFile(options.cursorFile, "utf8");
  const parsed = validateCursor(raw);
  const records = await readScanArchiveRecords(options.archivePath);
  return checkCursorConsistency(parsed.file, records, {
    expectedContracts: options.expectedContracts,
  });
}

interface CliOptions {
  file?: string;
  out?: string;
  from?: string;
  archive?: string;
  "market-contract"?: string;
  "squad-contract"?: string;
  force: boolean;
  json: boolean;
}

const VALUE_OPTIONS = ["--file", "--out", "--from", "--archive", "--market-contract", "--squad-contract"] as const;
type ValueOption = (typeof VALUE_OPTIONS)[number];

function parseOptions(args: string[]): CliOptions {
  const options: CliOptions = { force: false, json: false };
  const seen = new Set<string>();

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--force" || arg === "--json") {
      if (seen.has(arg)) throw new Error(`duplicate ${arg} option`);
      seen.add(arg);
      options[arg.slice(2) as "force" | "json"] = true;
      continue;
    }
    if (!VALUE_OPTIONS.includes(arg as ValueOption)) {
      throw new Error(`unknown option: ${arg}`);
    }
    if (seen.has(arg!)) throw new Error(`duplicate ${arg} option`);
    seen.add(arg!);
    const value = args[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`${arg} requires ${arg!.endsWith("-contract") ? "a contract id" : "a path"}`);
    }
    options[arg!.slice(2) as Exclude<keyof CliOptions, "force" | "json">] = value;
    index += 1;
  }

  return options;
}

function resolvePath(value: string): string {
  return path.resolve(process.cwd(), value);
}

async function canonicalizePath(filePath: string): Promise<string> {
  const resolved = path.resolve(filePath);
  const normalizeCase = (value: string) =>
    process.platform === "win32" ? value.toLowerCase() : value;

  try {
    return normalizeCase(await realpath(resolved));
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
      throw err;
    }
  }

  // If the leaf does not exist yet, resolve symlinks on the nearest existing ancestor.
  const segments: string[] = [];
  let current = resolved;
  for (;;) {
    const parent = path.dirname(current);
    if (parent === current) {
      return normalizeCase(resolved);
    }
    segments.unshift(path.basename(current));
    current = parent;
    try {
      const realParent = await realpath(current);
      return normalizeCase(path.join(realParent, ...segments));
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
        throw err;
      }
    }
  }
}

export async function assertDifferentPaths(left: string, right: string): Promise<void> {
  const [canonicalLeft, canonicalRight] = await Promise.all([
    canonicalizePath(left),
    canonicalizePath(right),
  ]);
  if (canonicalLeft === canonicalRight) {
    throw new Error("backup and live cursor paths must be different");
  }
}

async function main(): Promise<void> {
  loadDotenv({ path: process.env.DOTENV_CONFIG_PATH || ".env" });
  const [action, ...args] = process.argv.slice(2);
  if (action !== "backup" && action !== "restore" && action !== "history" && action !== "check") {
    throw new Error(
      "usage: npm run cursor -- <backup|restore|history|check> [--file PATH] [--out PATH|--from PATH] [--force] " +
        "[--archive PATH [--json] [--market-contract ID] [--squad-contract ID]]",
    );
  }

  const options = parseOptions(args);
  const cursorFile = resolvePath(options.file ?? process.env.CURSOR_FILE ?? "./data/cursor.json");
  if (action === "check") {
    if (!options.archive) throw new Error("check requires --archive PATH");
    const report = await checkCursor({
      cursorFile,
      archivePath: resolvePath(options.archive),
      expectedContracts: {
        market: options["market-contract"] ?? (process.env.MARKET_CONTRACT_ID || undefined),
        squad: options["squad-contract"] ?? (process.env.SQUAD_CONTRACT_ID || undefined),
      },
    });
    process.stdout.write(
      options.json ? `${JSON.stringify(report, null, 2)}\n` : formatCursorConsistencyReport(report),
    );
    // Non-zero on any error finding so scripts can gate on it; nothing was written.
    if (!report.consistent) process.exitCode = 1;
    return;
  }
  if (action === "history") {
    const history = await readCursorRecoveryHistory(cursorFile);
    console.log(`[cursor] recovery history (${history.length}) for ${cursorFile}`);
    for (const record of history) {
      const targetPart = record.source ? ` source=${record.source}` : "";
      console.log(
        `  ${record.at} reason=${record.reason}${targetPart} ` +
          `before=[${summarizePositions(record.before)}] after=[${summarizePositions(record.after)}]`,
      );
    }
    return;
  }

  if (action === "backup") {
    const backupFile = resolvePath(
      options.out ?? `${cursorFile}.${new Date().toISOString().replace(/[:.]/g, "-")}.bak`,
    );
    await assertDifferentPaths(cursorFile, backupFile);
    await backupCursor({ cursorFile, backupFile, force: options.force });
    return;
  }

  if (!options.from) throw new Error("restore requires --from PATH");
  const backupFile = resolvePath(options.from);
  await assertDifferentPaths(cursorFile, backupFile);
  const lockFile = resolvePath(process.env.INSTANCE_LOCK_FILE ?? "./data/poller.lock");
  await restoreCursor({
    cursorFile,
    backupFile,
    lockFile,
    force: options.force,
  });
}

const invokedPath = process.argv[1];
if (invokedPath && import.meta.url === pathToFileURL(invokedPath).href) {
  main().catch((err: unknown) => {
    const message = err instanceof Error ? err.message : "cursor command failed";
    console.error(`[cursor] ${message.replace(/\s+/g, " ").slice(0, 300)}`);
    process.exit(1);
  });
}
