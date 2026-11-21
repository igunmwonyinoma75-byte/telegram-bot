import { createHash } from "node:crypto";
import type { DecodedEvent } from "./decode.js";
import { eventHistogram, scanJsonReplacer } from "./events.js";
import { validateLedgerRange, type LedgerRange } from "./ledger-range.js";

export interface ScanJsonlEventLine extends DecodedEvent {
  type: "event";
}

export interface ScanJsonlSummaryLine {
  type: "summary";
  eventCount: number;
  histogram: Record<string, number>;
}

/** Result of compacting a canonical JSONL scan export to an inclusive ledger range. */
export interface CompactScanJsonlResult {
  /** Re-serialized canonical LF JSONL: retained event lines plus one summary line. */
  jsonl: string;
  /** Events retained because their ledger fell inside the requested range. */
  retained: number;
  /** Events dropped because their ledger fell outside the requested range. */
  dropped: number;
}

/** Error thrown when a JSONL export contains a record that is not event or summary. */
export class MalformedScanJsonlRecordError extends Error {
  readonly lineNumber: number;

  constructor(lineNumber: number) {
    super(`Malformed JSONL record at line ${lineNumber}`);
    this.name = "MalformedScanJsonlRecordError";
    this.lineNumber = lineNumber;
  }
}

/**
 * Hash-chained manifest binding ordered records, network and ledger bounds
 * to a single digest. Each record hash commits to the previous record hash,
 * so missing, reordered or altered records fail verification.
 */
export interface ScanJsonlManifest {
  type: "manifest";
  version: 1;
  network: string;
  ledgerMin: number | null;
  ledgerMax: number | null;
  recordCount: number;
  recordHashes: string[];
  chainHead: string;
}

const GENESIS_HASH = "0".repeat(64);

function sha256Hex(data: string): string {
  return createHash("sha256").update(data, "utf8").digest("hex");
}

function chainStep(prevHash: string, recordLine: string): string {
  return sha256Hex(`${prevHash}\n${recordLine}`);
}

function ledgerOf(event: DecodedEvent): number | null {
  const ledger = (event as { ledger?: unknown }).ledger;
  return typeof ledger === "number" && Number.isFinite(ledger) ? ledger : null;
}

/**
 * Format decoded events as canonical LF (`\n`)-delimited JSON Lines (NDJSON):
 * one event line per decoded event followed by a final `{ type: "summary" }`
 * line containing the histogram from {@link eventHistogram}, then a
 * hash-chained manifest line binding the ordered records, network and ledger
 * bounds. Output has a trailing `\n` regardless of host operating system.
 */
export function formatScanJsonl(
  events: readonly DecodedEvent[],
  network = "unknown",
): string {
  const lines: string[] = [];
  for (const event of events) {
    lines.push(JSON.stringify({ type: "event", ...event }, scanJsonReplacer));
  }
  const summary: ScanJsonlSummaryLine = {
    type: "summary",
    eventCount: events.length,
    histogram: eventHistogram([...events]),
  };
  lines.push(JSON.stringify(summary, scanJsonReplacer));

  const recordHashes: string[] = [];
  let head = GENESIS_HASH;
  for (const line of lines) {
    head = chainStep(head, line);
    recordHashes.push(head);
  }

  const ledgers = events.map(ledgerOf).filter((l): l is number => l !== null);
  const manifest: ScanJsonlManifest = {
    type: "manifest",
    version: 1,
    network,
    ledgerMin: ledgers.length ? Math.min(...ledgers) : null,
    ledgerMax: ledgers.length ? Math.max(...ledgers) : null,
    recordCount: lines.length,
    recordHashes,
    chainHead: head,
  };
  lines.push(JSON.stringify(manifest, scanJsonReplacer));
  return lines.join("\n") + "\n";
}

export interface VerifyScanJsonlResult {
  ok: boolean;
  reason?: string;
}

/**
 * Verify a JSONL export produced by {@link formatScanJsonl}. Rejects missing,
 * reordered or altered records, and any mismatch in network or ledger bounds.
 */
export function verifyScanJsonl(text: string): VerifyScanJsonlResult {
  const all = text.split("\n");
  if (all[all.length - 1] === "") all.pop();
  if (all.length < 2) return { ok: false, reason: "missing manifest" };

  const manifestLine = all[all.length - 1];
  const recordLines = all.slice(0, -1);
  let manifest: ScanJsonlManifest;
  try {
    manifest = JSON.parse(manifestLine) as ScanJsonlManifest;
  } catch {
    return { ok: false, reason: "manifest is not valid JSON" };
  }
  if (manifest.type !== "manifest") return { ok: false, reason: "missing manifest" };
  if (manifest.recordCount !== recordLines.length) {
    return { ok: false, reason: "record count mismatch" };
  }
  if (!Array.isArray(manifest.recordHashes) || manifest.recordHashes.length !== recordLines.length) {
    return { ok: false, reason: "record hash count mismatch" };
  }

  let head = GENESIS_HASH;
  for (let i = 0; i < recordLines.length; i++) {
    head = chainStep(head, recordLines[i]);
    if (head !== manifest.recordHashes[i]) {
      return { ok: false, reason: `record ${i} altered, reordered or missing` };
    }
  }
  if (head !== manifest.chainHead) return { ok: false, reason: "chain head mismatch" };

  const events: DecodedEvent[] = [];
  for (const line of recordLines) {
    const parsed = JSON.parse(line) as { type: string };
    if (parsed.type === "event") events.push(parsed as unknown as DecodedEvent);
  }
  const ledgers = events.map(ledgerOf).filter((l): l is number => l !== null);
  const ledgerMin = ledgers.length ? Math.min(...ledgers) : null;
  const ledgerMax = ledgers.length ? Math.max(...ledgers) : null;
  if (ledgerMin !== manifest.ledgerMin || ledgerMax !== manifest.ledgerMax) {
    return { ok: false, reason: "ledger bounds mismatch" };
  }
  return { ok: true };
}

/**
 * Compact a canonical JSONL scan export to an inclusive ledger range.
 *
 * Retains only `{ type: "event" }` records whose `ledger` is within
 * `[firstLedger, lastLedger]`, then rebuilds the trailing `{ type: "summary" }`
 * metadata (`eventCount` and histogram) from the retained events. Any existing
 * summary line is replaced, so a stale summary can never survive compaction.
 * Output is canonical LF JSONL with a single trailing newline.
 *
 * A malformed or unrecognized record aborts the compaction by throwing
 * {@link MalformedScanJsonlRecordError}; input is never silently dropped.
 *
 * @throws {RangeError} when the range is invalid (see `validateLedgerRange`).
 * @throws {MalformedScanJsonlRecordError} on a non-JSON, non-event, or
 * ledger-less record.
 */
export function compactScanJsonl(
  input: string,
  range: LedgerRange,
): CompactScanJsonlResult {
  validateLedgerRange(range);
  const { firstLedger, lastLedger } = range;

  const retainedEvents: DecodedEvent[] = [];
  let dropped = 0;

  const lines = input.split(/\r?\n/);
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index]!;
    const trimmed = line.trim();
    if (!trimmed) continue;

    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      throw new MalformedScanJsonlRecordError(index + 1);
    }

    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new MalformedScanJsonlRecordError(index + 1);
    }

    const record = parsed as Record<string, unknown>;
    if (record.type === "summary" || record.type === "manifest") {
      continue; // rebuilt from the retained events below
    }
    if (record.type !== "event") {
      throw new MalformedScanJsonlRecordError(index + 1);
    }

    const ledger = record.ledger;
    if (typeof ledger !== "number" || !Number.isSafeInteger(ledger)) {
      throw new MalformedScanJsonlRecordError(index + 1);
    }

    if (ledger >= firstLedger && ledger <= lastLedger) {
      retainedEvents.push(parsed as DecodedEvent);
    } else {
      dropped += 1;
    }
  }

  return {
    jsonl: formatScanJsonl(retainedEvents),
    retained: retainedEvents.length,
    dropped,
  };
}