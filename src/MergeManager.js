'use strict';
// A compact, dependency-free 3-way line merge (diff3-style). Used for text conflicts: given the last
// synced base plus the local and remote versions, produce a merged text when the edits do not overlap.
// Overlapping edits that disagree are reported as conflicts (region markers included).

function splitLines(s) { return String(s).split('\n'); }

/** Longest common subsequence match pairs [iInA, iInB], increasing in both. O(n*m), fine for notes. */
function lcsMatch(a, b) {
  const n = a.length; const m = b.length;
  // Trim common prefix/suffix first so the DP table stays small for typical edits.
  let p = 0; while (p < n && p < m && a[p] === b[p]) p++;
  let s = 0; while (s < n - p && s < m - p && a[n - 1 - s] === b[m - 1 - s]) s++;
  const pairs = [];
  for (let i = 0; i < p; i++) pairs.push([i, i]);
  const an = n - s; const bn = m - s;
  if (an > p && bn > p) {
    const rows = an - p; const cols = bn - p;
    const dp = Array.from({ length: rows + 1 }, () => new Int32Array(cols + 1));
    for (let i = rows - 1; i >= 0; i--) {
      for (let j = cols - 1; j >= 0; j--) {
        dp[i][j] = a[p + i] === b[p + j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
      }
    }
    let i = 0; let j = 0;
    while (i < rows && j < cols) {
      if (a[p + i] === b[p + j]) { pairs.push([p + i, p + j]); i++; j++; }
      else if (dp[i + 1][j] >= dp[i][j + 1]) i++;
      else j++;
    }
  }
  for (let k = s - 1; k >= 0; k--) pairs.push([n - 1 - k, m - 1 - k]);
  pairs.sort((x, y) => x[0] - y[0] || x[1] - y[1]);
  return pairs;
}

const eq = (a, b) => a.length === b.length && a.every((x, i) => x === b[i]);

/** Merge three texts. Returns { ok, text, conflicts }. When not ok, `text` still holds the best-effort
 *  result with Git-style conflict markers for the user to resolve by hand. */
function merge3(baseStr, localStr, remoteStr) {
  const base = splitLines(baseStr); const local = splitLines(localStr); const remote = splitLines(remoteStr);
  const ab = lcsMatch(base, local);      // base index -> local index
  const rb = lcsMatch(base, remote);     // base index -> remote index
  const inA = new Map(ab); const inR = new Map(rb);

  // Anchors: base lines matched in BOTH sides (unchanged by both).
  const anchors = [];
  for (const [bi, li] of ab) { const ri = inR.get(bi); if (ri !== undefined) anchors.push([bi, li, ri]); }
  anchors.sort((x, y) => x[0] - y[0]);

  const out = []; let conflicts = 0;
  let bp = 0; let lp = 0; let rp = 0;
  const emit = (bEnd, lEnd, rEnd) => {
    const bl = base.slice(bp, bEnd); const ll = local.slice(lp, lEnd); const rl = remote.slice(rp, rEnd);
    if (eq(ll, bl)) out.push(...rl);
    else if (eq(rl, bl)) out.push(...ll);
    else if (eq(ll, rl)) out.push(...ll);
    else {
      conflicts++;
      out.push('<<<<<<< local', ...ll, '=======', ...rl, '>>>>>>> remote');
    }
    bp = bEnd; lp = lEnd; rp = rEnd;
  };
  for (const [bi, li, ri] of anchors) { emit(bi, li, ri); out.push(base[bi]); bp = bi + 1; lp = li + 1; rp = ri + 1; }
  emit(base.length, local.length, remote.length);
  return { ok: conflicts === 0, text: out.join('\n'), conflicts };
}

module.exports = { merge3, lcsMatch };
