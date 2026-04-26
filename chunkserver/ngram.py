import array
from collections import defaultdict

from chunkserver.varint import encodePositions, decodePositions


# A query trigram with > STOP_TRIGRAM_BYTES bytes of postings is "too common"
# to help narrow candidates — its decode + set-intersection cost outweighs the
# filtering it provides. Skip it (or fall back to linear scan if the rarest
# query trigram exceeds this).
STOP_TRIGRAM_BYTES = 50_000


class NGramIndex:
    """Char-trigram index for substring search inside a single chunk.
    Storage shape: chunkHandle -> {trigram: delta-varint position bytes}.
    Trigrams are 3-char overlapping windows on the lowercased text.
    Positions are char offsets into the lowercased text.
    """

    index: dict[str, dict[str, bytes]]

    def __init__(self):
        self.index = {}

    def tokenizeChunk(self, chunkHandle: str, text: str):
        """Pure function - returns trigram -> positions_bytes for this chunk. Safe without lock."""
        lower = text.lower()
        # array.array('i') stores raw 4-byte ints; list[int] would box every
        # position into a 28-byte Python int and blow up peak memory on big chunks.
        positions = defaultdict(lambda: array.array("i"))
        for i in range(len(lower) - 2):
            positions[lower[i:i + 3]].append(i)
        return {trigram: encodePositions(pos) for trigram, pos in positions.items()}

    def mergeChunk(self, chunkHandle: str, trigrams: dict):
        self.index[chunkHandle] = trigrams

    def indexChunk(self, chunkHandle: str, text: str):
        self.mergeChunk(chunkHandle, self.tokenizeChunk(chunkHandle, text))

    def removeChunk(self, chunkHandle: str):
        self.index.pop(chunkHandle, None)

    def candidatePositions(self, chunkHandle: str, query: str):
        """Returns candidate start positions in lowercased(text) where `query`
        may begin. Three states:
          None  -> fall back to linear scan (query <3 chars, chunk not indexed,
                   or every query trigram too common to be useful)
          []    -> definitively no matches
          [...] -> sorted candidates; caller still verifies with regex
        """
        if len(query) < 3:
            return None
        chunkTrigrams = self.index.get(chunkHandle)
        if chunkTrigrams is None:
            return None

        q = query.lower()

        # Look up every query trigram up front. If any is absent, no match
        # is possible regardless of stop-trigram skipping.
        entries = []
        for k in range(len(q) - 2):
            trigram = q[k:k + 3]
            posBytes = chunkTrigrams.get(trigram)
            if posBytes is None:
                return []
            entries.append((k, posBytes))

        # Sort rarest-first using varint byte length as a cheap proxy for
        # position count (avoids decoding to count).
        entries.sort(key=lambda e: len(e[1]))

        # If even the rarest query trigram is too common, the trigram path is
        # a net loss — caller should fall back to linear regex.
        if len(entries[0][1]) > STOP_TRIGRAM_BYTES:
            return None

        candidates = None
        for i, (k, posBytes) in enumerate(entries):
            # Always process the rarest one. Skip any later trigram whose
            # posting list is too large — its decode + intersect cost would
            # outweigh the additional filtering.
            if i > 0 and len(posBytes) > STOP_TRIGRAM_BYTES:
                break
            shifted = {p - k for p in decodePositions(posBytes)}
            if candidates is None:
                candidates = shifted
            else:
                candidates &= shifted
                if not candidates:
                    return []

        return sorted(p for p in candidates if p >= 0)
