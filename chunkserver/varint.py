def encodePositions(positions):
    """Delta-varint LEB128 encode a monotonic iterable of non-negative ints."""
    out = bytearray()
    prev = 0
    for pos in positions:
        delta = pos - prev
        prev = pos
        while delta >= 0x80:
            out.append((delta & 0x7F) | 0x80)
            delta >>= 7
        out.append(delta)
    return bytes(out)


def decodePositions(data):
    """Yield absolute positions from delta-varint bytes object."""
    pos = 0
    shift = 0
    current = 0
    for byte in data:
        current |= (byte & 0x7F) << shift
        if byte & 0x80:
            shift += 7
        else:
            pos += current
            yield pos
            current = 0
            shift = 0
