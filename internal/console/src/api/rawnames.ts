const MAX_LEAF_BYTES = 4096;
const utf8Encoder = new TextEncoder();
const escapedCharacters = /[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/u;

export type RawName = {
  rawLeaf: string;
  rawBytes: Uint8Array;
  displayName: string;
};

export const checkRawLeaf = (bytes: Uint8Array): void => {
  if (bytes.length === 0 || bytes.length > MAX_LEAF_BYTES) {
    throw new RangeError(`leaf name must contain 1..${MAX_LEAF_BYTES} bytes`);
  }
  if (
    (bytes.length === 1 && bytes[0] === 46) ||
    (bytes.length === 2 && bytes[0] === 46 && bytes[1] === 46) ||
    bytes.some((byte) => byte === 0 || byte === 47)
  ) {
    throw new TypeError(
      "leaf name cannot be dot, dot-dot, contain NUL or slash",
    );
  }
};

export const rawNameToBase64 = (bytes: Uint8Array): string => {
  checkRawLeaf(bytes);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

export const rawNameFromBase64 = (encoded: string): Uint8Array => {
  if (
    typeof encoded !== "string" ||
    encoded.length > 4 * Math.ceil(MAX_LEAF_BYTES / 3) ||
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
      encoded,
    )
  ) {
    throw new TypeError("rawLeaf must use standard padded base64");
  }
  const binary = atob(encoded);
  const bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0));
  if (rawNameToBase64(bytes) !== encoded)
    throw new TypeError("rawLeaf must use canonical base64");
  return bytes;
};

export const rawNameFromText = (text: string): Uint8Array => {
  for (const character of text) {
    const point = character.codePointAt(0)!;
    if (point >= 0xd800 && point <= 0xdfff) {
      throw new TypeError(
        "leaf input must contain valid Unicode scalar values",
      );
    }
  }
  const bytes = utf8Encoder.encode(text);
  checkRawLeaf(bytes);
  return bytes;
};

export const encodeRawLeaf = rawNameToBase64;
export const rawLeafFromInput = (text: string): string =>
  rawNameToBase64(rawNameFromText(text));

const utf8Scalar = (
  bytes: Uint8Array,
  start: number,
): { point: number; length: number } | undefined => {
  const first = bytes[start];
  if (first < 0x80) return { point: first, length: 1 };
  const length =
    first >= 0xc2 && first <= 0xdf
      ? 2
      : first >= 0xe0 && first <= 0xef
        ? 3
        : first >= 0xf0 && first <= 0xf4
          ? 4
          : 0;
  if (length === 0 || start + length > bytes.length) return undefined;
  let point = first & (0x7f >> length);
  for (let offset = 1; offset < length; offset += 1) {
    const next = bytes[start + offset];
    if (next < 0x80 || next > 0xbf) return undefined;
    point = (point << 6) | (next & 0x3f);
  }
  const minimum = length === 2 ? 0x80 : length === 3 ? 0x800 : 0x10000;
  if (
    point < minimum ||
    point > 0x10ffff ||
    (point >= 0xd800 && point <= 0xdfff)
  )
    return undefined;
  return { point, length };
};

export const displayRawName = (bytes: Uint8Array): string => {
  let result = "";
  for (let index = 0; index < bytes.length;) {
    const scalar = utf8Scalar(bytes, index);
    if (scalar === undefined) {
      result += `\\x${bytes[index].toString(16).toUpperCase().padStart(2, "0")}`;
      index += 1;
      continue;
    }
    const character = String.fromCodePoint(scalar.point);
    result +=
      character === "\\"
        ? "\\\\"
        : character === "/" || escapedCharacters.test(character)
          ? `\\u{${scalar.point.toString(16).toUpperCase()}}`
          : character;
    index += scalar.length;
  }
  return result;
};

export const decodeRawLeaf = (rawLeaf: string): RawName => {
  const rawBytes = rawNameFromBase64(rawLeaf);
  return { rawLeaf, rawBytes, displayName: displayRawName(rawBytes) };
};
