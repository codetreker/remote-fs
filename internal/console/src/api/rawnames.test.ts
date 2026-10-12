import { describe, expect, it } from "vitest";
import {
  checkRawLeaf,
  decodeRawLeaf,
  displayRawName,
  rawLeafFromInput,
  rawNameFromBase64,
  rawNameFromText,
  rawNameToBase64,
} from "./rawnames";

const utf8 = (text: string): Uint8Array => new TextEncoder().encode(text);

describe("raw leaf bytes and canonical base64", () => {
  it.each([
    new Uint8Array([0xff]),
    utf8("名字"),
    utf8("a"),
    utf8("ab"),
    utf8("abc"),
    utf8("a\\b"),
  ])("round-trips exact bytes %j", (bytes) => {
    const base64 = rawNameToBase64(bytes);
    expect(rawNameFromBase64(base64)).toEqual(bytes);
    expect(decodeRawLeaf(base64)).toEqual({
      rawLeaf: base64,
      rawBytes: bytes,
      displayName: displayRawName(bytes),
    });
  });

  it.each([
    "YQ",
    "YQ=",
    "YQ===",
    "YR==",
    "YWI",
    "YWJ=",
    "YQ==\n",
    " YQ==",
    "_w==",
    "-w==",
    "%%%=",
    "",
  ])("rejects noncanonical base64 %j", (base64) => {
    expect(() => rawNameFromBase64(base64)).toThrow();
  });

  it.each(["", ".", "..", "a/b", "a\0b"])(
    "rejects invalid neutral leaves %j",
    (text) => {
      expect(() => rawNameFromText(text)).toThrow();
      expect(() => rawNameFromBase64(btoa(text))).toThrow();
    },
  );

  it("checks the neutral 4096-byte bound", () => {
    const maximum = utf8("a".repeat(4096));
    expect(rawNameFromBase64(rawNameToBase64(maximum))).toEqual(maximum);
    expect(() => checkRawLeaf(utf8("a".repeat(4097)))).toThrow();
    expect(() => rawNameFromBase64(btoa("a".repeat(4097)))).toThrow();
  });

  it.each([
    "CON",
    "NUL",
    "a:b",
    "a\\b",
    "trailing.",
    "trailing ",
    'name?*<>"|',
  ])("accepts neutral leaf independent of Windows rules %j", (text) => {
    expect(rawNameFromBase64(rawLeafFromInput(text))).toEqual(utf8(text));
  });

  it("rejects unpaired surrogates instead of inventing replacement bytes", () => {
    expect(() => rawNameFromText("a\ud800")).toThrow();
    expect(() => rawNameFromText("\udc00a")).toThrow();
    expect(rawNameFromText("😀")).toEqual(
      new Uint8Array([0xf0, 0x9f, 0x98, 0x80]),
    );
  });
});

describe("injective raw name display", () => {
  it("distinguishes invalid bytes from ASCII escape spellings", () => {
    expect(displayRawName(new Uint8Array([0xff]))).toBe("\\xFF");
    expect(displayRawName(utf8("\\xFF"))).toBe("\\\\xFF");
    expect(displayRawName(utf8("\\u{202E}"))).toBe("\\\\u{202E}");
    expect(displayRawName(utf8("\u202e"))).toBe("\\u{202E}");
    expect(displayRawName(utf8("a/b\\c"))).toBe("a\\u{2F}b\\\\c");
  });

  it("escapes controls, Unicode format characters, bidi and separators", () => {
    const points = [
      0, 9, 10, 13, 31, 127, 128, 159, 0xad, 0x600, 0x61c, 0x200b, 0x200d,
      0x200e, 0x200f, 0x2028, 0x2029, 0x202a, 0x202e, 0x2066, 0x2069, 0xfeff,
      0xe0001,
    ];
    for (const point of points) {
      expect(displayRawName(utf8(String.fromCodePoint(point)))).toBe(
        `\\u{${point.toString(16).toUpperCase()}}`,
      );
    }
  });

  it("preserves valid text and nonnormalized spellings", () => {
    const composed = "é";
    const decomposed = "e\u0301";
    expect(displayRawName(rawNameFromText(composed))).toBe(composed);
    expect(displayRawName(rawNameFromText(decomposed))).toBe(decomposed);
    expect(rawLeafFromInput(composed)).not.toBe(rawLeafFromInput(decomposed));
    expect(displayRawName(utf8("名字😀�"))).toBe("名字😀�");
  });

  it.each([
    [0xc0, 0xaf],
    [0xed, 0xa0, 0x80],
    [0xf4, 0x90, 0x80, 0x80],
    [0xe2, 0x82],
    [0x80],
    [0xe2, 0x41, 0x82],
    [0xff, 0xfe],
    [0xf5, 0x80, 0x80, 0x80],
  ])("preserves each malformed UTF-8 byte %j", (...bytes) => {
    const expected = bytes
      .map((byte) =>
        byte === 0x41
          ? "A"
          : `\\x${byte.toString(16).toUpperCase().padStart(2, "0")}`,
      )
      .join("");
    expect(displayRawName(new Uint8Array(bytes))).toBe(expected);
  });
});
