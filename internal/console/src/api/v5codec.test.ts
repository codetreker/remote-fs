import { describe, expect, it } from "vitest";
import { LosslessNumber, parse } from "lossless-json";
import {
  INT64_MAX,
  INT64_MIN,
  UINT64_MAX,
  int64,
  parseV5,
  readV5Response,
  stringifyV5,
  uint64,
  validatedNumber,
  v5Int64,
  v5SmallNumber,
  v5Uint64,
} from "./v5codec";

describe("exact v5 integer tokens", () => {
  it.each(["9007199254740993", "9223372036854775807", "18446744073709551615"])(
    "preserves %s",
    (text) => {
      const value = parseV5(text);
      expect(value).toBeInstanceOf(LosslessNumber);
      expect((value as LosslessNumber).value).toBe(text);
      expect(uint64(value)).toBe(BigInt(text));
      expect(stringifyV5(v5Uint64(uint64(value)))).toBe(text);
    },
  );

  it("round-trips signed and unsigned boundaries", () => {
    for (const value of [INT64_MIN, INT64_MAX, -9007199254740993n, 0n]) {
      expect(int64(parseV5(stringifyV5(v5Int64(value))))).toBe(value);
    }
    expect(uint64(parseV5(stringifyV5(v5Uint64(UINT64_MAX))))).toBe(UINT64_MAX);
  });

  it.each(["18446744073709551616", "-1", "-0"])(
    "rejects out-of-range unsigned %s",
    (text) => {
      expect(() => uint64(parseV5(text))).toThrow();
    },
  );

  it.each(["9223372036854775808", "-9223372036854775809"])(
    "rejects out-of-range signed %s",
    (text) => {
      expect(() => int64(parseV5(text))).toThrow();
    },
  );

  it("rejects oversized integer tokens before BigInt allocation", () => {
    expect(() => uint64(parseV5("1".repeat(10000)))).toThrow(
      /exceeds a 64-bit integer/,
    );
    expect(() => int64(parseV5(`-${"1".repeat(10000)}`))).toThrow(
      /exceeds a 64-bit integer/,
    );
  });

  it.each(["1.0", "1e2", "1.5", '"123"', "true", "null"])(
    "rejects noninteger token %s",
    (text) => {
      expect(() => int64(parseV5(text))).toThrow();
    },
  );

  it("keeps lexical spelling before field validation", () => {
    expect((parseV5("1.000e+2") as LosslessNumber).value).toBe("1.000e+2");
    expect((parseV5("-0") as LosslessNumber).value).toBe("-0");
  });

  it("converts only explicitly bounded small numbers", () => {
    expect(validatedNumber(parseV5("2"), 0, 2, "kind")).toBe(2);
    expect(stringifyV5(v5SmallNumber(2, 0, 2))).toBe("2");
    expect(() => validatedNumber(parseV5("3"), 0, 2)).toThrow();
    expect(() =>
      validatedNumber(parseV5("1"), 0, Number.MAX_SAFE_INTEGER + 1),
    ).toThrow();
    expect(() => v5SmallNumber(1.5, 0, 2)).toThrow();
  });

  it("serializes numeric identity fields with exact Go names", () => {
    const body = stringifyV5({
      parent: { NodeID: v5Uint64(UINT64_MAX), Scope: { Token: "scope" } },
      rawLeaf: "/w==",
    });
    expect(body).toBe(
      '{"parent":{"NodeID":18446744073709551615,"Scope":{"Token":"scope"}},"rawLeaf":"/w=="}',
    );
    const decoded = parseV5(body) as {
      parent: { NodeID: unknown; Scope: { Token: string } };
    };
    expect(uint64(decoded.parent.NodeID)).toBe(UINT64_MAX);
    expect(decoded.parent.Scope.Token).toBe("scope");
  });

  it("reads response text through the exact parser", async () => {
    const value = await readV5Response({
      text: async () => "9007199254740993",
    });
    expect(uint64(value)).toBe(9007199254740993n);
  });
});

describe("strict distributed lossless parser", () => {
  it("applies the strict parser to the shipped UMD entry too", async () => {
    const umdPath = new URL(
      "../../node_modules/lossless-json/lib/umd/lossless-json.js",
      import.meta.url,
    ).href;
    const imported = await import(/* @vite-ignore */ umdPath);
    const umd = (imported.default ?? imported) as {
      parse: (text: string, reviver?: undefined, options?: unknown) => unknown;
    };
    for (const value of ["null", "true", "false", "0", '"same"', "[]", "{}"]) {
      expect(() =>
        umd.parse(`{"key":${value},"key":${value}}`, undefined, {
          onDuplicateKey: () => null,
        }),
      ).toThrow(/Duplicate key/);
    }
    const result = umd.parse('{"__proto__":{"polluted":true}}') as Record<
      string,
      unknown
    >;
    expect(Object.getPrototypeOf(result)).toBe(Object.prototype);
    expect(Object.hasOwn(result, "__proto__")).toBe(true);
    expect(result.__proto__).toEqual({ polluted: true });
  });

  it.each([
    "null",
    "true",
    "false",
    "0",
    '"same"',
    "[]",
    "{}",
    '[1, {"child":true}]',
  ])("rejects identical duplicate values %s", (value) => {
    expect(() => parseV5(`{"key":${value},"key":${value}}`)).toThrow(
      /Duplicate key/,
    );
    expect(() =>
      parse(`{"key":${value},"key":${value}}`, undefined, {
        onDuplicateKey: () => null,
      }),
    ).toThrow(/Duplicate key/);
  });

  it("rejects different, nested and escape-equivalent duplicate keys", () => {
    for (const text of [
      '{"a":1,"a":2}',
      '{"nested":{"a":null,"a":true}}',
      '{"a":1,"\\u0061":1}',
      '{"__proto__":{},"__proto__":{}}',
    ]) {
      expect(() => parseV5(text)).toThrow(/Duplicate key/);
    }
  });

  it("preserves __proto__ as an own key without changing the prototype", () => {
    const decoded = parseV5(
      '{"__proto__":{"polluted":true},"constructor":"own","toString":null}',
    ) as Record<string, unknown>;
    expect(Object.getPrototypeOf(decoded)).toBe(Object.prototype);
    expect(Object.hasOwn(decoded, "__proto__")).toBe(true);
    expect(decoded.__proto__).toEqual({ polluted: true });
    expect(decoded.constructor).toBe("own");
    expect(Object.hasOwn({}, "polluted")).toBe(false);
    expect(stringifyV5(decoded)).toBe(
      '{"__proto__":{"polluted":true},"constructor":"own","toString":null}',
    );
  });

  it.each([
    "",
    "{}{}",
    "1 trailing",
    "[1,]",
    '{"a":1,}',
    '{"a":}',
    "{a:1}",
    "01",
    "+1",
    "1.",
    "1e",
    "NaN",
    "Infinity",
    '"unterminated',
    '"\\xFF"',
    '{"a":"\n"}',
  ])("rejects malformed or trailing JSON %j", (text) => {
    expect(() => parseV5(text)).toThrow();
  });

  it("does not serialize accidental coercions or unsupported numeric values", () => {
    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;
    for (const value of [
      1,
      9007199254740992,
      1n,
      NaN,
      undefined,
      new Date(),
      new Number(1),
      { id: 1 },
      { id: 1n },
      new LosslessNumber("1e2"),
      new LosslessNumber("18446744073709551616"),
      [undefined],
      cycle,
      { toJSON: () => ({ id: 1 }) },
    ]) {
      expect(() => stringifyV5(value)).toThrow();
    }
    expect(() => v5Uint64(-1n)).toThrow();
    expect(() => v5Int64(INT64_MAX + 1n)).toThrow();
  });
});
