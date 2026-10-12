import { LosslessNumber, parse, stringify } from "lossless-json";

export const UINT64_MAX = (1n << 64n) - 1n;
export const INT64_MIN = -(1n << 63n);
export const INT64_MAX = (1n << 63n) - 1n;

export const parseV5 = (text: string): unknown => parse(text);

export const readV5Response = async (
  response: Pick<Response, "text">,
): Promise<unknown> => parseV5(await response.text());

const integerToken = (value: unknown, label: string): bigint => {
  if (
    !(value instanceof LosslessNumber) ||
    !/^-?(0|[1-9][0-9]*)$/.test(value.value)
  ) {
    throw new TypeError(`${label} must be a JSON integer token`);
  }
  if (value.value.length - (value.value.startsWith("-") ? 1 : 0) > 20) {
    throw new RangeError(`${label} exceeds a 64-bit integer`);
  }
  return BigInt(value.value);
};

const integerRange = (
  value: bigint,
  minimum: bigint,
  maximum: bigint,
  label: string,
): bigint => {
  if (value < minimum || value > maximum) {
    throw new RangeError(`${label} is outside ${minimum}..${maximum}`);
  }
  return value;
};

export const uint64 = (value: unknown, label = "uint64"): bigint => {
  const integer = integerToken(value, label);
  if ((value as LosslessNumber).value.startsWith("-")) {
    throw new RangeError(`${label} cannot contain an unsigned negative token`);
  }
  return integerRange(integer, 0n, UINT64_MAX, label);
};

export const int64 = (value: unknown, label = "int64"): bigint =>
  integerRange(integerToken(value, label), INT64_MIN, INT64_MAX, label);

export const validatedNumber = (
  value: unknown,
  minimum: number,
  maximum: number,
  label = "number",
): number => {
  if (
    !Number.isSafeInteger(minimum) ||
    !Number.isSafeInteger(maximum) ||
    minimum > maximum
  ) {
    throw new RangeError(`${label} needs a safe integer range`);
  }
  const checked = integerRange(
    integerToken(value, label),
    BigInt(minimum),
    BigInt(maximum),
    label,
  );
  return Number(checked);
};

export const v5Uint64 = (value: bigint): LosslessNumber => {
  if (typeof value !== "bigint")
    throw new TypeError("uint64 request value must be bigint");
  return new LosslessNumber(
    integerRange(value, 0n, UINT64_MAX, "uint64").toString(),
  );
};

export const v5Int64 = (value: bigint): LosslessNumber => {
  if (typeof value !== "bigint")
    throw new TypeError("int64 request value must be bigint");
  return new LosslessNumber(
    integerRange(value, INT64_MIN, INT64_MAX, "int64").toString(),
  );
};

export const v5SmallNumber = (
  value: number,
  minimum: number,
  maximum: number,
): LosslessNumber => {
  if (!Number.isSafeInteger(value))
    throw new TypeError("small request number must be a safe integer");
  const token = new LosslessNumber(value.toString());
  validatedNumber(token, minimum, maximum);
  return token;
};

const checkRequest = (value: unknown, ancestors: Set<object>): void => {
  if (value instanceof LosslessNumber) {
    integerRange(
      integerToken(value, "request number"),
      INT64_MIN,
      UINT64_MAX,
      "request number",
    );
    return;
  }
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return;
  if (typeof value !== "object") {
    throw new TypeError(
      "v5 request numbers must use an explicitly validated LosslessNumber",
    );
  }
  if (ancestors.has(value)) throw new TypeError("v5 request contains a cycle");
  if (
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) !== Object.prototype &&
    Object.getPrototypeOf(value) !== null
  ) {
    throw new TypeError("v5 request must contain only JSON objects and arrays");
  }
  ancestors.add(value);
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1)
      checkRequest(value[index], ancestors);
  } else {
    for (const child of Object.values(value)) checkRequest(child, ancestors);
  }
  ancestors.delete(value);
};

export const stringifyV5 = (value: unknown): string => {
  checkRequest(value, new Set());
  const encoded = stringify(value);
  if (typeof encoded !== "string")
    throw new TypeError("v5 request has no JSON value");
  return encoded;
};
