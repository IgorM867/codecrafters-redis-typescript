export function serialazeBulkString(value: string) {
  if (value.length === 0) {
    const str = `$-1\r\n`;

    return new Uint8Array(Buffer.from(str));
  }

  const str = `$${value.length}\r\n${value}\r\n`;

  return new Uint8Array(Buffer.from(str));
}

export function serialazeSimpleString(value: string) {
  const str = `+${value}\r\n`;

  return new Uint8Array(Buffer.from(str));
}

export function serialazeSimpleError(value: string) {
  const str = `-${value}\r\n`;

  return new Uint8Array(Buffer.from(str));
}

export function serialazeArray(...args: Uint8Array[]) {
  let str = `*${args.length}\r\n`;
  const buffer = new Uint8Array(Buffer.from(str));

  const length = buffer.length + args.reduce((acc, el) => acc + el.length, 0);
  const newBuffer = new Uint8Array(length);

  let offset = 0;

  [buffer, ...args].forEach((arr) => {
    newBuffer.set(arr, offset);
    offset += arr.length;
  });

  return newBuffer;
}

export function serialazeBulkStringArray(arr: string[]) {
  const strings = arr.map((val) => serialazeBulkString(val));

  return serialazeArray(...strings);
}
