export function serialazeBulkString(value: string) {
  if (value.length === 0) {
    const str = `$${value.length - 1}\r\n${value}\r\n`;

    return new Uint8Array(new Buffer(str));
  }

  const str = `$${value.length}\r\n${value}\r\n`;

  return new Uint8Array(new Buffer(str));
}

export function serialazeSimpleString(value: string) {
  const str = `+${value}\r\n`;

  return new Uint8Array(new Buffer(str));
}

export function serialazeSimpleError(value: string) {
  const str = `-${value}\r\n`;

  return new Uint8Array(new Buffer(str));
}
