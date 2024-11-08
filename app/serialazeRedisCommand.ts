export function serialazeBulkString(value: string) {
  const str = `$${value.length}\r\n${value}\r\n`;

  return new Uint8Array(new Buffer(str));
}

export function serialazeSimpleString(value: string) {
  const str = `+${value}\r\n`;

  return new Uint8Array(new Buffer(str));
}
