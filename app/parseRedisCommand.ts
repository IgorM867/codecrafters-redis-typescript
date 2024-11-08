type BulkString = string;
type SimpleString = string;
export type RESPDataType = Array<RESPDataType> | BulkString | SimpleString;

const RESP_TYPES: Record<number, RESPDataType> = {
  36: "BulkString", // $
  42: "Array", // *
  43: "SimpleString", //+
};

export function parse(data: Buffer, currentByte: number = 0): [RESPDataType, number] {
  const RESPDataType = RESP_TYPES[data[currentByte]];

  switch (RESPDataType) {
    case "Array":
      return parseArray(data, ++currentByte);
    case "BulkString":
      return parseBulkString(data, ++currentByte);
    case "SimpleString":
      return parseSimpleString(data, ++currentByte);
    default:
      throw Error("Unexpected data type");
  }
}
function parseArray(data: Buffer, currentByte: number): [Array<RESPDataType>, number] {
  const [length, lengthEnd] = parseElement(data, currentByte);
  currentByte = lengthEnd;
  const arrayElements: RESPDataType[] = [];

  for (let i = 0; i < Number(length); i++) {
    const [element, elementEnd] = parse(data, currentByte);
    currentByte = elementEnd;
    arrayElements.push(element);
  }

  return [arrayElements, currentByte];
}
function parseBulkString(data: Buffer, currentByte: number): [BulkString, number] {
  const [length, lengthEnd] = parseElement(data, currentByte);
  currentByte = lengthEnd;

  let [value, valueEnd] = parseElement(data, currentByte);
  currentByte = valueEnd;

  if (value.length !== Number(length)) {
    throw Error("Invalid length of the string");
  }

  return [value, currentByte];
}
function parseSimpleString(data: Buffer, currentByte: number): [SimpleString, number] {
  const [str, strEnd] = parseElement(data, currentByte);
  currentByte = strEnd;

  return [str, currentByte];
}
function parseElement(data: Buffer, currentByte: number): [string, number] {
  let value = String.fromCharCode(data[currentByte]);
  currentByte++;

  while (data[currentByte] !== 13 && currentByte <= data.length) {
    value += String.fromCharCode(data[currentByte]);
    currentByte++;
  }
  if (data[++currentByte] !== 10) {
    throw Error("Expecting \\n character after \\r");
  }
  currentByte++;

  return [value, currentByte];
}
