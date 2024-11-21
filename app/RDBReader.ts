import type { BunFile } from "bun";
import type { DataType, DBValue } from "./main";

const opCodes: Record<number, string> = {
  0xff: "EOF",
  0xfe: "SELECTDB",
  0xfd: "EXPIRETIME",
  0xfc: "EXPIRETIMES",
  0xfb: "RESIZEDB",
  0xfa: "AUX",
};

/*
    Value Types
    0 = String Encoding
    1 = List Encoding (Not implemented)
    2 = Set Encoding (Not implemented)
    3 = Sorted Set Encoding (Not implemented)
    4 = Hash Encoding (Not implemented)
    9 = Zipmap Encoding (Not implemented)
    10 = Ziplist Encoding (Not implemented)
    11 = Intset Encoding (Not implemented)
    12 = Sorted Set in Ziplist Encoding (Not implemented)
    13 = Hashmap in Ziplist Encoding (Introduced in RDB version 4) (Not implemented)
    14 = List in Quicklist encoding (Introduced in RDB version 7) (Not implemented)
*/
export class RDBReader {
  private buffer: ArrayBuffer;
  private view: DataView;
  private offset: number = 0;

  constructor(buffer: ArrayBuffer) {
    this.buffer = buffer;
    this.view = new DataView(buffer);
  }
  static async loadFile(file: BunFile) {
    const buffer = await file.arrayBuffer();

    return new RDBReader(buffer);
  }
  readFile() {
    const header = this.readString(9);

    const metadata = this.decodeMetadata();
    const db = this.decodeDB();

    const opCode = opCodes[this.readByte()];

    if (opCode !== "EOF") throw Error("File reading error: Expecting EOF");

    return {
      header,
      metadata,
      db,
    };
  }
  private decodeDB() {
    const opCode = this.peekByte();
    if (opCode !== 0xfe) throw Error("File reading error: SELECTDB Op Code");
    this.offset++;
    const dbIndex = this.decodeSize();

    this.readByte(); //tableSizeOpCode
    const tableSize = this.decodeSize();
    const expiryTableSize = this.decodeSize();

    const values: Record<string, DBValue> = {};

    for (let i = 0; i < tableSize; i++) {
      const currentByte = this.peekByte();
      if (currentByte === 0xfc) {
        this.offset++;
        const timeStamp = this.view.getBigUint64(this.offset, true);
        this.offset += 8;
        const { key, value, type } = this.decodeKeyValuePair();

        values[key] = { value, expire: timeStamp, type };
      } else if (currentByte === 0xfd) {
        this.offset++;
        const timeStamp = this.view.getUint32(this.offset, true);
        const { key, value, type } = this.decodeKeyValuePair();
        values[key] = { value, expire: BigInt(timeStamp) * 1000n, type };
      } else {
        const { key, value, type } = this.decodeKeyValuePair();

        values[key] = { value, expire: null, type };
      }
    }

    return {
      index: dbIndex,
      tableSize,
      expiryTableSize,
      values,
    };
  }
  private decodeKeyValuePair() {
    const dataType = this.readByte();
    const key = this.decodeString();
    let value;
    let type: DataType;

    if (dataType === 0) {
      value = this.decodeString();
      type = "string";
    } else {
      throw Error(`This data type is not supported: ${dataType}`);
    }

    return { key, value, type };
  }
  private decodeMetadata() {
    const metadata: Record<string, string> = {};

    while (this.peekByte() === 0xfa) {
      this.offset++;
      const name = this.decodeString();
      const value = this.decodeString();

      metadata[name] = value;
    }
    return metadata;
  }

  private decodeString() {
    const firstTwoBits = this.peekByte() >> 6;

    if (firstTwoBits === 0b11) {
      const stringFormat = this.decodeSize();

      if (stringFormat === 0x0) {
        return this.readByte().toString();
      } else if (stringFormat === 0x1) {
        const string = this.view.getUint16(this.offset, true).toString();
        this.offset += 2;
        return string;
      } else if (stringFormat === 0x2) {
        const string = this.view.getUint32(this.offset, true).toString();
        this.offset += 4;
        return string;
      } else if (stringFormat === 0x3) {
        throw new Error("LZF-compressed strings is not implemented");
      }
    }

    const size = this.decodeSize();
    const string = this.readString(size);

    return string;
  }
  private decodeSize(): number {
    const firstByte = this.readByte();
    const firstTwoBits = firstByte >> 6;

    if (firstTwoBits === 0b00) {
      return firstByte & 0b00111111;
    } else if (firstTwoBits === 0b01) {
      const maskedFirstByte = firstByte & 0b00111111;
      const secondByte = this.readByte();

      return (maskedFirstByte << 8) | secondByte;
    } else if (firstTwoBits === 0b10) {
      const number = this.view.getUint32(this.offset);
      this.offset += 4;

      return number;
    } else if (firstTwoBits === 0b11) {
      //type of string encoding
      return firstByte & 0b00111111;
    }
    return 0;
  }
  private readByte() {
    const byte = this.view.getUint8(this.offset);
    this.offset++;
    return byte;
  }
  private peekByte() {
    return this.view.getUint8(this.offset);
  }
  private readString(length: number) {
    const string = new TextDecoder().decode(this.buffer.slice(this.offset, this.offset + length));

    this.offset += length;

    return string;
  }
}
