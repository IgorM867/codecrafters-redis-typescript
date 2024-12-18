import type { CommandName } from "./commands";

type Command = {
  name: CommandName;
  args: string[];
  length: number;
};

export class CommandParser {
  private currentByte: number = 0;
  private lastCommandEnd: number = 0;
  constructor(private data: Buffer) {}

  parse(): [Error | null, Command[]] {
    try {
      const commands: Command[] = [];

      while (!this.isEnd()) {
        if (this.readByte() !== 42) throw Error("Expecting array"); // 42 -> '*'
        const array = this.parseArray();
        commands.push({
          name: array[0].toUpperCase() as CommandName,
          args: array.slice(1),
          length: this.currentByte - this.lastCommandEnd,
        });
        this.lastCommandEnd = this.currentByte;
      }

      return [null, commands];
    } catch (error) {
      return [error as Error, []];
    }
  }
  parseString(): string {
    const byte = this.readByte();

    if (byte === 36) {
      const length = Number(this.parseElement());
      const data = this.readBytes(length);

      this.readBytes(2); // \r\n

      return data.toString();
      //bulk string
    } else if (byte === 43) {
      return this.parseElement();
      //simple string
    } else {
      throw Error("Expecting string data type identifier");
    }
  }
  parseElement(): string {
    let value = String.fromCharCode(this.readByte());

    while (this.peekByte() !== 13 && !this.isEnd()) {
      value += String.fromCharCode(this.readByte());
    }
    this.readByte();
    if (this.readByte() !== 10) {
      throw Error("Expecting \\n character after \\r");
    }

    return value;
  }
  isEnd() {
    return this.currentByte >= this.data.length;
  }
  readBytes(offset: number) {
    const data = this.data.subarray(this.currentByte, this.currentByte + offset);
    this.currentByte += offset;
    return data;
  }
  peekBytes(offset: number) {
    return this.data.subarray(this.currentByte, this.currentByte + offset);
  }
  private parseArray(): string[] {
    const length = this.parseElement();
    const arrayElements: string[] = [];

    for (let i = 0; i < Number(length); i++) {
      const element = this.parseString();
      arrayElements.push(element);
    }

    return arrayElements;
  }
  private readByte() {
    const byte = this.data[this.currentByte];
    this.currentByte++;
    return byte;
  }
  private peekByte() {
    return this.data[this.currentByte];
  }
}
