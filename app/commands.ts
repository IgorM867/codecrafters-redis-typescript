import * as net from "net";
import { db, serverState, values } from "./main";
import type { RESPDataType } from "./parseRedisCommand";
import {
  serialazeArray,
  serialazeBulkString,
  serialazeSimpleError,
  serialazeSimpleString,
} from "./serialazeRedisCommand";

const commands = {
  PING: () => serialazeSimpleString("PONG"),
  ECHO: (arg: string) => serialazeBulkString(arg),
  SET: (...args: Array<string | undefined>) => {
    const key = args[0];
    const value = args[1];
    if (!key) {
      return serialazeSimpleError("ERR wrong number of arguments for 'set' command");
    }
    if (!value) {
      return serialazeSimpleError("ERR wrong number of arguments for 'set' command");
    }
    if (args[2] && args[2].toUpperCase() === "PX") {
      if (!args[3]) return serialazeSimpleString("ERR syntax error");

      db.set(key, { value, expire: BigInt(Date.now()) + BigInt(args[3]) });

      return serialazeSimpleString("OK");
    }

    db.set(key, { value, expire: null });
    return serialazeSimpleString("OK");
  },
  GET: (key: string | undefined) => {
    if (!key || !db.has(key)) {
      return serialazeBulkString("");
    }
    const entry = db.get(key)!;

    if (entry.expire && entry.expire - BigInt(Date.now()) <= 0) {
      return serialazeBulkString("");
    }
    return serialazeBulkString(entry.value);
  },
  GET_CONFIG: (arg: string | undefined) => {
    if (!arg) return serialazeSimpleError("ERR wrong number of arguments for 'config|get' command");

    switch (arg.toLowerCase()) {
      case "dir":
        return serialazeArray(serialazeBulkString("dir"), serialazeBulkString(values.dir || ""));
      case "dbfilename":
        return serialazeArray(serialazeBulkString("dbfilename"), serialazeBulkString(values.dbfilename || ""));
      default:
        return serialazeSimpleError(`Invalid parameter: '${arg}'`);
    }
  },
  KEYS: (arg: string | undefined) => {
    if (!arg) return serialazeSimpleError("ERR wrong number of arguments for 'keys' command");

    if (arg === "*") {
      const keys = Array.from(db.keys()).map((key) => serialazeBulkString(key));

      return serialazeArray(...keys);
    }

    return serialazeBulkString("");
  },
  INFO: (arg: string | undefined) => {
    const sections = [
      {
        name: "Replication",
        values: [
          `role:${serverState.role}`,
          `master_replid:${serverState.master_replid}`,
          `master_repl_offset:${serverState.master_repl_offset}`,
        ],
      },
    ];

    if (arg === "replication") {
      const values = sections.find(({ name }) => name === "Replication")!.values.join("\n");

      return serialazeBulkString("# Replication\n" + values);
    }
    let result = "";
    sections.forEach((section) => {
      result += `# ${section.name}\n`;
      result += section.values.join("\n");
    });

    return serialazeBulkString(result);
  },
  REPLCONF: (args: Array<string | undefined>) => {
    return serialazeSimpleString("OK");
  },
  PSYNC: (args: Array<string | undefined>) => {
    return serialazeSimpleString(`FULLRESYNC ${serverState.master_replid} ${serverState.master_repl_offset}`);
  },
};

export function execCommand(input: RESPDataType[], connection: net.Socket) {
  switch ((input[0] as string).toUpperCase()) {
    case "PING":
      connection.write(commands.PING());
      break;
    case "ECHO":
      connection.write(commands.ECHO(input[1] as string));
      break;
    case "SET":
      connection.write(commands.SET(...(input.slice(1) as Array<string | undefined>)));
      break;
    case "GET":
      connection.write(commands.GET(input[1] as string | undefined));
      break;
    case "CONFIG":
      switch ((input[1] as string).toUpperCase()) {
        case "GET":
          connection.write(commands.GET_CONFIG(input[2] as string | undefined));
          break;
        default:
          connection.write(serialazeSimpleError(`Unknown CONFIG command: ${input[1]}`));
      }
      break;
    case "KEYS":
      connection.write(commands.KEYS(input[1] as string | undefined));
      break;
    case "INFO":
      connection.write(commands.INFO(input[1] as string | undefined));
      break;
    case "REPLCONF":
      connection.write(commands.REPLCONF(input.slice(1) as Array<string | undefined>));
      break;
    case "PSYNC":
      {
        connection.write(commands.PSYNC(input.slice(1) as Array<string | undefined>));
        sendRDBFile(connection);
      }
      break;
    default:
      connection.write(serialazeSimpleError(`Unknown command: ${input[0]}`));
  }
}
function sendRDBFile(con: net.Socket) {
  const emptyRDBFile =
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

  const buffer = Uint8Array.from(Buffer.from(emptyRDBFile, "hex"));
  const length = Uint8Array.from(Buffer.from(`$${buffer.length}\r\n`, "utf-8"));
  const message = Uint8Array.from(Buffer.concat([length, buffer]));

  con.write(message);
}
