import * as net from "net";
import { db, serverState, values } from "./main";
import { CommandParser } from "./parseRedisCommand";
import {
  serialazeArray,
  serialazeBulkString,
  serialazeBulkStringArray,
  serialazeInteger,
  serialazeSimpleError,
  serialazeSimpleString,
} from "./serialazeRedisCommand";

type CommandName = "PING" | "ECHO" | "SET" | "GET" | "CONFIG" | "KEYS" | "INFO" | "REPLCONF" | "PSYNC" | "WAIT";

const commands = {
  PING: () => serialazeSimpleString("PONG"),
  ECHO: (arg: string | undefined) => {
    if (arg) {
      return serialazeBulkString(arg);
    } else {
      return serialazeSimpleError("ERR wrong number of arguments for 'echo' command");
    }
  },
  SET: (args: string[]) => {
    if (args.length < 2) return serialazeSimpleError("ERR wrong number of arguments for 'set' command");
    const key = args[0];
    const value = args[1];

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
  CONFIG: (args: string[]) => {
    if (args.length === 0) return serialazeSimpleError("ERR wrong number of arguments for 'config' command");

    switch (args[0].toUpperCase()) {
      case "GET":
        return commands.GET_CONFIG(args.at(1));
      default:
        return serialazeSimpleError(`Unknown CONFIG subcommand: ${args[1]}`);
    }
  },
  GET_CONFIG: (arg: string | undefined) => {
    if (!arg) return serialazeSimpleError("ERR wrong number of arguments for 'config|get' command");

    switch (arg.toLowerCase()) {
      case "dir":
        return serialazeArray(serialazeBulkString("dir"), serialazeBulkString(values.dir || ""));
      case "dbfilename":
        return serialazeArray(serialazeBulkString("dbfilename"), serialazeBulkString(values.dbfilename || ""));
      default:
        return serialazeArray();
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
  REPLCONF: (args: string[]) => {
    if (serverState.role === "slave" && args[0].toUpperCase() === "GETACK") {
      if (args[1].toUpperCase() !== "*") return serialazeSimpleString("ERR syntax error");

      return serialazeBulkStringArray(["REPLCONF", "ACK", String(serverState.master_repl_offset)]);
    }

    return serialazeSimpleString("OK");
  },
  PSYNC: (args: string[]) => {
    return serialazeSimpleString(`FULLRESYNC ${serverState.master_replid} ${serverState.master_repl_offset}`);
  },
  WAIT: (args: string[]) => {
    return serialazeInteger(serverState.replicas_connections.length);
  },
};

const writeCommands = ["SET"];

export function execCommand(data: Buffer, connection: net.Socket, fromMaster = false) {
  const parser = new CommandParser(data);

  const [err, results] = parser.parse();
  if (err) {
    connection.write(serialazeSimpleError(err.message));
    return;
  }

  let response: Uint8Array = new Uint8Array();

  results.forEach((command) => {
    const commandName = command.name.toUpperCase();
    switch (commandName as CommandName) {
      case "PING":
        response = commands.PING();
        break;
      case "ECHO":
        response = commands.ECHO(command.args.at(0));
        break;
      case "SET":
        response = commands.SET(command.args);
        break;
      case "GET":
        response = commands.GET(command.args.at(0));
        break;
      case "CONFIG":
        response = commands.CONFIG(command.args);
        break;
      case "KEYS":
        response = commands.KEYS(command.args.at(0));
        break;
      case "INFO":
        response = commands.INFO(command.args.at(0));
        break;
      case "REPLCONF":
        response = commands.REPLCONF(command.args);
        break;
      case "PSYNC":
        response = commands.PSYNC(command.args);
        break;
      case "WAIT":
        response = commands.WAIT(command.args);
        break;
      default:
        response = serialazeSimpleError(`Unknown command: ${command.name}`);
    }
    if (serverState.role === "master") {
      connection.write(response);
      if (commandName === "PSYNC") {
        sendRDBFile(connection);
        serverState.replicas_connections.push(connection);
      }
      if (writeCommands.includes(commandName)) {
        serverState.replicas_connections.forEach((con) => con.write(Uint8Array.from(data)));
      }
    }
    if (serverState.role === "slave") {
      if (commandName === "REPLCONF" || !fromMaster) {
        connection.write(response);
      }
      if (fromMaster) {
        serverState.master_repl_offset += command.length;
      }
    }
  });
}
function sendRDBFile(con: net.Socket) {
  const emptyRDBFile =
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

  const buffer = Uint8Array.from(Buffer.from(emptyRDBFile, "hex"));
  const length = Uint8Array.from(Buffer.from(`$${buffer.length}\r\n`, "utf-8"));
  const message = Uint8Array.from(Buffer.concat([length, buffer]));

  con.write(message);
}
