import * as net from "net";
import { db, serverState, values } from "./main";
import { CommandParser } from "./parseRedisCommand";
import {
  serialazeArray,
  serialazeBulkString,
  serialazeSimpleError,
  serialazeSimpleString,
} from "./serialazeRedisCommand";

type CommandName = "PING" | "ECHO" | "SET" | "GET" | "CONFIG" | "KEYS" | "INFO" | "REPLCONF" | "PSYNC";

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
    return serialazeSimpleString("OK");
  },
  PSYNC: (args: string[]) => {
    return serialazeSimpleString(`FULLRESYNC ${serverState.master_replid} ${serverState.master_repl_offset}`);
  },
};

export function execCommand(data: Buffer, connection: net.Socket) {
  const parser = new CommandParser(data);

  const [err, results] = parser.parse();
  if (err) {
    connection.write(serialazeSimpleError(err.message));
    return;
  }

  results.forEach((command) => {
    switch (command.name.toUpperCase() as CommandName) {
      case "PING":
        connection.write(commands.PING());
        break;
      case "ECHO":
        connection.write(commands.ECHO(command.args.at(0)));
        break;
      case "SET":
        {
          if (serverState.role === "master") {
            connection.write(commands.SET(command.args));
            serverState.replicas_connections.forEach((con) => con.write(Uint8Array.from(data)));
          } else if (serverState.role === "slave") {
            commands.SET(command.args);
          }
        }
        break;
      case "GET":
        connection.write(commands.GET(command.args.at(0)));
        break;
      case "CONFIG":
        connection.write(commands.CONFIG(command.args));
        break;
      case "KEYS":
        connection.write(commands.KEYS(command.args.at(0)));
        break;
      case "INFO":
        connection.write(commands.INFO(command.args.at(0)));
        break;
      case "REPLCONF":
        connection.write(commands.REPLCONF(command.args));
        break;
      case "PSYNC":
        {
          connection.write(commands.PSYNC(command.args));
          sendRDBFile(connection);
          serverState.replicas_connections.push(connection);
        }
        break;
      default:
        connection.write(serialazeSimpleError(`Unknown command: ${command.name}`));
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
