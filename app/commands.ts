import { db, serverState, values } from "./main";
import {
  serialazeArray,
  serialazeBulkString,
  serialazeSimpleError,
  serialazeSimpleString,
} from "./serialazeRedisCommand";

export const commands = {
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
};
