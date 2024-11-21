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
import { logFormattedData } from "./util";

export type CommandName =
  | "PING"
  | "ECHO"
  | "SET"
  | "GET"
  | "CONFIG"
  | "KEYS"
  | "INFO"
  | "REPLCONF"
  | "PSYNC"
  | "WAIT"
  | "TYPE"
  | "XADD"
  | "XRANGE";

const waitState = {
  isWaiting: false,
  acknowledgeGoal: 0,
  acknowledgeCount: 0,
  resolvePromise: () => {},
  reset() {
    this.isWaiting = false;
    this.acknowledgeGoal = 0;
    this.acknowledgeCount = 0;
    this.resolvePromise = () => {};
  },
};
const noResponseQueue: net.Socket[] = [];

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

      db.set(key, { value, expire: BigInt(Date.now()) + BigInt(args[3]), type: "string" });

      return serialazeSimpleString("OK");
    }

    db.set(key, { value, expire: null, type: "string" });
    return serialazeSimpleString("OK");
  },
  GET: (key: string | undefined) => {
    if (!key || !db.has(key)) {
      return serialazeBulkString("");
    }
    const entry = db.get(key)!;
    if (entry.type === "string") {
      if (entry.expire && entry.expire - BigInt(Date.now()) <= 0) {
        return serialazeBulkString("");
      }
      return serialazeBulkString(entry.value);
    }
    return serialazeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value");
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
    if (serverState.role === "master" && args[0].toUpperCase() === "ACK" && waitState.isWaiting) {
      waitState.acknowledgeCount++;

      if (waitState.acknowledgeCount >= waitState.acknowledgeGoal) {
        waitState.resolvePromise();
      }

      return;
    }

    return serialazeSimpleString("OK");
  },
  PSYNC: (args: string[]) => {
    return serialazeSimpleString(`FULLRESYNC ${serverState.master_replid} ${serverState.master_repl_offset}`);
  },
  WAIT: (args: string[]) => {
    const acknowledgeGoal = Number(args[0]);
    const timeout = Number(args[1]);

    if (acknowledgeGoal <= 0) {
      return serialazeInteger(0);
    }

    if (serverState.master_repl_offset === 0) {
      return serialazeInteger(serverState.replicas_connections.length);
    }

    return new Promise<Uint8Array>((res) => {
      waitState.isWaiting = true;
      waitState.acknowledgeGoal = acknowledgeGoal;
      waitState.acknowledgeCount = 0;
      waitState.resolvePromise = () => res(serialazeInteger(waitState.acknowledgeCount));

      propagateToReplicas(serialazeBulkStringArray(["REPLCONF", "GETACK", "*"]), true);

      const timeoutId = setTimeout(() => {
        res(serialazeInteger(waitState.acknowledgeCount));
        waitState.reset();
      }, timeout);

      const originalResolve = waitState.resolvePromise;
      waitState.resolvePromise = () => {
        clearTimeout(timeoutId);
        originalResolve();
        waitState.reset();
      };
    });
  },
  TYPE: (key: string | undefined) => {
    if (!key) return serialazeSimpleError("ERR wrong number of arguments for 'type' command");
    const value = db.get(key);

    if (!value) return serialazeSimpleString("none");

    return serialazeSimpleString(value.type);
  },
  XADD: (args: string[]) => {
    const streamKey = args.at(0);
    const entryId = args.at(1);

    if (!streamKey || !entryId) return serialazeSimpleError("Err wrong number of arguments for 'XADD' command");

    const values: { key: string; value: string }[] = [];
    for (let i = 2; i < args.length; i += 2) {
      const key = args[i];
      const value = args.at(i + 1);
      if (!value) return serialazeSimpleError("Err wrong number of arguments for 'XADD' command");

      values.push({ key, value });
    }

    const existingEntry = db.get(streamKey);

    if (existingEntry) {
      if (existingEntry.type === "string")
        return serialazeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value");

      const [err, newEntryId] = validateEntryId(entryId, existingEntry.lastId);
      if (err) return serialazeSimpleError(err);

      existingEntry.value.set(newEntryId, values);
      existingEntry.lastId = newEntryId;

      return serialazeBulkString(newEntryId);
    }

    const [err, newEntryId] = validateEntryId(entryId);
    if (err) return serialazeSimpleError(err);

    db.set(streamKey, { type: "stream", value: new Map().set(newEntryId, values), lastId: newEntryId });
    return serialazeBulkString(newEntryId);
  },
  XRANGE: (args: string[]) => {
    const streamKey = args.at(0);
    if (!streamKey) return serialazeSimpleError("ERR wrong number of arguments for 'xrange' command");

    const stream = db.get(streamKey);
    if (!stream) return serialazeArray();
    if (stream.type !== "stream") {
      return serialazeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    const startId = args.at(1);
    const endId = args.at(2);
    if (!startId || !endId) return serialazeSimpleError("ERR wrong number of arguments for 'xrange' command");
    const startTimeSeq = startId.split("-").map(Number);
    const endTimeSeq = endId.split("-").map(Number);

    const startTime = startTimeSeq[0] || 0;
    const startSeq = startTimeSeq.at(1) || 0;

    const endTime = endTimeSeq[0] || Infinity;
    const endSeq = endTimeSeq.at(1) || Infinity;

    const entires = [];

    for (const [entryId, values] of stream.value.entries()) {
      const [time, seq] = entryId.split("-").map(Number);

      if (time < startTime || time > endTime) continue;
      if (time === startTime && (seq < startSeq || seq > endSeq)) continue;

      const keyValues = [];
      for (const pair of values) {
        keyValues.push(pair.key);
        keyValues.push(pair.value);
      }
      entires.push(serialazeArray(serialazeBulkString(entryId), serialazeBulkStringArray(keyValues)));
    }
    return serialazeArray(...entires);
  },
};

const writeCommands = ["SET"];

export async function execCommand(data: Buffer, connection: net.Socket, fromMaster = false) {
  const parser = new CommandParser(data);

  const [err, results] = parser.parse();
  if (err) {
    connection.write(serialazeSimpleError(err.message));
    return;
  }

  let response: Uint8Array | Promise<Uint8Array> | undefined = new Uint8Array();

  for (const command of results) {
    const commandName = command.name.toUpperCase() as CommandName;
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
      case "TYPE":
        response = commands.TYPE(command.args.at(0));
        break;
      case "XADD":
        response = commands.XADD(command.args);
        break;
      case "XRANGE":
        response = commands.XRANGE(command.args);
        break;
      default:
        response = serialazeSimpleError(`Unknown command: ${command.name}`);
    }
    let resolvedResponse = await Promise.resolve(response);
    if (resolvedResponse === undefined) return;

    if (serverState.role === "master") {
      if (noResponseQueue.includes(connection)) {
        logFormattedData("stop response", resolvedResponse);
        noResponseQueue.splice(noResponseQueue.indexOf(connection), 1);
        return;
      }

      logFormattedData("sending", resolvedResponse);
      connection.write(resolvedResponse);
      if (commandName === "PSYNC") {
        sendRDBFile(connection);
        serverState.replicas_connections.push(connection);
      }
      if (writeCommands.includes(commandName)) {
        propagateToReplicas(Uint8Array.from(data));
      }
    }
    if (serverState.role === "slave") {
      if (commandName === "REPLCONF" || !fromMaster) {
        logFormattedData("sending", resolvedResponse);
        connection.write(resolvedResponse);
      }
      if (fromMaster) {
        serverState.master_repl_offset += command.length;
      }
    }
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
function propagateToReplicas(data: Uint8Array, noReply: boolean = false) {
  logFormattedData("propagates", data);

  if (noReply) {
    noResponseQueue.push(...serverState.replicas_connections);
  }
  serverState.replicas_connections.forEach((con) => con.write(data));
  serverState.master_repl_offset += data.length;
}
function validateEntryId(newEntryId: string, lastEntryId: string = "0-0"): [string | null, string] {
  const [lastTime, lastSeqNumber] = lastEntryId.split("-").map(Number);

  if (newEntryId === "*") {
    const timeStamp = Date.now();
    let newSeqNumber = 0;
    if (lastTime === timeStamp) newSeqNumber = lastSeqNumber + 1;

    return [null, `${timeStamp}-${newSeqNumber}`];
  }

  let [time, seqNumber] = newEntryId.split("-");

  if (isNaN(Number(time))) {
    return ["ERR Invalid stream ID specified as stream command argument", ""];
  }
  if (Number(time) <= 0 && Number(seqNumber) <= 0) {
    return ["ERR The ID specified in XADD must be greater than 0-0", ""];
  }
  if (Number(time) < lastTime) {
    return ["ERR The ID specified in XADD is equal or smaller than the target stream top item", ""];
  }

  if (seqNumber === "*") {
    let newSeqNumber = 0;
    if (time === "0") newSeqNumber = 1;
    if (Number(time) === lastTime) newSeqNumber = lastSeqNumber + 1;

    return [null, `${time}-${newSeqNumber}`];
  }

  if (Number(time) === lastTime && Number(seqNumber) <= lastSeqNumber) {
    return ["ERR The ID specified in XADD is equal or smaller than the target stream top item", ""];
  }

  return [null, newEntryId];
}
