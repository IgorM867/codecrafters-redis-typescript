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
  | "XRANGE"
  | "XREAD"
  | "INCR"
  | "MULTI"
  | "EXEC";

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

const blockState = {
  isBlocking: false,
  blockingKeys: [] as string[],
  resolvePromise: (addedKey: string, startId: string) => {},
  reset() {
    this.isBlocking = false;
    this.resolvePromise = () => {};
  },
};

const transactionState = {
  isStarted: false,
  queue: [] as Array<() => Uint8Array | Promise<Uint8Array> | undefined>,
  connection: null as net.Socket | null,
};

const commands = {
  PING: () => serialazeSimpleString("PONG"),
  ECHO: (args: string[]) => {
    if (args.at(0)) {
      return serialazeBulkString(args[0]);
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
  GET: (args: string[]) => {
    const key = args.at(0);
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
        return commands.GET_CONFIG(args.slice(1));
      default:
        return serialazeSimpleError(`Unknown CONFIG subcommand: ${args[1]}`);
    }
  },
  GET_CONFIG: (args: string[]) => {
    const arg = args.at(0);
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
  KEYS: (args: string[]) => {
    const arg = args.at(0);
    if (!arg) return serialazeSimpleError("ERR wrong number of arguments for 'keys' command");

    if (arg === "*") {
      const keys = Array.from(db.keys()).map((key) => serialazeBulkString(key));

      return serialazeArray(...keys);
    }

    return serialazeBulkString("");
  },
  INFO: (args: string[]) => {
    const arg = args.at(0);
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
  TYPE: (args: string[]) => {
    const key = args.at(0);
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

      if (blockState.isBlocking && blockState.blockingKeys.includes(streamKey)) {
        blockState.resolvePromise(streamKey, newEntryId);
      }

      return serialazeBulkString(newEntryId);
    }

    const [err, newEntryId] = validateEntryId(entryId);
    if (err) return serialazeSimpleError(err);

    db.set(streamKey, { type: "stream", value: new Map().set(newEntryId, values), lastId: newEntryId });
    if (blockState.isBlocking && blockState.blockingKeys.includes(streamKey)) {
      blockState.resolvePromise(streamKey, newEntryId);
    }
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
  XREAD: (args: string[]) => {
    let timeout: string | undefined = "";
    let isBlocked = false;
    if (args.at(0) === "block") {
      timeout = args.at(1);
      if (!timeout || isNaN(Number(timeout))) {
        return serialazeSimpleError("ERR wrong number of arguments for 'xread' command");
      }
      isBlocked = true;
      args = args.slice(2);
    }

    if (args.at(0) !== "streams") return serialazeSimpleError("ERR syntax error");
    const streamKeys: string[] = [];
    const startIds = [];

    for (let i = 1; i < args.length; i++) {
      if (args[i].includes("-") || args[i] === "$") {
        startIds.push(args[i]);
        continue;
      }

      streamKeys.push(args[i]);
    }

    if (streamKeys.length !== startIds.length)
      return serialazeSimpleError("ERR wrong number of arguments for 'xread' command");

    const streams = [];

    for (let i = 0; i < streamKeys.length; i++) {
      const stream = db.get(streamKeys[i]);
      if (!stream) continue;
      if (stream.type !== "stream") {
        return serialazeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      const startId = startIds[i] === "$" ? stream.lastId : startIds[i];
      const startTimeSeq = startId.split("-").map(Number);

      const startTime = startTimeSeq[0] || 0;
      const startSeq = startTimeSeq.at(1) || 0;

      const entires = [];

      for (const [entryId, values] of stream.value.entries()) {
        const [time, seq] = entryId.split("-").map(Number);

        if (time < startTime) continue;
        if (time === startTime && seq < startSeq) continue;

        const keyValues = [];
        for (const pair of values) {
          keyValues.push(pair.key);
          keyValues.push(pair.value);
        }
        entires.push(serialazeArray(serialazeBulkString(entryId), serialazeBulkStringArray(keyValues)));
      }
      streams.push(serialazeArray(serialazeBulkString(streamKeys[i]), serialazeArray(...entires)));
    }
    if (isBlocked) {
      return new Promise<Uint8Array>((res) => {
        blockState.isBlocking = true;
        blockState.blockingKeys = streamKeys;
        blockState.resolvePromise = (addedKey: string, startId: string) =>
          res(commands.XREAD(["streams", addedKey, startId]));

        let timeoutId: Timer | null = null;

        if (Number(timeout) > 0) {
          timeoutId = setTimeout(() => {
            res(serialazeBulkString(""));
            blockState.reset();
          }, Number(timeout));
        }

        const originalResolve = blockState.resolvePromise;
        blockState.resolvePromise = (addedKey: string, startId: string) => {
          if (timeoutId) {
            clearTimeout(timeoutId);
          }
          originalResolve(addedKey, startId);
          waitState.reset();
        };
      });
    }

    return serialazeArray(...streams);
  },
  INCR: (args: string[]) => {
    const key = args.at(0);
    if (!key) {
      return serialazeSimpleError("ERR wrong number of arguments for 'incr' command");
    }
    const entry = db.get(key);

    if (!entry) {
      commands.SET([key, "1"]);
      return serialazeInteger(1);
    }
    if (entry.type !== "string") {
      return serialazeSimpleError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    if (isNaN(Number(entry.value))) {
      return serialazeSimpleError("ERR value is not an integer or out of range");
    }
    const newValue = Number(entry.value) + 1;
    entry.value = String(newValue);

    return serialazeInteger(newValue);
  },
  MULTI: () => {
    transactionState.isStarted = true;
    return serialazeSimpleString("OK");
  },
  EXEC: async () => {
    if (!transactionState.isStarted) {
      return serialazeSimpleError("ERR EXEC without MULTI");
    }
    const responses = [];

    for (const command of transactionState.queue) {
      const response = command();
      const resolvedResponse = await Promise.resolve(response);
      if (resolvedResponse === undefined) continue;

      responses.push(resolvedResponse);
    }

    transactionState.isStarted = false;
    transactionState.connection = null;
    transactionState.queue = [];
    return serialazeArray(...responses);
  },
};

const writeCommands = ["SET"];

let connectionLast: any = null;

export async function execCommand(data: Buffer, connection: net.Socket, fromMaster = false) {
  const parser = new CommandParser(data);
  console.log(connectionLast === connection);
  connectionLast = connection;

  const [err, results] = parser.parse();
  if (err) {
    connection.write(serialazeSimpleError(err.message));
    return;
  }

  let response: Uint8Array | Promise<Uint8Array> | undefined = new Uint8Array();

  for (const command of results) {
    const commandName = command.name.toUpperCase() as CommandName;

    const fun = commands[commandName];

    if (!fun) {
      response = serialazeSimpleError(`Unknown command: ${command.name}`);
    } else {
      if (transactionState.isStarted && transactionState.connection === connection && commandName !== "EXEC") {
        response = serialazeSimpleString("QUEUED");
        transactionState.queue.push(() => fun(command.args));
      } else {
        response = fun(command.args);
      }
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
      } else if (commandName === "MULTI") {
        transactionState.connection = connection;
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
