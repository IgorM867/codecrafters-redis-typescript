import * as net from "net";
import { execCommand } from "./commands";
import { RDBReader } from "./RDBReader";
import { parseArgs } from "util";
import { serialazeArray, serialazeBulkString, serialazeBulkStringArray } from "./serialazeRedisCommand";
import { CommandParser } from "./parseRedisCommand";
import { logFormattedData } from "./util";

enum HandshapeStep {
  PING = 1,
  REPLCONF,
  PSYNC,
  FULLRESYNC,
  FILE_TRANSFER,
  DONE,
}

export const { values } = parseArgs({
  args: Bun.argv,
  options: {
    dir: {
      type: "string",
    },
    dbfilename: {
      type: "string",
    },
    port: {
      type: "string",
    },
    replicaof: {
      type: "string",
    },
  },
  strict: true,
  allowPositionals: true,
});

type Stream = Map<string, { key: string; value: string }[]>;

export type DataType = "string" | "stream";
export type DBValue =
  | { type: "string"; value: string; expire: bigint | null }
  | { type: "stream"; value: Stream; lastId: string };

export let db: Map<string, DBValue> = new Map();

export const serverState: {
  port: number;
  role: "master" | "slave";
  master_replid: string;
  master_repl_offset: number;
  replicas_connections: net.Socket[];
} = {
  port: Number(values.port) || 6379,
  role: values.replicaof ? "slave" : "master",
  master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
  master_repl_offset: 0,
  replicas_connections: [],
};
main();

async function main() {
  const result = await readRDBFile(values.dbfilename);

  if (result) {
    db = new Map(Object.entries(result.db.values));
  }

  const server: net.Server = net.createServer((connection: net.Socket) => {
    connection.on("data", (data) => {
      logFormattedData("received", Uint8Array.from(data));
      execCommand(data, connection);
    });
  });

  server.listen(serverState.port, "127.0.0.1");
  console.log(`Redis server is listening on port ${serverState.port}...`);

  if (serverState.role === "slave") {
    const [host, port] = values.replicaof!.split(" ");
    const masterConnection: net.Socket = net.createConnection({ host, port: Number(port) });
    masterConnection.on("ready", () => {
      masterConnection.write(serialazeArray(serialazeBulkString("PING")));
    });

    let handshakeStep = HandshapeStep.PING;

    masterConnection.on("data", (data) => {
      logFormattedData("received", Uint8Array.from(data));

      if (handshakeStep === HandshapeStep.DONE) {
        execCommand(data, masterConnection, true);
      } else {
        if (handshakeStep === HandshapeStep.PING && data.toString() === "+PONG\r\n") {
          masterConnection.write(serialazeBulkStringArray(["REPLCONF", "listening-port", String(serverState.port)]));
          handshakeStep = HandshapeStep.REPLCONF;
          return;
        }
        if (handshakeStep === HandshapeStep.REPLCONF && data.toString() === "+OK\r\n") {
          masterConnection.write(serialazeBulkStringArray(["REPLCONF", "capa", "psync2"]));
          handshakeStep = HandshapeStep.PSYNC;
          return;
        }
        if (handshakeStep === HandshapeStep.PSYNC && data.toString() === "+OK\r\n") {
          masterConnection.write(serialazeBulkStringArray(["PSYNC", "?", "-1"]));
          handshakeStep = HandshapeStep.FULLRESYNC;
          return;
        }
        const parser = new CommandParser(data);
        while (!parser.isEnd()) {
          if (handshakeStep === HandshapeStep.FULLRESYNC) {
            parser.parseString();
            handshakeStep = HandshapeStep.FILE_TRANSFER;
            continue;
          } else if (handshakeStep === HandshapeStep.FILE_TRANSFER) {
            // readfile
            const length = Number(parser.parseElement().slice(1));

            let str = parser.peekBytes(length).toString("utf-8");
            let i = length;

            while (str.length !== length && i < data.length) {
              str = parser.peekBytes(i).toString("utf-8");
              i++;
            }
            parser.readBytes(i - 2);
            handshakeStep = HandshapeStep.DONE;
          } else if (handshakeStep === HandshapeStep.DONE) {
            execCommand(parser.peekBytes(Infinity), masterConnection, true);
            break;
          }
        }
      }
    });
  }
}

async function readRDBFile(path: string | undefined) {
  if (!path) return null;

  const file = Bun.file(`${values.dir}/${path}`);
  if (!(await file.exists())) {
    return null;
  }

  const reader = await RDBReader.loadFile(file);
  const result = reader.readFile();

  return result;
}
