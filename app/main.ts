import * as net from "net";
import { parse, type RESPDataType } from "./parseRedisCommand";
import { execCommand } from "./commands";
import { RDBReader } from "./RDBReader";
import { parseArgs } from "util";
import { serialazeArray, serialazeBulkString } from "./serialazeRedisCommand";

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

export let db: Map<string, { value: string; expire: bigint | null }> = new Map();
export const serverState = {
  port: Number(values.port) || 6379,
  role: values.replicaof ? "slave" : "master",
  master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
  master_repl_offset: 0,
};
main();

async function main() {
  const result = await readRDBFile(values.dbfilename);

  if (result) {
    db = new Map(Object.entries(result.db.values));
  }

  const server: net.Server = net.createServer((connection: net.Socket) => {
    connection.on("data", (data) => {
      const [input]: [RESPDataType[], number] = parse(data) as [RESPDataType[], number];

      execCommand(input, connection);
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
