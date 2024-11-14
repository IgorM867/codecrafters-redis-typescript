import * as net from "net";
import { parse, type RESPDataType } from "./parseRedisCommand";
import { serialazeSimpleError } from "./serialazeRedisCommand";
import { commands } from "./commands";
import { RDBReader } from "./RDBReader";
import { parseArgs } from "util";

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
  },
  strict: true,
  allowPositionals: true,
});

export let db: Map<string, { value: string; expire: bigint | null }> = new Map();

main();

async function main() {
  const result = await readRDBFile(values.dbfilename);

  if (result) {
    db = new Map(Object.entries(result.db.values));
  }

  const server: net.Server = net.createServer((connection: net.Socket) => {
    connection.on("data", (data) => {
      const [input]: [RESPDataType[], number] = parse(data) as [RESPDataType[], number];

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
        default:
          connection.write(serialazeSimpleError(`Unknown command: ${input[0]}`));
      }
    });
  });

  server.listen(Number(values.port) || 6379, "127.0.0.1");

  console.log("Redis server is working...");
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
