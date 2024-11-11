import * as net from "net";
import { parse, type RESPDataType } from "./parseRedisCommand";
import { serialazeBulkString, serialazeSimpleError, serialazeSimpleString } from "./serialazeRedisCommand";

const storage = new Map<string, string>();

const commands = {
  PING: () => serialazeSimpleString("PONG"),
  ECHO: (arg: string) => serialazeBulkString(arg),
  SET: (...args: Array<string | undefined>) => {
    const key = args[0];
    const value = args[1];
    if (!key) {
      return serialazeSimpleError("SYNTAX ERR expecting key");
    }
    if (!value) {
      return serialazeSimpleError("SYNTAX ERR expecting value after key");
    }
    if (args[2] && args[2].toUpperCase() === "PX") {
      if (!args[3]) return serialazeSimpleString("SYNTAX ERR expecting expiry time after PX");

      storage.set(key, value);
      setTimeout(() => storage.delete(key), Number(args[3]));
      return serialazeSimpleString("OK");
    }

    storage.set(key, value);
    return serialazeSimpleString("OK");
  },
  GET: (key: string | undefined) => {
    if (!key || !storage.has(key)) {
      return serialazeBulkString("");
    }

    return serialazeBulkString(storage.get(key)!);
  },
};

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
      default:
        connection.write(serialazeSimpleError(`Unknown command: ${input[0]}`));
    }
  });
});

server.listen(6379, "127.0.0.1");

console.log("Redis server is working...");
