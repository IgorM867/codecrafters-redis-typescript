import * as net from "net";
import { parse } from "./parseRedisCommand";
import { serialazeBulkString, serialazeSimpleError, serialazeSimpleString } from "./serialazeRedisCommand";

const storage = new Map();

const commands = {
  PING: () => serialazeSimpleString("PONG"),
  ECHO: (arg: string) => serialazeBulkString(arg),
  SET: (key: string | undefined, value: string | undefined) => {
    if (!key) {
      return serialazeSimpleError("SYNTAX ERR expecting key");
    }
    if (!value) {
      return serialazeSimpleError("SYNTAX ERR expecting value after key");
    }

    storage.set(key, value);
    return serialazeSimpleString("OK");
  },
  GET: (key: string | undefined) => {
    if (!key || !storage.has(key)) {
      return serialazeBulkString("");
    }

    return serialazeBulkString(storage.get(key));
  },
};

const server: net.Server = net.createServer((connection: net.Socket) => {
  connection.on("data", (data) => {
    const [input] = parse(data);

    switch ((input[0] as string).toUpperCase()) {
      case "PING":
        connection.write(commands.PING());
        break;
      case "ECHO":
        connection.write(commands.ECHO(input[1] as string));
        break;
      case "SET":
        connection.write(commands.SET(input[1] as string | undefined, input[2] as string | undefined));
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
