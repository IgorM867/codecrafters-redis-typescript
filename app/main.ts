import * as net from "net";
import { parse } from "./parseRedisCommand";
import { serialazeBulkString, serialazeSimpleString } from "./serialazeRedisCommand";

const commands = {
  PING: () => serialazeSimpleString("PONG"),
  ECHO: (arg: string) => serialazeBulkString(arg),
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
      default:
        //TODO serialaze REDIS error
        throw Error(`Unknown command: ${input[0]}`);
    }
  });
});

server.listen(6379, "127.0.0.1");
