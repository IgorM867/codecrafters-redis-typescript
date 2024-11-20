export function logFormattedData(event: string, data: Uint8Array) {
  console.log(
    `${event}:`,
    new TextDecoder()
      .decode(data)
      .replace(/ /g, "\\s")
      .replace(/\t/g, "\\t")
      .replace(/\n/g, "\\n")
      .replace(/\r/g, "\\r")
  );
}
