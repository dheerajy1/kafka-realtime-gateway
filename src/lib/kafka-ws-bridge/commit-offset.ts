/**
 * Resolve offset + commit next offset + heartbeat (shared helper).
 */

export async function commitMessageOffset(args: {
  consumer: {
    commitOffsets: (
      offsets: { topic: string; partition: number; offset: string }[],
    ) => Promise<void>;
  };
  topic: string;
  partition: number;
  offset: string;
  resolveOffset: (offset: string) => void;
  heartbeat: () => Promise<void>;
}): Promise<void> {
  const { consumer, topic, partition, offset, resolveOffset, heartbeat } = args;

  const nextOffset = (BigInt(offset) + 1n).toString();

  resolveOffset(offset);

  await consumer.commitOffsets([
    {
      topic,
      partition,
      offset: nextOffset,
    },
  ]);

  await heartbeat();
}
