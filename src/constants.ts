export const DEFAULT_PREFIX = 'presence';
export const DEFAULT_TTL_SECONDS = 30;
export const DEFAULT_SWEEP_INTERVAL_MS = 15000;

export function formatPrefix(prefix = DEFAULT_PREFIX): string {
  // Use Redis Cluster hash tag format {prefix} if not already enclosed
  if (prefix.startsWith('{') && prefix.endsWith('}')) {
    return prefix;
  }
  return `{${prefix}}`;
}

export function getUserKey(userId: string, prefix = DEFAULT_PREFIX): string {
  const p = formatPrefix(prefix);
  return `${p}:user:${userId}`;
}

export function getUserRoomsKey(userId: string, prefix = DEFAULT_PREFIX): string {
  const p = formatPrefix(prefix);
  return `${p}:user:${userId}:rooms`;
}

export function getRoomKey(roomId: string, prefix = DEFAULT_PREFIX): string {
  const p = formatPrefix(prefix);
  return `${p}:room:${roomId}`;
}

export function getRoomSocketsKey(
  roomId: string,
  userId: string,
  prefix = DEFAULT_PREFIX,
): string {
  const p = formatPrefix(prefix);
  return `${p}:room:${roomId}:sockets:${userId}`;
}

export function getSocketKey(socketId: string, prefix = DEFAULT_PREFIX): string {
  const p = formatPrefix(prefix);
  return `${p}:socket:${socketId}`;
}

export function getSocketRoomsKey(socketId: string, prefix = DEFAULT_PREFIX): string {
  const p = formatPrefix(prefix);
  return `${p}:socket:${socketId}:rooms`;
}
