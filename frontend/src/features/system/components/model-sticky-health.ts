// Failed channels retain their failure count after the cooldown expires.
export function modelStickyCooldownActive(until: string, now = Date.now()): boolean {
  return new Date(until).getTime() > now;
}
