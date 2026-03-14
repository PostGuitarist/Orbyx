/**
 * RealtimeChannel: thin LISTEN/NOTIFY wrapper over a dedicated pg.Client.
 * Separate from the pool — one long-lived connection per channel subscription.
 */

import { Client } from "pg";
import type { ConnectionConfig } from "./types/index";

/** Handler called with the raw payload string (or null) on each notification. */
export type NotificationHandler = (payload: string | null) => void;

/**
 * Quotes a Postgres identifier (e.g. channel name) for safe use in LISTEN/UNLISTEN.
 * Channel names are identifiers in Postgres — double-quoting allows any characters.
 */
function quoteChannelId(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Subscribes to a Postgres LISTEN channel. Call .on() to register handlers,
 * then .subscribe() to connect. Call .unsubscribe() to clean up.
 *
 * @example
 * const ch = client.channel("user_updates");
 * ch.on((payload) => console.log(payload));
 * await ch.subscribe();
 * // ...later:
 * await ch.unsubscribe();
 */
export class RealtimeChannel {
  private readonly channelName: string;
  private readonly connection: string | ConnectionConfig;
  private pgClient: Client | null = null;
  private handlers: NotificationHandler[] = [];
  private subscribingPromise: Promise<{ error: Error | null }> | null = null;

  constructor(channelName: string, connection: string | ConnectionConfig) {
    this.channelName = channelName;
    this.connection = connection;
  }

  /**
   * Register a notification handler. May be called multiple times.
   * All registered handlers fire on each notification.
   */
  on(handler: NotificationHandler): this {
    this.handlers.push(handler);
    return this;
  }

  /**
   * Remove a previously registered notification handler.
   * Does not affect the subscription — use unsubscribe() to tear down the connection.
   */
  off(handler: NotificationHandler): this {
    this.handlers = this.handlers.filter((h) => h !== handler);
    return this;
  }

  /**
   * Connect and issue LISTEN. Idempotent — safe to call when already subscribed.
   * Returns { error: null } on success, { error: Error } on failure.
   */
  async subscribe(): Promise<{ error: Error | null }> {
    if (this.pgClient !== null) return { error: null };

    // If another subscribe() is already in flight, await its result directly
    // rather than polling — avoids busy-waiting and gives callers the actual outcome.
    if (this.subscribingPromise !== null) {
      return this.subscribingPromise;
    }

    let resolveSubscribing!: (result: { error: Error | null }) => void;
    this.subscribingPromise = new Promise<{ error: Error | null }>((resolve) => {
      resolveSubscribing = resolve;
    });

    let client: Client | undefined;
    try {
      const connOpts =
        typeof this.connection === "string"
          ? { connectionString: this.connection }
          : {
              host: this.connection.host,
              port: this.connection.port,
              user: this.connection.user,
              password: this.connection.password,
              database: this.connection.database,
              ssl: this.connection.ssl as boolean | { rejectUnauthorized?: boolean } | undefined,
            };
      client = new Client(connOpts);
      await client.connect();
      const connectedClient = client;
      connectedClient.on("error", (err) => {
        console.error(
          `[Orbyx RealtimeChannel] Connection error on channel "${this.channelName}":`,
          err,
        );
        connectedClient.removeAllListeners();
        connectedClient.end().catch(() => {});
        this.pgClient = null;
      });
      connectedClient.on("notification", (msg) => {
        const payload = msg.payload ?? null;
        for (const h of this.handlers) {
          try {
            h(payload);
          } catch (handlerErr) {
            console.error(
              `[Orbyx RealtimeChannel] Handler error on channel "${this.channelName}":`,
              handlerErr,
            );
          }
        }
      });
      await connectedClient.query(`LISTEN ${quoteChannelId(this.channelName)}`);
      this.pgClient = connectedClient;
      const successResult: { error: Error | null } = { error: null };
      resolveSubscribing(successResult);
      return successResult;
    } catch (err) {
      if (client && this.pgClient !== client) {
        client.removeAllListeners();
        client.end().catch(() => {});
      }
      this.pgClient = null;
      const error = err instanceof Error ? err : new Error(String(err));
      const failResult = { error };
      resolveSubscribing(failResult);
      return failResult;
    } finally {
      this.subscribingPromise = null;
    }
  }

  /**
   * Issue UNLISTEN and close the dedicated connection. Idempotent.
   */
  async unsubscribe(): Promise<void> {
    if (!this.pgClient) return;
    const client = this.pgClient;
    this.pgClient = null;
    try {
      await client.query(`UNLISTEN ${quoteChannelId(this.channelName)}`);
      await client.end();
    } catch {
      // ignore cleanup errors; connection is already closing
    }
  }

  /** True after subscribe() succeeds and before unsubscribe() completes. */
  get isSubscribed(): boolean {
    return this.pgClient !== null;
  }
}
